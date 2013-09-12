#
# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import math
import numpy
import random
import Queue
from util import Job, TaskDistributions

MEDIAN_TASK_DURATION = 100
NETWORK_DELAY = 0.5
TASKS_PER_JOB = 100
# This needs to be set!
SLOTS_PER_WORKER = 4
TOTAL_WORKERS = 10000
PROBE_RATIO = 2

# Whether to do per-task assignment.
PER_TASK = True

def get_percentile(N, percent, key=lambda x:x):
    if not N:
        return 0
    k = (len(N) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0 + d1

def plot_cdf(values, filename):
    values.sort()
    f = open(filename, "w")
    for percent in range(100):
        fraction = percent / 100.
        f.write("%s\t%s\n" % (fraction, get_percentile(values, fraction)))
    f.close()

class Event(object):
    """ Abstract class representing events. """
    def __init__(self):
        raise NotImplementedError("Event is an abstract class and cannot be "
                                  "instantiated directly")

    def run(self, current_time):
        """ Returns any events that should be added to the queue. """
        raise NotImplementedError("The run() method must be implemented by "
                                  "each class subclassing Event")

class JobArrival(Event):
    """ Event to signify a job arriving at a scheduler. """
    def __init__(self, simulation, interarrival_delay, task_distribution):
        self.simulation = simulation
        self.interarrival_delay = interarrival_delay
        self.task_distribution = task_distribution

    def run(self, current_time):
        job = Job(TASKS_PER_JOB, current_time, self.task_distribution, MEDIAN_TASK_DURATION)
        #print "Job %s arrived at %s" % (job.id, current_time)
        # Schedule job.
        probe_event = self.simulation.send_probes(job, current_time)
        # Add new Job Arrival event, for the next job to arrive after this one.
        arrival_delay = random.expovariate(1.0 / self.interarrival_delay)
        job_arrival_event = (current_time + arrival_delay, self)
        return [probe_event, job_arrival_event]

class ProbeEvent(Event):
    """ Event to signify all probes for a job arriving at all probed workers.

    Probed workers are selected when the probes "arrive" (i.e., in the run method)."""
    def __init__(self, simulation, job_id):
        self.simulation = simulation
        self.job_id = job_id

    def run(self, current_time):
        task_arrival_events = []
        if PER_TASK:
            task_arrival_events = self.simulation.assign_tasks_per_task(current_time, self.job_id)
        else:
            task_arrival_events = self.simulation.assign_tasks(current_time, self.job_id)
        assert (len(task_arrival_events) == self.simulation.jobs[self.job_id].num_tasks)
        return task_arrival_events

class TaskArrival(Event):
    """ Event to signify a task arriving at a worker. """
    def __init__(self, worker, task_duration, job_id):
        self.worker = worker
        self.task_duration = task_duration
        self.job_id = job_id

    def run(self, current_time):
        return self.worker.add_task(current_time, self.task_duration, self.job_id)


class TaskEndEvent():
    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        return self.worker.free_slot(current_time)

class Worker(object):
    def __init__(self, simulation, num_slots, id):
        self.simulation = simulation
        self.free_slots = num_slots
        # Just a list of (task duration, job id) pairs.
        self.queued_tasks = Queue.Queue()
        self.num_queued_tasks = 0
        self.id = id

    def queue_length(self):
        queue_length = self.num_queued_tasks
        if self.free_slots > 0:
            assert self.num_queued_tasks == 0
            return -self.free_slots
        return queue_length

    def add_task(self, current_time, task_duration, job_id):
        self.queued_tasks.put((task_duration, job_id))
        self.num_queued_tasks += 1
        return self.maybe_start_task(current_time)

    def free_slot(self, current_time):
        """ Frees a slot on the worker and attempts to launch another task in that slot. """
        self.free_slots += 1
        get_task_events = self.maybe_start_task(current_time)
        return get_task_events

    def maybe_start_task(self, current_time):
        if not self.queued_tasks.empty() and self.free_slots > 0:
            # Account for "running" task
            self.free_slots -= 1
            task_duration, job_id = self.queued_tasks.get()
            self.num_queued_tasks -= 1
            task_end_time = current_time + task_duration
            #print ("Task for job %s on worker %s launched at %s; will complete at %s" %
            #       (job_id, self.id, current_time, task_end_time))
            self.simulation.add_task_completion_time(job_id, task_end_time)
            return [(task_end_time, TaskEndEvent(self))]
        return []

class Simulation(object):
    def __init__(self, num_jobs, file_prefix, load, task_distribution):
        avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        print ("Interarrival delay: %s (avg slots in use: %s)" %
               (self.interarrival_delay, avg_used_slots))
        self.jobs = {}
        self.remaining_jobs = num_jobs
        self.event_queue = Queue.PriorityQueue()
        self.workers = []
        self.file_prefix = file_prefix
        while len(self.workers) < TOTAL_WORKERS:
            self.workers.append(Worker(self, SLOTS_PER_WORKER, len(self.workers)))
        self.worker_indices = range(TOTAL_WORKERS)
        self.task_distribution = task_distribution

    def assign_tasks(self, current_time, job_id):
        # Randomly choose workers
        #(note that this doesn't have to be done until the probe "arrives").
        job = self.jobs[job_id]
        random.shuffle(self.worker_indices)
        num_probes = PROBE_RATIO * job.num_tasks
        loads = [(i, self.workers[i].queue_length())
                 for i in self.worker_indices[:num_probes]]
        loads.sort(key = lambda x: x[1])
        #loads_str = ", ".join(["%s:%s" % (l[0], l[1]) for l in loads])

        # Assign tasks
        task_arrival_events = []
        for i, (worker_index, load) in enumerate(loads[:job.num_tasks]):
            #print ("Assigning task of duration %s for job %s to %s" %
            #       (job.unscheduled_tasks[i], job_id, worker_index))
            task_arrival_events.append(
                        (current_time + 2*NETWORK_DELAY,
                         TaskArrival(self.workers[worker_index], job.unscheduled_tasks[i], job_id)))
        return task_arrival_events

    def assign_tasks_per_task(self, current_time, job_id):
        """ Non-generously does per-task sampling."""
        job = self.jobs[job_id]
        random.shuffle(self.worker_indices)
        task_arrival_events = []
        for i, task_duration in enumerate(job.unscheduled_tasks):
            loads = [(i, self.workers[i].queue_length())
                     for i in self.worker_indices[PROBE_RATIO*i:PROBE_RATIO*(i+1)]]
            #loads_str = ", ".join(["%s:%s" % (l[0], l[1]) for l in loads])
            #print "Loads: %s" % loads_str
            loads.sort(key = lambda x: x[1])
            #print ("Assigning task of duration %s for job %s to worker %s" %
            #       (task_duration, job_id, loads[0][0]))
            task_arrival_events.append(
                    (current_time + 2*NETWORK_DELAY,
                     TaskArrival(self.workers[loads[0][0]], task_duration, job_id)))
        return task_arrival_events

    def send_probes(self, job, current_time):
        """ Send probes to acquire load information, in order to schedule a job. """
        self.jobs[job.id] = job
        return (current_time + NETWORK_DELAY, ProbeEvent(self, job.id))

    def add_task_completion_time(self, job_id, completion_time):
        job_complete = self.jobs[job_id].task_completed(completion_time)
        if job_complete:
            self.remaining_jobs -= 1

    def run(self):
        self.event_queue.put((0, JobArrival(self, self.interarrival_delay, self.task_distribution)))
        last_time = 0
        while self.remaining_jobs > 0:
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        print ("Simulation ended after %s milliseconds (%s jobs started)" %
               (last_time, len(self.jobs)))
        complete_jobs = [j for j in self.jobs.values() if j.completed_tasks_count == j.num_tasks]
        print "%s complete jobs" % len(complete_jobs)
        response_times = [job.end_time - job.start_time for job in complete_jobs
                          if job.start_time > 500]
        print "Included %s jobs" % len(response_times)
        plot_cdf(response_times, "%s_response_times.data" % self.file_prefix)
        print "Average response time: ", numpy.mean(response_times)

        longest_tasks = [job.longest_task for job in complete_jobs]
        plot_cdf(longest_tasks, "%s_ideal_response_time.data" % self.file_prefix)
        return response_times

def main():
    sim = Simulation(10, "per_task", 0.9, TaskDistributions.CONSTANT)
    sim.run()

if __name__ == "__main__":
    main()