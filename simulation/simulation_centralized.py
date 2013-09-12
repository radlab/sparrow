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

""" Simulates a central scheduler with a complete cluster view. """

import logging
import math
import numpy
import random
import Queue
from util import Job, TaskDistributions

MEDIAN_TASK_DURATION = 100
NETWORK_DELAY = 0.5
TASKS_PER_JOB = 100
SLOTS_PER_WORKER = 4
TOTAL_WORKERS = 10000

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
        new_events = self.simulation.schedule_tasks(job, current_time)
        # Add new Job Arrival event, for the next job to arrive after this one.
        arrival_delay = random.expovariate(1.0 / self.interarrival_delay)
        new_events.append((current_time + arrival_delay, self))
        #print "Retuning %s events" % len(new_events)
        return new_events

class TaskEndEvent():
    """ This event is used to signal to the scheduler that a slot is free (so should include
    the RTT to notify the scheduler). """
    def __init__(self, simulation):
        self.simulation = simulation

    def run(self, current_time):
        return self.simulation.free_slot(current_time)

class Simulation(object):
    def __init__(self, num_jobs, file_prefix, load, task_distribution):
        avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        print ("Interarrival delay: %s (avg slots in use: %s)" %
               (self.interarrival_delay, avg_used_slots))
        self.jobs = {}
        self.remaining_jobs = num_jobs
        self.event_queue = Queue.PriorityQueue()
        self.num_free_slots = TOTAL_WORKERS * SLOTS_PER_WORKER
        self.unscheduled_jobs = []
        self.file_prefix = file_prefix
        self.task_distribution = task_distribution

    def schedule_tasks(self, job, current_time):
        self.jobs[job.id] = job
        self.unscheduled_jobs.append(job)
        #print "Job %s arrived at %s" % (job.id, current_time)
        return self.maybe_launch_tasks(current_time)

    def maybe_launch_tasks(self, current_time):
        task_end_events = []
        while self.num_free_slots > 0 and len(self.unscheduled_jobs) > 0:
            self.num_free_slots -= 1
            job = self.unscheduled_jobs[0]
            task_duration = job.unscheduled_tasks[0]
            job.unscheduled_tasks = job.unscheduled_tasks[1:]
            if len(job.unscheduled_tasks) == 0:
                logging.info("Finished scheduling tasks for job %s" % job.id)
            #print ("Launching task for job %s at %s (duration %s); %s remaining slots" %
            #       (job.id, current_time + NETWORK_DELAY, task_duration, self.num_free_slots))
            task_end_time = current_time + task_duration + NETWORK_DELAY
            scheduler_notify_time = task_end_time + NETWORK_DELAY
            task_end_events.append((scheduler_notify_time, TaskEndEvent(self)))

            job_complete = job.task_completed(task_end_time)
            if job_complete:
                #print "Completed job %s in %s" % (job.id, job.end_time - job.start_time)
                self.remaining_jobs -= 1
                self.unscheduled_jobs = self.unscheduled_jobs[1:]
        return task_end_events

    def free_slot(self, current_time):
        self.num_free_slots += 1
        return self.maybe_launch_tasks(current_time)

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
    logging.basicConfig(level=logging.INFO)
    sim = Simulation(100000, "centralized", 0.95, TaskDistributions.CONSTANT)
    sim.run()

if __name__ == "__main__":
    main()