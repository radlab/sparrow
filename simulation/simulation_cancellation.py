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
PROBE_RATIO = 2
CANCELLATION = False
WORK_STEALING = False
NUM_SCHEDULERS = 1

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
        scheduler = random.randint(0, NUM_SCHEDULERS - 1)
        job = Job(TASKS_PER_JOB, current_time, self.task_distribution, MEDIAN_TASK_DURATION,
                  scheduler)
        #print "Job %s arrived at %s" % (job.id, current_time)
        # Schedule job.
        new_events = self.simulation.send_probes(job, current_time)
        # Add new Job Arrival event, for the next job to arrive after this one.
        arrival_delay = random.expovariate(1.0 / self.interarrival_delay)
        new_events.append((current_time + arrival_delay, self))
        #print "Retuning %s events" % len(new_events)
        return new_events

class ProbeEvent(Event):
    """ Event to signify a probe arriving at a worker. """
    def __init__(self, worker, job_id):
        self.worker = worker
        self.job_id = job_id

    def run(self, current_time):
        #print ("Probe for job %s arrived at worker %s at %s" %
        #       (self.job_id, self.worker.id, current_time))
        return self.worker.add_probe(self.job_id, current_time)

class NoopGetTaskResponseEvent(Event):
    """ Signifies when a getTask() RPC response arrives at a worker, with a noop response. """
    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        #print ("getTask() request for worker %s returned no task at %s" %
        #       (self.worker.id, current_time))
        return self.worker.free_slot(current_time)


class TaskEndEvent():
    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        return self.worker.free_slot(current_time)

class CancellationEvent():
    def __init__(self, worker, job_id):
        self.worker = worker
        self.job_id = job_id

    def run(self, current_time):
        self.worker.cancel_probe(self.job_id, current_time)
        return []

class Worker(object):
    def __init__(self, simulation, num_slots, id):
        self.simulation = simulation

        # List of times when slots were freed, for each free slot (used to track the time
        # the worker spends idle).
        self.free_slots = []
        while len(self.free_slots) < num_slots:
            self.free_slots.append(0)
        self.idle_ms = 0

        # Just a list of job ids!
        self.queued_probes = []
        self.id = id
        self.successful_cancellations = 0

    def add_probe(self, job_id, current_time):
        self.queued_probes.append(job_id)
        return self.maybe_get_task(current_time)

    def free_slot(self, current_time):
        """ Frees a slot on the worker and attempts to launch another task in that slot. """
        self.free_slots.append(current_time)
        get_task_events = self.maybe_get_task(current_time)
        if len(get_task_events) > 0:
            return get_task_events

        if WORK_STEALING:
            # Choose a random scheduler.
            scheduler = random.randint(0, NUM_SCHEDULERS - 1)
            time_slot_freed = self.free_slots.pop(0)
            self.idle_ms += current_time - time_slot_freed
            new_task_events = self.simulation.get_any_task(self, scheduler, current_time)
            assert len(new_task_events) >= 1
            return new_task_events

        return []

    def maybe_get_task(self, current_time):
        if len(self.queued_probes) > 0 and len(self.free_slots) > 0:
            # Account for "running" task
            time_slot_freed = self.free_slots.pop(0)
            self.idle_ms += current_time - time_slot_freed

            job_id = self.queued_probes[0]
            self.queued_probes = self.queued_probes[1:]

            return self.simulation.get_task(job_id, self, current_time)
        return []

    def cancel_probe(self, job_id, current_time):
        logging.debug("Attempting to cancel probe for job %s on worker %s (queue: %s) at %s" %
                      (job_id, self.id, self.queued_probes, current_time))
        if job_id in self.queued_probes:
            self.queued_probes.remove(job_id)
            self.successful_cancellations += 1

    def finish_simulation(self, current_time):
        """ Completes the simulation by adding the idle time at the end. """
        for time_freed in self.free_slots:
            self.idle_ms += time_freed - current_time

class Simulation(object):
    def __init__(self, num_jobs, file_prefix, load, task_distribution):
        avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        print ("Interarrival delay: %s (avg slots in use: %s)" %
               (self.interarrival_delay, avg_used_slots))
        self.jobs = {}
        self.unscheduled_jobs = []
        while len(self.unscheduled_jobs) < NUM_SCHEDULERS:
            self.unscheduled_jobs.append([])
        self.remaining_jobs = num_jobs
        self.event_queue = Queue.PriorityQueue()
        self.workers = []
        self.file_prefix = file_prefix
        while len(self.workers) < TOTAL_WORKERS:
            self.workers.append(Worker(self, SLOTS_PER_WORKER, len(self.workers)))
        self.worker_indices = range(TOTAL_WORKERS)
        self.task_distribution = task_distribution

        self.tasks_stolen = 0
        self.attempted_tasks_stolen = 0

        self.attempted_cancellations = 0

        self.successful_get_tasks = 0
        self.total_get_tasks = 0

    def send_probes(self, job, current_time):
        """ Send probes to acquire load information, in order to schedule a job. """
        self.jobs[job.id] = job
        self.unscheduled_jobs[job.scheduler].append(job)

        random.shuffle(self.worker_indices)
        probe_events = []
        num_probes = PROBE_RATIO * len(job.unscheduled_tasks)
        for worker_index in self.worker_indices[:num_probes]:
            probe_events.append((current_time + NETWORK_DELAY,
                                 ProbeEvent(self.workers[worker_index], job.id)))
            job.probed_workers.add(worker_index)
        return probe_events

    def get_task(self, job_id, worker, current_time):
        self.total_get_tasks += 1
        job = self.jobs[job_id]
        # Need to add a network delay here to account for the fact that we
        # call get_task when the probe is launched (not when it is received) to
        # avoid additional event processing.
        job.add_probe_response(worker, current_time + NETWORK_DELAY)
        response_time = current_time + 2*NETWORK_DELAY
        if len(job.unscheduled_tasks) > 0:
            self.successful_get_tasks += 1
            return self.get_task_for_job(job, worker, response_time)
        else:
            return [(response_time, NoopGetTaskResponseEvent(worker))]

    def get_task_for_job(self, job, worker, response_time):
        assert len(job.unscheduled_tasks) > 0
        events = []
        task_duration = job.unscheduled_tasks.pop(0)

        task_end_time = task_duration + response_time
        self.add_task_completion_time(job.id, task_end_time)

        # Remove this worker from the list of the job's probed workers.
        events.append((task_end_time, TaskEndEvent(worker)))

        if len(job.unscheduled_tasks) == 0:
            logging.info("Finished scheduling tasks for job %s" % job.id)
            self.unscheduled_jobs[job.scheduler].remove(job)
            if CANCELLATION:
                # Cancel remaining outstanding probes.
                logging.debug("Cancelling probes for job %s (will arrive at %s)" %
                              (job.id, response_time))
                for worker_id in job.probed_workers:
                    self.attempted_cancellations += 1
                    events.append((response_time,
                                   CancellationEvent(self.workers[worker_id], job.id)))
        return events

    def get_any_task(self, worker, scheduler, current_time):
        """ Used by an idle worker, to attempt to steal extra work."""
        self.attempted_tasks_stolen += 1
        response_time = current_time + 2*NETWORK_DELAY
        unscheduled_jobs = self.unscheduled_jobs[scheduler]
        if len(unscheduled_jobs) == 0:
            return [(response_time, NoopGetTaskResponseEvent(worker))]

        self.tasks_stolen += 1
        return self.get_task_for_job(unscheduled_jobs[0], worker, response_time)

    def add_task_completion_time(self, job_id, completion_time):
        job_complete = self.jobs[job_id].task_completed(completion_time)
        if job_complete:
            self.remaining_jobs -= 1
            #print ("Job %s completed in %s" %
            #       (job_id, self.jobs[job_id].end_time - self.jobs[job_id].start_time))

    def run(self):
        self.event_queue.put((0, JobArrival(self, self.interarrival_delay, self.task_distribution)))
        last_time = 0
        last_report = self.remaining_jobs
        half_jobs = self.remaining_jobs / 2
        output_worker_loads = False
        while self.remaining_jobs > 0:
            #if self.remaining_jobs != last_report and self.remaining_jobs % 100 == 0:
            #    print self.remaining_jobs, "jobs remaining"
            #    last_report = self.remaining_jobs
            # At a point in the middle of the experiment, get the distribution of tasks
            # on each worker.
            if not output_worker_loads and self.remaining_jobs == half_jobs:
                worker_loads = [len(w.free_slots) for w in self.workers]
                plot_cdf(worker_loads, "%s_worker_loads.data" % self.file_prefix)
                output_worker_loads = True

            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        for worker in self.workers:
            worker.finish_simulation(last_time)
        total_slot_ms = len(self.workers) * SLOTS_PER_WORKER * last_time
        total_idle_ms = sum([x.idle_ms for x in self.workers])
        fraction_idle = total_idle_ms * 1.0 / total_slot_ms

        rtt = 2 * NETWORK_DELAY
        total_failed_get_task_ms = (self.total_get_tasks - self.successful_get_tasks) * rtt
        fraction_failed_get_task = total_failed_get_task_ms * 1.0 / total_slot_ms

        total_failed_stolen_ms = (self.attempted_tasks_stolen - self.tasks_stolen) * rtt
        fraction_failed_stolen = total_failed_stolen_ms * 1.0 / total_slot_ms

        total_successful_get_task_ms = self.successful_get_tasks * rtt
        fraction_successful_get_task = total_successful_get_task_ms * 1.0 / total_slot_ms
        total_successful_stolen_ms = self.tasks_stolen * 2 * NETWORK_DELAY
        fraction_successful_stolen = total_successful_stolen_ms * 1.0 / total_slot_ms
        print (("Idle time: %s (%s), failed get task time: %s (%s), failed steals time: %s (%s), "
                "successful GetTask()s: %s (%s), successful steals: %s (%s), (total: %s)") %
               (total_idle_ms, fraction_idle, total_failed_get_task_ms, fraction_failed_get_task,
                total_failed_stolen_ms, fraction_failed_stolen,
                total_successful_get_task_ms, fraction_successful_get_task,
                total_successful_stolen_ms, fraction_successful_stolen,
                fraction_idle + fraction_failed_get_task + fraction_failed_stolen +
                fraction_successful_get_task + fraction_successful_stolen))
        print ("Simulation ended after %s milliseconds (%s jobs started, %s tasks stolen)" %
               (last_time, len(self.jobs), self.tasks_stolen))
        total_successful_cancellations = sum([x.successful_cancellations for x in self.workers])
        print ("%s/%s cancellations successful" %
               (total_successful_cancellations, self.attempted_cancellations))

        tasks_stolen_file = open("%s_tasks_stolen" % self.file_prefix, "w")
        tasks_stolen_file.write("Attempts: %s, successes: %s\n" %
                                (self.attempted_tasks_stolen, self.tasks_stolen))
        tasks_stolen_file.close()

        complete_jobs = [j for j in self.jobs.values() if j.completed_tasks_count == j.num_tasks]
        print "%s complete jobs" % len(complete_jobs)
        response_times = [job.end_time - job.start_time for job in complete_jobs
                          if job.start_time > 500]
        print "Included %s jobs" % len(response_times)
        plot_cdf(response_times, "%s_response_times.data" % self.file_prefix)
        print "Average response time: ", numpy.mean(response_times)

        wait_times = [j.time_all_tasks_scheduled - j.start_time for j in complete_jobs
                      if j.start_time > 500]
        plot_cdf(wait_times, "%s_wait_times.data" % self.file_prefix)
        print "Average wait time: ", numpy.mean(wait_times)

        num_no_wait_jobs = len([w for w in wait_times if w == 2 * NETWORK_DELAY])
        frac_no_wait_jobs = num_no_wait_jobs * 1.0 / len(complete_jobs)
        print "No wait jobs: %s/%s (%f)" % (num_no_wait_jobs, len(complete_jobs),
                                            frac_no_wait_jobs)

        cancellation_time_window = [job.last_probe_reply_time - job.time_all_tasks_scheduled
                                    for job in complete_jobs]
        plot_cdf(cancellation_time_window, "%s_cancellation_window.data" % self.file_prefix)

        longest_tasks = [job.longest_task for job in complete_jobs]
        plot_cdf(longest_tasks, "%s_ideal_response_time.data" % self.file_prefix)
        return response_times

def main():
    logging.basicConfig(level=logging.ERROR)
    sim = Simulation(5000, "test", 0.8, TaskDistributions.EXP_JOBS)
    sim.run()

if __name__ == "__main__":
    main()
