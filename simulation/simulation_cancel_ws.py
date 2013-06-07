import math
import numpy
import random
import Queue
from util import TaskDistributions, Job

MEDIAN_TASK_DURATION = 100
NETWORK_DELAY = 1
TASKS_PER_JOB = 500
SLOTS_PER_WORKER = 4
TOTAL_WORKERS = 10000
PROBE_RATIO = 2
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
        job = Job(TASKS_PER_JOB, current_time, self.task_distribution, MEDIAN_TASK_DURATION, scheduler)
        #print "Job %s arrived at %s" % (job.id, current_time)
        # Schedule job.
        new_events = self.simulation.send_probes(job, scheduler, current_time)
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
        self.worker.cancel_probe(self.job_id)
        return []

class Worker(object):
    def __init__(self, simulation, num_slots, id):
        self.simulation = simulation
        self.free_slots = num_slots
        # Just a list of job ids!
        self.queued_probes = []
        self.id = id

    def add_probe(self, job_id, current_time):
        self.queued_probes.append(job_id)
        return self.maybe_get_task(current_time)

    def free_slot(self, current_time):
        """ Frees a slot on the worker and attempts to launch another task in that slot. """
        self.free_slots += 1
        get_task_events = self.maybe_get_task(current_time)
        return get_task_events

    def maybe_get_task(self, current_time):
        if len(self.queued_probes) > 0 and self.free_slots > 0:
            # Account for "running" task
            self.free_slots -= 1
            job_id = self.queued_probes[0]
            self.queued_probes = self.queued_probes[1:]

            task_duration = self.simulation.get_task(job_id)
            probe_response_time = current_time + 2*NETWORK_DELAY
            if task_duration > 0:
                task_end_time = probe_response_time + task_duration
                #print (("Task for job %s running on worker %s (get task at: %s, duration: %s, "
                #        "end: %s)") %
                #       (job_id, self.id, current_time, task_duration, task_end_time))
                self.simulation.add_task_completion_time(
                            job_id, task_end_time)
                new_event = TaskEndEvent(self)
                return [(task_end_time, new_event)]
            else:
                # There was no task left for the job, so send another probe
                # after 1RTT.
                return [(probe_response_time, NoopGetTaskResponseEvent(self))]
        return []

    def cancel_probe(self, job_id):
        self.queued_probes.remove(job_id)

class Simulation(object):
    def __init__(self, num_jobs, file_prefix, load, task_distribution):
        avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        print ("Interarrival delay: %s (avg slots in use: %s)" %
               (self.interarrival_delay, avg_used_slots))
        self.jobs = {}

        self.unscheduled_jobs = []
        while len(self.unscheduled_jobs) < NUM_SCHEDULERS:
            self.unscheduled_jobs.append({})

        self.unscheduled_jobs = []
        while len(self.unscheduled_jobs) < NUM_SCHEDULERS:
            self.unscheduled_jobs.append({})

        self.remaining_jobs = num_jobs
        self.event_queue = Queue.PriorityQueue()
        self.workers = []
        self.file_prefix = file_prefix
        while len(self.workers) < TOTAL_WORKERS:
            self.workers.append(Worker(self, SLOTS_PER_WORKER, len(self.workers)))
        self.worker_indices = range(TOTAL_WORKERS)
        self.task_distribution = task_distribution

    def send_probes(self, job, scheduler, current_time):
        """ Send probes to acquire load information, in order to schedule a job. """
        self.jobs[job.id] = job
        self.unscheduled_jobs[scheduler][job.id] = job

        random.shuffle(self.worker_indices)
        probe_events = []
        num_probes = PROBE_RATIO * len(job.unscheduled_tasks)
        for worker_index in self.worker_indices[:num_probes]:
            probe_events.append((current_time + NETWORK_DELAY,
                                 ProbeEvent(self.workers[worker_index], job.id)))
            job.probed_workers.add(worker_index)
        return probe_events

    def get_task(self, job_id):
        job = self.jobs[job_id]
        if len(job.unscheduled_tasks) > 0:
            task_duration = job.unscheduled_tasks[0]
            job.unscheduled_tasks = job.unscheduled_tasks[1:]
            if len(job.unscheduled_tasks) == 0:
                # All tasks have been scheduled! Can cancel outstanding probes.
                self.unscheduled_jobs[job.scheduler].pop(job.id)
            return task_duration
        return -1

    def add_task_completion_time(self, job_id, completion_time):
        job_complete = self.jobs[job_id].task_completed(completion_time)
        if job_complete:
            self.remaining_jobs -= 1
            #print ("Job %s completed in %s" %
            #       (job_id, self.jobs[job_id].end_time - self.jobs[job_id].start_time))

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
    sim = Simulation(10000, "cancellation", 0.95, TaskDistributions.EXP_JOBS)
    sim.run()

if __name__ == "__main__":
    main()