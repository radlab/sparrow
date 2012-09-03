""" Parses a log file and outputs aggregated information about the experiment.

All times are in milliseconds unless otherwise indicated.
"""
import functools
import logging
import math
import os
import subprocess
import stats
import sys
import time

INVALID_TIME = 0
INVALID_TIME_DELTA = -sys.maxint - 1
INVALID_QUEUE_LENGTH = -1

START_SEC = 200
END_SEC = 300

""" from http://code.activestate.com/
         recipes/511478-finding-the-percentile-of-the-values/ """
def get_percentile(N, percent, key=lambda x:x):
    """ Find the percentile of a list of values.

    Args:
      percent: a float value from 0.0 to 1.0.
      key: optional key function to compute value from each element of N.

    Returns:
      The percentile of the values
    """
    if not N:
        return 0
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

class Task:
    """ Class to store information about a task.

    We store a variety of events corresponding to each task launch,
    as described in __init__
    """
    def __init__(self, id):
        self.__logger = logging.getLogger("Task")

        # When the scheduler (resident with the frontend) assigned the task to the slave.
        self.scheduler_launch_time = INVALID_TIME
        # When the node monitor (resident with the backend) launched the task
        self.node_monitor_launch_time = INVALID_TIME
        # When the backend completed the task
        self.completion_time = INVALID_TIME
        # Address of the machine that the task ran on.
        self.id = id

    def set_scheduler_launch_time(self, time):
        if self.scheduler_launch_time != INVALID_TIME:
            self.__logger.warn(("Task %s launched at scheduler twice; expect "
                                "task to only launch once") % id)
        self.scheduler_launch_time = time

    def set_node_monitor_launch_time(self, time):
        if self.node_monitor_launch_time != INVALID_TIME:
            self.__logger.warn(("Task %s launched twice; expect task to "
                                "only launch once") % id)
        self.node_monitor_launch_time = time

    def set_completion_time(self, time):
        if self.completion_time != INVALID_TIME:
            self.__logger.warn(("Task %s completed twice; "
                              "expect task to only complete once") % id)
        self.completion_time = time

    def queued_time(self):
        """ Returns the time spent waiting to launch on the backend. """
        return (self.node_monitor_launch_time - self.node_monitor_submit_time)

    def service_time(self):
        """ Returns the service time (time executing on backend)."""
        return (self.completion_time - self.node_monitor_launch_time)

    def complete(self):
        """ Returns whether we have complete information on this task. """
        return (self.scheduler_launch_time != INVALID_TIME and
                self.node_monitor_launch_time != INVALID_TIME and
                self.completion_time != INVALID_TIME)

class Request:
    def __init__(self, id):
        self.__id = id
        self.__num_tasks = 0
        self.__arrival_time = INVALID_TIME
        self.__tasks = {}
        # Address of the scheduler that received the request (and placed it).
        self.__scheduler_address = ""
        self.__logger = logging.getLogger("Request")
        # List of times when reservations were replied to.
        self.__reservation_replies = []

    def add_arrival(self, time, num_tasks, address):
        self.__arrival_time = time
        self.__num_tasks = int(num_tasks)
        self.__scheduler_address = address

    def add_reservation_reply(self, time):
        self.__reservation_replies.append(time)

    def get_reservation_replies(self):
        return (self.__arrival_time, self.__reservation_replies)

    def add_scheduler_task_launch(self, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_scheduler_launch_time(launch_time)

    def add_node_monitor_task_launch(self, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_node_monitor_launch_time(launch_time)

    def add_task_completion(self, task_id, completion_time):
        # We might see a task completion before a task launch, depending on the
        # order that we read log files in.
        task = self.__get_task(task_id)
        task.set_completion_time(completion_time)

    def arrival_time(self):
        """ Returns the time at which the job arrived at the scheduler. """
        return self.__arrival_time

    def scheduler_address(self):
        return self.__scheduler_address

    def network_delays(self):
        """ Returns a list of delays for all __tasks with delay information. """
        network_delays = []
        for task in self.__tasks.values():
            if task.complete():
                network_delays.append(task.network_delay())
                if task.network_delay() > 20:
                  print "Long launch %s" % self.__id
                  print task.node_monitor_submit_time
                  print task.scheduler_launch_time
                  print task.id
                  print task.address
                  print
        return network_delays

    def start_and_service_times(self):
        """ Returns a list of (start time, service time) tuples for complete __tasks. """
        return [(x.scheduler_launch_time, x.service_time()) for x in self.__tasks.values()
                if x.complete()]

    def service_times(self):
        """ Returns a list of service times for complete __tasks. """
        return [task.service_time() for task in self.__tasks.values() if task.complete()]

    def queue_times(self):
        """ Returns a list of queue times for all complete __tasks. """
        return [task.queued_time() for task in self.__tasks.values() if task.complete()]

    def response_time(self):
        """ Returns the time from when the job arrived to when it completed.

        Returns INVALID_TIME_DELTA if we don't have completion information on the job.  Note
        that we may have information about when the job completed, but not
        complete information about the job (e.g. we don't know when one of the tasks
        was launched).
        """
        if self.__arrival_time == INVALID_TIME:
            self.__logger.debug("Request %s missing arrival time" % self.__id)
            return INVALID_TIME_DELTA
        completion_time = self.__arrival_time
        for task_id, task in self.__tasks.items():
            if task.completion_time == INVALID_TIME:
                self.__logger.debug(("Task %s in request %s missing completion "
                                   "time") % (task_id, self.__id))
                return INVALID_TIME_DELTA
            task_completion_time = task.completion_time
            #if task.scheduler_launch_time > task.node_monitor_launch_time:
                 #self.__logger.warn(("Task %s suggests clock skew: scheduler launch time %d, node "
                 #                    "monitor launch time %d") %

                                    #(task_id, task.scheduler_launch_time,
                                    # task.node_monitor_launch_time))
            completion_time = max(completion_time, task_completion_time)

        if (completion_time - self.__arrival_time) > 2000:
          pass
        return completion_time - self.__arrival_time

    def complete(self):
        """ Returns whether we have complete info for the request.

        Due to incomplete log files, it's possible that we'll have completion
        information but not start information for a job. """
        if (self.__num_tasks == 0 or
            self.__arrival_time == 0 or
            self.__num_tasks != len(self.__tasks)):
            return False
        for task in self.__tasks.values():
            if not task.complete():
                return False
        return True

    def __get_task(self, task_id):
        """ Gets the task from the map of __tasks.

        Creates a new task if the task with the given ID doesn't already
        exist.
        """
        if task_id not in self.__tasks:
            self.__tasks[task_id] = Task(task_id)
        return self.__tasks[task_id]

    def __get_probe(self, address):
        """ Gets the probe from the map of __probes.

        Creates a new probe if the probe with the given address doesn't already
        exist.
        """
        if address not in self.__probes:
            self.__probes[address] = Probe(self.__id, address)
        return self.__probes[address]

class LogParser:
    """ Helps extract job information from log files.

    Attributes:
        requests: A map of strings specifying request IDs to jobs.
    """
    CLASS_INDEX = 0
    TIME_INDEX = 1
    AUDIT_EVENT_INDEX = 2

    def __init__(self):
        self.__requests = {}
        self.__logger = logging.getLogger("LogParser")
        self.__earliest_time = (time.time() * 1000)**2
        # Mapping of node monitor IP addresses to a list of (queue length, time) pairs observed at
        # that IP address.
        self.__node_monitor_queue_lengths = {}

    def parse_file(self, filename):
        file = open(filename, "r")
        for line in file:
            # Strip off the newline at the end of the line.
            items = line[:-1].split("\t")
            if len(items) != 3:
                self.__logger.warn(("Ignoring log message '%s' with unexpected "
                                  "number of items (expected 3; found %d)") %
                                 (line, len(items)))
                continue

            # Time is expressed in epoch milliseconds.
            time = int(items[self.TIME_INDEX])
            self.__earliest_time = min(self.__earliest_time, time)

            audit_event_params = items[self.AUDIT_EVENT_INDEX].split(":")
            if audit_event_params[0] == "arrived":
                request = self.__get_request(audit_event_params[1])
                request.add_arrival(time, audit_event_params[2],
                                    audit_event_params[3])
            elif audit_event_params[0] == "reservation_enqueued":
                self.__reservation_enqueued(time, audit_event_params[1], audit_event_params[3])
            elif audit_event_params[0] == "assigned_task":
                 request = self.__get_request(audit_event_params[1])
                 request.add_scheduler_task_launch(audit_event_params[2], time)
                 request.add_reservation_reply(time)
            elif audit_event_params[0] == "get_task_no_task":
                request = self.__get_request(audit_event_params[1])
                request.add_reservation_reply(time)
            elif audit_event_params[0] == "task_launch":
                request = self.__get_request(audit_event_params[1])
                request.add_node_monitor_task_launch(audit_event_params[2], time)
            elif audit_event_params[0] == "task_completed":
                request = self.__get_request(audit_event_params[1])
                request.add_task_completion(audit_event_params[2], time)
            else:
                self.__logger.warn("Received unknown audit event: " + audit_event_params[0])

    def output_reservation_queue_lengths(self, output_directory):
        gnuplot_file = open("%s/reservation_queue_lengths.gp" % output_directory, "w")
        gnuplot_file.write("set terminal postscript color 'Helvetica' 12\n")
        gnuplot_file.write("set output '%s/reservation_queue_length.ps'\n" %
                           output_directory)
        gnuplot_file.write("set xlabel 'Time (ms)'\n")
        gnuplot_file.write("set ylabel 'Queue Length'\n")
        gnuplot_file.write("plot ")
        is_first = True
        for (node_monitor_address, queue_lengths) in self.__node_monitor_queue_lengths.items():
            results_filename = "%s/%s_queue_lengths" % (output_directory, node_monitor_address)
            file = open(results_filename, "w")
            file.write("time\tQueue Length\n")
            for time, queue_length in queue_lengths:
                file.write("%s\t%s\n" % (time, queue_length))
            file.close()
            if not is_first:
                gnuplot_file.write(",\\\n")
            is_first = False
            gnuplot_file.write("'%s' using 1:2 lw 1 with lp" % results_filename)

    def output_tasks_launched_versus_time(self, output_directory):
        """ Creates a gnuplot file to plot tasks launched versus time for 10 requests in the
            middle of the experiment. """
        gnuplot_file = open("%s/task_launches_vs_time.gp" % output_directory, "w")
        gnuplot_file.write("set terminal postscript color 'Helvetica' 12\n")
        gnuplot_file.write("set output '%s/task_launches_vs_time.ps'\n" % output_directory)
        gnuplot_file.write("set xlabel 'Time (ms)'\n")
        gnuplot_file.write("set ylabel 'Tasks Launched'\n")
        gnuplot_file.write("plot ")

        job_count = 0
        for id, request in self.__requests.items():
            results_filename = "%s/%s_tasks_launched_vs_time" % (output_directory, id)
            file = open(results_filename, "w")
            arrival_time, reservation_replies = request.get_reservation_replies()
            reservation_count = 0
            file.write("0\t0\n")
            for reservation in reservation_replies:
                reservation_count += 1
                # Write the elapsed time since the request arrived.
                file.write("%s\t%s\n" % (reservation - arrival_time, reservation_count))
            file.close()

            if job_count != 0:
                gnuplot_file.write(",\\\n")
            gnuplot_file.write("'%s' using 1:2 lw 1 with lp" % results_filename)
            job_count += 1
            if job_count >= 20:
                break
        gnuplot_file.close()

    def output_results(self, output_directory, aggregate_results_filename):
        # Response time is the time from when the job arrived at a scheduler
        # to when it completed.
        response_times = []
        # Network/processing delay for each task.
        network_delays = []
        service_times = []
        # Used to look at the effects of jitting.
        start_and_service_times = []
        queue_times = []
        probe_times = []
        start_time = self.__earliest_time + (START_SEC * 1000)
        end_time = self.__earliest_time + (END_SEC * 1000)

        test_requests = filter(lambda k: k.complete(), self.__requests.values())
        print len(test_requests)
        considered_requests = filter(lambda k: k.arrival_time() >= start_time and
                                     k.arrival_time() <= end_time and
                                     k.complete(),
                                     self.__requests.values())
        print "Included %s requests" % len(considered_requests)
        print "Excluded %s requests" % (len(self.__requests.values()) - len(considered_requests))
        for request in considered_requests:
            scheduler_address = request.scheduler_address()
            # TODO: Fix network delay calculation code.
            #network_delays.extend(request.network_delays())
            service_times.extend(request.service_times())
            start_and_service_times.extend(request.start_and_service_times())
            # TODO: Fix queue time calculation.
            #queue_times.extend(request.queue_times())
            response_time = request.response_time()
            response_times.append(response_time)

        # Output data for response time and network delay CDFs.
        results_filename = "%s/results.data" % output_directory
        file = open(results_filename, "w")
        file.write("%ile\tResponseTime\tNetworkDelay\tServiceTime\tQueuedTime\n")
        NUM_DATA_POINTS = 100
        response_times.sort()
        network_delays.sort()
        service_times.sort()
        queue_times.sort()

        for i in range(NUM_DATA_POINTS):
            i = float(i) / NUM_DATA_POINTS
            file.write("%f\t%d\t%d\t%d\t%d\n" % (i,
                get_percentile(response_times, i),
                get_percentile(network_delays, i),
                get_percentile(service_times, i),
                get_percentile(queue_times, i)))
        file.close()

        # Output task run time as a function of start time.
        start_and_service_times.sort(key = lambda x: x[0])
        first_start_time = start_and_service_times[0][0]
        stride = max(1, len(start_and_service_times) / 500)
        start_and_service_filename = "%s/start_and_service_time.data" % output_directory
        start_and_service_file = open(start_and_service_filename, "w")
        for start_time, service_time in start_and_service_times[::stride]:
            start_and_service_file.write("%s\t%s\n" % (start_time - first_start_time,
                                                       service_time))
        start_and_service_file.close();
        start_and_service_gnuplot_file = open("%s/start_and_service_time.gp" % output_directory,
                                              "w")
        start_and_service_gnuplot_file.write("set terminal postscript color\n")
        start_and_service_gnuplot_file.write("set output '%s/start_and_service_time.ps'\n" %
                                             output_directory)
        start_and_service_gnuplot_file.write("set xlabel 'Time'\n")
        start_and_service_gnuplot_file.write("set yrange [0:]\n")
        start_and_service_gnuplot_file.write("set ylabel 'Task Duration'\n")
        start_and_service_gnuplot_file.write("plot '%s' using 1:2 with lp lw 4 notitle\n" %
                                             start_and_service_filename)
        start_and_service_gnuplot_file.close()

        self.plot_response_time_cdf(results_filename, output_directory)

        summary_file = open("%s" % aggregate_results_filename, 'a')
        summary_file.write("%s %s %s\n" % (get_percentile(response_times, .5),
                                         get_percentile(response_times, .95),
                                         get_percentile(response_times, .99)))
        summary_file.close()

    def plot_response_time_cdf(self, results_filename, output_directory):
        gnuplot_file = open("%s/response_time_cdf.gp" % output_directory, "w")
        gnuplot_file.write("set terminal postscript color\n")
        #gnuplot_file.write("set size 0.5,0.5\n")
        gnuplot_file.write("set output '%s/response_time_cdf.ps'\n" %
                           output_directory)
        gnuplot_file.write("set xlabel 'Response Time (ms)'\n")
        gnuplot_file.write("set ylabel 'Cumulative Probability'\n")
        gnuplot_file.write("set yrange [0:1]\n")
        gnuplot_file.write("plot '%s' using 2:1 lw 4 with lp\\\n" %
                           results_filename)
        gnuplot_file.close()

    def __reservation_enqueued(self, time, ip_address, num_queued_reservations):
        if ip_address not in self.__node_monitor_queue_lengths:
            self.__node_monitor_queue_lengths[ip_address] = []
        self.__node_monitor_queue_lengths[ip_address].append((time, num_queued_reservations))

    def __get_request(self, request_id):
        """ Gets the request from the map of requests.

        Creates a new request if a request with the given ID doesn't already
        exist.
        """
        if request_id not in self.__requests:
            self.__requests[request_id] = Request(request_id)
        return self.__requests[request_id]

def main(argv):
    PARAMS = ["log_dir", "output_dir", "start_sec", "end_sec", "aggregate_results_filename"]
    if "help" in argv[0]:
        print ("Usage: python parse_logs.py " +
               " ".join(["[%s=v]" % k for k in PARAMS]))
        return

    log_parser = LogParser()

    log_files = []
    output_dir = "experiment"
    aggregate_results_filename = ""
    for arg in argv:
        kv = arg.split("=")
        if kv[0] == PARAMS[0]:
            log_dir = kv[1]
            unqualified_log_files = filter(lambda x: "sparrow_audit" in x,
                                           os.listdir(log_dir))
            log_files = [os.path.join(log_dir, filename) for \
                         filename in unqualified_log_files]
        elif kv[0] == PARAMS[1]:
            output_dir = kv[1]
        elif kv[0] == PARAMS[2]:
            global START_SEC
            START_SEC = int(kv[1])
        elif kv[0] == PARAMS[3]:
            global END_SEC
            END_SEC = int(kv[1])
        elif kv[0] == PARAMS[4]:
            aggregate_results_filename = kv[1]
        else:
            print "Warning: ignoring parameter %s" % kv[0]

    if len(log_files) == 0:
        print "No valid log files found!"
        return

    if aggregate_results_filename == "":
        aggregate_results_filename = os.path.join(output_dir, "response_time_summary")

    logging.basicConfig(level=logging.DEBUG)

    for filename in log_files:
        log_parser.parse_file(filename)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    print "Outputting reservation queue length versus time"
    log_parser.output_reservation_queue_lengths(output_dir)

    print "Outputting tasks launched versus time"
    log_parser.output_tasks_launched_versus_time(output_dir)

    print "Outputting general results"

    log_parser.output_results(output_dir, aggregate_results_filename)


if __name__ == "__main__":
    main(sys.argv[1:])
