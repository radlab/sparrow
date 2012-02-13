""" Parses a log file and outputs aggregated information about the experiment.

All times are in milliseconds unless otherwise indicated.

TODO(kay): Generally, make things fail more gracefully, since it's possible
(and likely) that we'll see anomalies in the log files, but we still want to
get as much info as possible out of them.

TODO(kay): Add functionality to make response time vs. utilization graph.
"""
import logging
import os
import sys

import stats

INVALID_TIME = 0
INVALID_TIME_DELTA = -sys.maxint - 1

class Probe:
    def __init__(self, request_id, address):
        self.request_id = request_id
        self.address = address
        self.launch_time = INVALID_TIME
        self.received_time = INVALID_TIME
        self.completion_time = INVALID_TIME
        
    def set_launch_time(self, time):
        if self.launch_time != INVALID_TIME:
            self.logger.warn(("Probe for request %s on machine %s launched "
                              "twice; expect it to only launch once") %
                             self.request_id, self.address)
        self.launch_time = time
        
    def set_received_time(self, time):
        if self.received_time != INVALID_TIME:
            self.logger.warn(("Probe for request %s on machine %s received "
                              "twice; expect it to only be received once") %
                             self.request_id, self.address)
        self.received_time = time
        
    def set_completion_time(self, time):
        if self.completion_time != INVALID_TIME:
            self.logger.warn(("Probe for request %s on machine %s completed "
                              "twice; expect it to only launch once") %
                             self.request_id, self.address)
        self.completion_time = time
        
    def complete(self):
        """ Returns whether there's complete log information for this probe."""
        return (self.launch_time != INVALID_TIME and
                self.received_time != INVALID_TIME and
                self.completion_time != INVALID_TIME)
        
    def get_clock_skew(self):
        """ Returns the clock skew of the probed machine.
        
        Returns the number of milliseconds by which the probed machine is ahead
        of the probing machine.  The caller should verify that complete
        information is available for this probe before calling this function.
        
        Ignores processing time at the node monitor, which we assume to be
        small.
        """
        expected_received_time = (self.launch_time + self.completion_time) / 2.
        return self.received_time - expected_received_time
        

class Task:
    """ Class to store information about a task.
    
    We currently store two launch times: the time that the task was sent
    from the scheduler to the node monitor, and the time the task was sent
    from the node monitor to the application backend.  Under many
    implementations, these times should only differ by the network latency.
    """
    def __init__(self, id):
        self.logger = logging.getLogger("Task")
        self.scheduler_launch_time = INVALID_TIME
        self.node_monitor_launch_time = INVALID_TIME
        self.completion_time = INVALID_TIME
        # Estimate of the millis by which the machine this task ran on is
        # ahead of the node the task was scheduled from.
        self.clock_skew = INVALID_TIME_DELTA
        # Address of the machine that the task ran on.
        self.address = ""
        self.id = id
        
    def set_scheduler_launch_time(self, time):
        if self.scheduler_launch_time != INVALID_TIME:
            self.logger.warn(("Task %s launched at scheduler twice; expect "
                              "task to only launch once") % id)
        self.scheduler_launch_time = time
        
    def set_node_monitor_launch_time(self, address, time):
        if self.node_monitor_launch_time != INVALID_TIME:
            self.logger.warn(("Task %s launched at %s twice; expect task to "
                              "only launch once") % (id, address))
        self.node_monitor_launch_time = time
        self.address = address
        
    def set_completion_time(self, time):
        if self.completion_time != INVALID_TIME:
            self.logger.warn(("Task %s completed twice; "
                              "expect task to only complete once") % id)
        self.completion_time = time
        
    def network_delay(self):
        """ Returns the network delay (as the difference between launch times).
        
        In the presence of clock skew, this may be negative. The caller should
        ensure that complete information is available for this task before
        calling this function.
        """
        return (self.node_monitor_launch_time - self.clock_skew -
                self.scheduler_launch_time)

    def processing_time(self):    
        """ Returns the processing time (time executing on backend)."""

        return (self.completion_time - self.node_monitor_launch_time)
    
    def complete(self):
        """ Returns whether we have complete information on this task. """
        return (self.scheduler_launch_time != INVALID_TIME and
                self.node_monitor_launch_time != INVALID_TIME and
                self.completion_time != INVALID_TIME and
                self.clock_skew != INVALID_TIME_DELTA)

class Request:
    def __init__(self, id):
        self.__id = id
        self.__num_tasks = 0
        self.__arrival_time = INVALID_TIME
        self.__tasks = {}
        # Map of machine addresses to probes.
        self.__probes = {}
        self.logger = logging.getLogger("Request")
        
    def set_num_tasks(self, num_tasks):
        self.__num_tasks = num_tasks
        
    def set_arrival_time(self, time):
        self.__arrival_time = time
        
    def add_probe_launch(self, address, time):
        probe = self.__get_probe(address)
        probe.set_launch_time(time)
        
    def add_probe_received(self, address, time):
        probe = self.__get_probe(address)
        probe.set_received_time(time)
        
    def add_probe_completion(self, address, time):
        probe = self.__get_probe(address)
        probe.set_completion_time(time)
        
    def add_scheduler_task_launch(self, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_scheduler_launch_time(launch_time)
        
    def add_node_monitor_task_launch(self, address, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_node_monitor_launch_time(address, launch_time)
        
    def add_task_completion(self, task_id, completion_time):
        # We might see a task completion before a task launch, depending on the
        # order that we read log files in.
        task = self.__get_task(task_id)
        task.set_completion_time(completion_time)
        
    def set_clock_skews(self):
        """ Sets the clock skews for all tasks. """
        for task in self.__tasks.values():
            if task.address not in self.__probes:
                self.logger.warn(("No probe information for request %s, "
                                  "machine %s") % (self.__id, task.address))
                continue
            probe = self.__probes[task.address]
            if not probe.complete():
                self.logger.warn(("Probe information for request %s, machine "
                                  "%s incomplete") % (self.__id, task.address))
            else:
                task.clock_skew = probe.get_clock_skew()
        
    def network_delays(self):
        """ Returns a list of delays for all __tasks with delay information. """
        network_delays = []
        for task in self.__tasks.values():
            if task.complete():
                network_delays.append(task.network_delay())
        return network_delays
    
    def processing_times(self):
        """ Returns a list of processing times for all __tasks with
            complete information. """
        processing_times = []
        for task in self.__tasks.values():
            if task.complete():
                processing_times.append(task.processing_time())
        return processing_times

    def response_time(self):
        """ Returns the time from when the task arrived to when it completed.
        
        Returns -1 if we don't have completion information on the task.  Note
        that we may have information about when the task completed, but not
        complete information about the task (e.g. we don't know when the task
        was launched).
        """
        if self.__arrival_time == INVALID_TIME:
            self.logger.debug("Request %s missing arrival time" % self.__id)
            return -1
        completion_time = self.__arrival_time
        for task_id, task in self.__tasks.items():
            if task.completion_time == INVALID_TIME:
                self.logger.debug(("Task %s in request %s missing completion "
                                   "time") % (task_id, self.__id))
                return INVALID_TIME_DELTA
            # Here we compare two event times: the completion time, as observed
            # the the node monitor, minus the clock skew; and the job arrival
            # time, as observed by the scheduler.  If the adjusted completion
            # time is before the arrival time, we know we've made an error in
            # calculating the clock skew.clock_skew
            adjusted_completion_time = task.completion_time - task.clock_skew
            if adjusted_completion_time < self.__arrival_time:
                self.logger.warn(("Task %s in request %s has estimated "
                                  "completion time before arrival time, "
                                  "indicating inaccuracy in clock skew "
                                  "computation.") % (task_id, self.__id))
            completion_time = max(completion_time, adjusted_completion_time)
        return completion_time - self.__arrival_time
        
    def complete(self):
        """ Returns whether we have complete info for the request.
        
        Due to incomplete log files, it's possible that we'll have completion
        information but not start information for a job. """
        if (self.__num_tasks == 0 or
            self.__arrival_time == 0 or
            self.__num_tasks != len(self.__tasks)):
            return False
        for task in self.__tasks:
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
        self.logger = logging.getLogger("LogParser")
        
    def parse_file(self, filename):
        file = open(filename, "r")
        for line in file:
            # Strip off the newline at the end of the line.
            items = line[:-1].split("\t")
            if len(items) != 3:
                self.logger.warn(("Ignoring log message '%s' with unexpected "
                                  "number of items (expected 3; found %d)") %
                                 (line, len(items)))
                continue
            
            # Time is expressed in epoch milliseconds.
            time = int(items[self.TIME_INDEX])
            
            audit_event_params = items[self.AUDIT_EVENT_INDEX].split(":")
            if audit_event_params[0] == "arrived":
                self.__add_request_arrival(audit_event_params[1],
                                           audit_event_params[2], time)
            elif audit_event_params[0] == "probe_launch":
                request = self.__get_request(audit_event_params[1])
                request.add_probe_launch(audit_event_params[2], time)
            elif audit_event_params[0] == "probe_received":
                request = self.__get_request(audit_event_params[1])
                request.add_probe_received(audit_event_params[2], time)
            elif audit_event_params[0] == "probe_completion":
                request = self.__get_request(audit_event_params[1])
                request.add_probe_completion(audit_event_params[2], time)
            elif audit_event_params[0] == "scheduler_launch":
                self.__add_scheduler_task_launch(audit_event_params[1],
                                                 audit_event_params[2], time)
            elif audit_event_params[0] == "nodemonitor_launch":
                self.__add_node_monitor_task_launch(audit_event_params[1],
                                                    audit_event_params[2],
                                                    audit_event_params[3],
                                                    time)
            elif audit_event_params[0] == "task_completion":
                self.__add_task_completion(audit_event_params[1],
                                           audit_event_params[2], time)
                
    def output_results(self, file_prefix):
        # Response time is the time from when the job arrived at a scheduler
        # to when it completed.
        response_times = []
        # Network/processing delay for each task.
        network_delays = []
        processing_times = []
        for request in self.__requests.values():
            request.set_clock_skews()
            network_delays.extend(request.network_delays())
            processing_times.extend(request.processing_times())
            response_time = request.response_time()
            if response_time != INVALID_TIME_DELTA:
                response_times.append(response_time)
            
        results_filename = "%s_results.data" % file_prefix
        file = open(results_filename, "w")
        file.write("%ile\tResponseTime\tNetworkDelay\tProcessingTime\n")
        num_data_points = 100
        response_times.sort()
        network_delays.sort()
        processing_times.sort()
        response_stride = max(1, len(response_times) / num_data_points)
        network_stride = max(1, len(network_delays) / num_data_points)
        processing_stride = max(1, len(processing_times) / num_data_points)
        for i, (response_time, network_delay, processing_time) in enumerate(
                zip(response_times[::response_stride],
                    network_delays[::network_stride],
                    processing_times[::processing_stride])):
            percentile = (i + 1) * response_stride * 1.0 / len(response_times)
            file.write("%f\t%d\t%d\t%d\n" % (percentile, response_time, network_delay, processing_time))
        file.close()
        
        self.plot_response_time_cdf(results_filename, file_prefix)
        
    def plot_response_time_cdf(self, results_filename, file_prefix):
        gnuplot_file = open("%s_response_time_cdf.gp" % file_prefix, "w")
        gnuplot_file.write("set terminal postscript color\n")
        gnuplot_file.write("set size 0.5,0.5\n")
        gnuplot_file.write("set output '%s_response_time_cdf.ps'\n" %
                           file_prefix)
        gnuplot_file.write("set xlabel 'Response Time'\n")
        gnuplot_file.write("set ylabel 'Cumulative Probability'\n")
        gnuplot_file.write("set yrange [0:1]\n")
        gnuplot_file.write("plot '%s' using 2:1 lw 4 with lp\\\n" %
                           results_filename)
        gnuplot_file.close()
        
    def __get_request(self, request_id):
        """ Gets the request from the map of requests.
        
        Creates a new request if a request with the given ID doesn't already
        exist.
        """
        if request_id not in self.__requests:
            self.__requests[request_id] = Request(request_id)
        return self.__requests[request_id]
        
    def __add_request_arrival(self, request_id, num_tasks, time):
        request = self.__get_request(request_id)
        request.set_num_tasks(num_tasks)
        request.set_arrival_time(time)
        
    def __add_scheduler_task_launch(self, request_id, task_id, time):
        request = self.__get_request(request_id)
        request.add_scheduler_task_launch(task_id, time)
        
    def __add_node_monitor_task_launch(self, request_id, address, task_id,
                                       time):
        request = self.__get_request(request_id)
        request.add_node_monitor_task_launch(address, task_id, time)
        
    def __add_task_completion(self, request_id, task_id, time):
        request = self.__get_request(request_id)
        request.add_task_completion(task_id, time)

def main(argv):
    PARAMS = ["log_dir", "output_file"]
    if "help" in argv[0]:
        print ("Usage: python parse_logs.py " +
               " ".join(["[%s=v]" % k for k in PARAMS]))
        return
        
    log_parser = LogParser()
    
    log_files = []
    output_filename = "experiment"
    for arg in argv:
        kv = arg.split("=")
        if kv[0] == PARAMS[0]:
            log_dir = kv[1]
            unqualified_log_files = filter(lambda x: "sparrow_audit" in x,
                                           os.listdir(log_dir))
            log_files = [os.path.join(log_dir, filename) for \
                         filename in unqualified_log_files]
        elif kv[0] == PARAMS[1]:
            output_filename = kv[1]
        else:
            print "Warning: ignoring parameter %s" % kv[0]
            
    if len(log_files) == 0:
        print "No valid log files found!"
        return
    
    logging.basicConfig(level=logging.DEBUG)
        
    for filename in log_files:
        log_parser.parse_file(filename)
        
    log_parser.output_results(output_filename)
    

if __name__ == "__main__":
    main(sys.argv[1:])
