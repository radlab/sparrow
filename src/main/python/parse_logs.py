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
        self.id = id
        
    def set_scheduler_launch_time(self, time):
        if self.scheduler_launch_time != INVALID_TIME:
            self.logger.warn(("Task %s launched at scheduler twice; expect "
                              "task to only launch once") % id)
        self.scheduler_launch_time = time
        
    def set_node_monitor_launch_time(self, time):
        if self.node_monitor_launch_time != INVALID_TIME:
            self.logger.warn(("Task %s launched at node monitor twice; "
                              "expect task to only launch once") % id)
        self.node_monitor_launch_time = time
        
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
        return self.node_monitor_launch_time - self.scheduler_launch_time
        
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
        self.logger = logging.getLogger("Request")
        
    def set_num_tasks(self, num_tasks):
        self.__num_tasks = num_tasks
        
    def set_arrival_time(self, time):
        self.__arrival_time = time
        
    def add_scheduler_task_launch(self, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_scheduler_launch_time(launch_time)
        
    def add_node_monitor_task_launch(self, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_node_monitor_launch_time(launch_time)
        
    def add_task_completion(self, task_id, completion_time):
        # We might see a task completion before a task launch, depending on the order
        # that we read log files in.
        task = self.__get_task(task_id)
        task.set_completion_time(completion_time)
        
    def network_delays(self):
        """ Returns a list of delays for all __tasks with delay information. """
        network_delays = []
        for task in self.__tasks.values():
            if task.complete():
                network_delays.append(task.network_delay())
        return network_delays
    
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
            assert task.completion_time >= self.__arrival_time
            completion_time = max(completion_time, task.completion_time)
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
            items = line.split("\t")
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
            elif audit_event_params[0] == "scheduler_launch":
                self.__add_scheduler_task_launch(audit_event_params[1],
                                                 audit_event_params[2], time)
            elif audit_event_params[0] == "nodemonitor_launch":
                self.__add_node_monitor_task_launch(audit_event_params[1],
                                                    audit_event_params[2],
                                                    time)
            elif audit_event_params[0] == "task_completion":
                self.__add_task_completion(audit_event_params[1],
                                           audit_event_params[2], time)
                
    def output_results(self, file_prefix):
        # Response time is the time from when the job arrived at a scheduler
        # to when it completed.
        response_times = []
        # Network delay for each task.
        network_delays = []
        for request in self.__requests.values():
            network_delays.extend(request.network_delays())

            response_time = request.response_time()
            if response_time != INVALID_TIME_DELTA:
                response_times.append(response_time)
            
        results_filename = "%s_results.data" % file_prefix
        file = open(results_filename, "w")
        file.write("%ile\tResponseTime\tNetworkDelay\n")
        num_data_points = 100
        response_times.sort()
        network_delays.sort()
        response_stride = max(1, len(response_times) / num_data_points)
        network_stride = max(1, len(network_delays) / num_data_points)
        for i, (response_time, network_delay) in enumerate(
                zip(response_times[::response_stride],
                    network_delays[::network_stride])):
            percentile = (i + 1) * response_stride * 1.0 / len(response_times)
            file.write("%f\t%d\t%d\n" % (percentile, response_time, network_delay))
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
        
    def __add_node_monitor_task_launch(self, request_id, task_id, time):
        request = self.__get_request(request_id)
        request.add_node_monitor_task_launch(task_id, time)
        
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