""" Parses a log file and outputs aggregated information about the experiment.

All times are in milliseconds unless otherwise indicated.

TODO(kay): Generally, make things fail more gracefully, since it's possible
(and likely) that we'll see anomalies in the log files, but we still want to
get as much info as possible out of them.
"""
import functools
import logging
import math
import os
import re
import sys
import stats
import time

INVALID_TIME = 0
INVALID_TIME_DELTA = -sys.maxint - 1
INVALID_QUEUE_LENGTH = -1
INVALID_ID = -1
TPCH_QUERY_ID_REGEX = re.compile('--(\d+) (\d+)--')

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

class Probe:
    def __init__(self, request_id, address):
        self.request_id = request_id
        self.address = address
        self.launch_time = INVALID_TIME
        self.received_time = INVALID_TIME
        self.completion_time = INVALID_TIME
        self.queue_length = INVALID_QUEUE_LENGTH
        self.__logger = logging.getLogger("Probe")

    def set_queue_length(self, queue_length):
        if self.queue_length != INVALID_QUEUE_LENGTH:
            self.__logger.warn(("Two queue lengths for request %s on machine %s"
                                "expected only launch one") %
                              (self.request_id, self.address))
        self.queue_length = int(queue_length)

    def set_launch_time(self, time):
        if self.launch_time != INVALID_TIME:
            self.__logger.warn(("Probe for request %s on machine %s launched "
                              "twice; expect it to only launch once") %
                              (self.request_id, self.address))
        self.launch_time = time

    def set_received_time(self, time):
        if self.received_time != INVALID_TIME:
            self.__logger.warn(("Probe for request %s on machine %s received "
                              "twice; expect it to only be received once") %
                             (self.request_id, self.address))
        self.received_time = time

    def set_completion_time(self, time):
        if self.completion_time != INVALID_TIME:
            self.__logger.warn(("Probe for request %s on machine %s completed "
                              "twice; expect it to only launch once") %
                             (self.request_id, self.address))
        self.completion_time = time

    def complete(self):
        """ Returns whether there's complete log information for this probe."""
	if self.launch_time == INVALID_TIME:
	  print "Missing probe launch time"
          return False
        if self.received_time == INVALID_TIME:
          print "Missing probe received time"
          return False
        if self.completion_time == INVALID_TIME:
          print "Missing probe completion time"
          return False
        return True

    def round_trip_time(self):
        """ Returns the delay experienced by the probing machine. """
        return self.completion_time - self.launch_time

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

			  We store a variety of events corresponding to each task launch,
        as described in __init__
    """
    def __init__(self, id):
        self.__logger = logging.getLogger("Task")

        # When the scheduler (resident with the frontend) launched the task
        self.scheduler_launch_time = INVALID_TIME
        # When the node monitor queued the task for execution
        self.node_monitor_submit_time = INVALID_TIME
        # When the node monitor (resident with the backend) launched the task
        self.node_monitor_launch_time = INVALID_TIME
        # When the backend completed the task
        self.completion_time = INVALID_TIME
        # Estimate of the millis by which the machine this task ran on is
        # ahead of the node the task was scheduled from.
        self.clock_skew = INVALID_TIME_DELTA
        # Address of the machine that the task ran on.
        self.address = ""
        self.id = id

    def set_scheduler_launch_time(self, time):
        if self.scheduler_launch_time != INVALID_TIME:
            self.__logger.warn(("Task %s launched at scheduler twice; expect "
                              "task to only launch once") % id)
        self.scheduler_launch_time = time

    def set_node_monitor_launch_time(self, address, time):
        if self.node_monitor_launch_time != INVALID_TIME:
            self.__logger.warn(("Task %s launched at %s twice; expect task to "
                              "only launch once") % (id, address))
        self.node_monitor_launch_time = time

    def set_node_monitor_submit_time(self, address, time):
        if self.node_monitor_submit_time != INVALID_TIME:
            self.__logger.warn(("Task %s submitted at %s twice; expect task to "
                                "only submit once") % (id, address))
        self.node_monitor_submit_time = time
        self.address = address

    def set_completion_time(self, time):
        if self.completion_time != INVALID_TIME:
            self.__logger.warn(("Task %s completed twice; "
                              "expect task to only complete once") % id)
        self.completion_time = time

    def network_delay(self):
        """ Returns the network delay (as the difference between launch times).

        In the presence of clock skew, this may be negative. The caller should
        ensure that complete information is available for this task before
        calling this function.
        """
        return (self.node_monitor_submit_time - self.clock_skew -
                self.scheduler_launch_time)

    def queued_time(self):
        """ Returns the time spent waiting to launch on the backend. """
        return (self.node_monitor_launch_time - self.node_monitor_submit_time)

    def service_time(self):
        """ Returns the service time (time executing on backend)."""
        return (self.completion_time - self.node_monitor_launch_time)

    def complete(self):
        """ Returns whether we have complete information on this task. """
        if self.scheduler_launch_time == INVALID_TIME:
          print "Missing task scheduler launch time"
          return False
        if self.node_monitor_launch_time == INVALID_TIME:
	  print "Missing task node monitor launch time"
	  return False
	if self.completion_time == INVALID_TIME:
	  print "Missing task completion time"
	  return False
	if self.clock_skew == INVALID_TIME_DELTA:
          print "Missing task clock skew"
	  return False
	return True

class Request:
    def __init__(self, id):
        self.__id = id
        self.__num_tasks = 0
        self.__arrival_time = INVALID_TIME
        self.__tasks = {}
        # Map of machine addresses to probes.
        self.__probes = {}
        # Address of the scheduler that received the request (and placed it).
        self.__scheduler_address = ""
        self.__logger = logging.getLogger("Request")
        
        self.__user = ""

        # TPCH query number.
        self.tpch_id = INVALID_ID
        # Unique query identifier assigned by shark (used to differentiate
        # different executions of the same TPC-H query).
        self.shark_id = INVALID_ID
        # Spark stage ID
        self.stage_id = INVALID_ID
        self.constrained = False

    def __str__(self):
        ret = "ID %s SHARK %s (stage %s), Constrained %s TPCH %s, %s tasks: " % (self.__id, self.shark_id, self.stage_id, self.constrained, self.tpch_id, len(self.__tasks))
        return ret

    def probe_stats(self):
        probe_items_sorted = \
          sorted(self.__probes.items(), key=lambda k:k[1].completion_time)
        # print ["%s " % x[1].completion_time for x in probe_items_sorted]
        machines_sorted = [k[0] for k in probe_items_sorted]

        out = []
        for (i, machine) in enumerate(machines_sorted):
            for task in self.__tasks.values():
                if task.address == machine:
                    out.append(i)
        return out


    def add_arrival(self, time, num_tasks, address, user, description, constrained):
        self.__arrival_time = time
        self.__num_tasks = int(num_tasks)
        self.__scheduler_address = address
        self.constrained = False
        if constrained == "true":
          self.constrained = True
        description_parts = description.split("-")
        if len(description_parts) < 2:
            print "Description not formatted as Spark/Shark description: " + description
        else:
            self.stage_id = description_parts[-1]
            match = TPCH_QUERY_ID_REGEX.search(description)
            if match == None:
                is_warmup_query = (description.find("SPREAD_EVENLY") != -1)
                is_create_table_query = (description.find("create table denorm") != -1)
                if not (is_warmup_query or is_create_table_query):
                    self.__logger.warn("Couldn't find TPCH query id in description: %s" % description)
                return
            # An identifier that's unique for the Shark driver, but not across all drivers.
            self.shark_id = match.group(1)
            self.tpch_id = match.group(2)
            #print ("Shark ID: %s, stage id: %s, TPCH id: %s for description %s" %
            #    (self.shark_id, self.stage_id, self.tpch_id, description))

    def add_probe_launch(self, address, time):
        probe = self.__get_probe(address)
        probe.set_launch_time(time)

    def add_probe_received(self, address, time):
        probe = self.__get_probe(address)
        probe.set_received_time(time)

    def add_probe_completion(self, address, queue_length, time):
        probe = self.__get_probe(address)
        probe.set_completion_time(time)
        probe.set_queue_length(queue_length)

    def add_scheduler_task_launch(self, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_scheduler_launch_time(launch_time)

    def add_node_monitor_task_launch(self, address, task_id, launch_time):
        task = self.__get_task(task_id)
        task.set_node_monitor_launch_time(address, launch_time)

    def add_node_monitor_task_submit(self, address, task_id, submit_time):
        task = self.__get_task(task_id)
        task.set_node_monitor_submit_time(address, submit_time)

    def add_task_completion(self, task_id, completion_time):
        # We might see a task completion before a task launch, depending on the
        # order that we read log files in.
        task = self.__get_task(task_id)
        task.set_completion_time(completion_time)

    def set_clock_skews(self):
        """ Sets the clock skews for all tasks. """
        for task in self.__tasks.values():
            if task.address not in self.__probes:
                #print self.__probes.keys()
                #self.__logger.warn(("No probe information for request %s, "
                #                  "machine %s") % (self.__id, task.address))
                continue
            probe = self.__probes[task.address]
            if not probe.complete():
                self.__logger.warn(("Probe information for request %s, machine "
                                  "%s incomplete") % (self.__id, task.address))
            else:
                task.clock_skew = probe.get_clock_skew()

    def arrival_time(self):
        """ Returns the time at which the task arrived at the scheduler. """
        return self.__arrival_time

    def scheduler_address(self):
        return self.__scheduler_address

    def clock_skews(self):
        """ Returns a map of machines to clock skews.

        Clock skews are given relative to the scheduler.
        """
        clock_skews = {}
        for address, probe in self.__probes.items():
            clock_skews[address] = probe.get_clock_skew()
        return clock_skews

    def network_delays(self):
        """ Returns a list of delays for all __tasks with delay information. """
        network_delays = []
        for task in self.__tasks.values():
            if task.complete():
                network_delays.append(task.network_delay())
                if task.network_delay() > 20:
                  print "Network delay over 20ms for %s" % self.__id
                  #print task.node_monitor_submit_time
                  #print task.scheduler_launch_time
                  #print task.clock_skew
                  #print task.id
                  #print task.address
                  #print
        return network_delays

    def service_times(self):
        """ Returns a list of service times for complete __tasks. """
        service_times = []
        for task in self.__tasks.values():
            if task.complete():
                x = task.service_time()
                service_times.append(task.service_time())
        return service_times

    def optimal_response_time(self):
        return max([t.service_time() for t in self.__tasks.values()])

    def queue_times(self):
        """ Returns a list of queue times for all complete __tasks. """
        queue_times = []
        for task in self.__tasks.values():
            if task.complete():
                queue_times.append(task.queued_time())
        return queue_times

    def probe_times(self):
        """ Returns a list of probe delays for all complete __probes. """
        probe_times = []
        for probe in self.__probes.values():
            if probe.complete():
                if probe.round_trip_time() > 20:
                  "Long probe: %s " %self.__id
                probe_times.append(probe.round_trip_time())
        return probe_times

    def receive_and_probing_time(self):
        """ Returns the time from when the request arrived until the last probe completed. """
        latest_completion = 0
        for probe in self.__probes.values():
		  	    if probe.complete():
			  		    latest_completion = max(latest_completion, probe.completion_time)
        return latest_completion - self.__arrival_time

    def queue_lengths(self):
       """ Returns an array of queue lengths observed during all probes. """
       out = []
       for probe in self.__probes.values():
         if probe.complete():
           out.append(probe.queue_length)
       return out

    def probing_time(self):
       """ Returns the total time spent in probing for this request. """
       earliest_launch = (time.time() * 1000)**2
       latest_completion = 0
       for probe in self.__probes.values():
           if probe.complete():
               earliest_launch = min(earliest_launch, probe.launch_time)
               latest_completion = max(latest_completion, probe.completion_time)
       return latest_completion - earliest_launch

    def worst_necessary_probe_time(self):
      """ Returns the nth longest probe time, where n is the number of tasks.

          This represents the thoeretical limit of the minimum time we could
          have spent probing that actually gets a probe from as many machines
          as we have tasks. Note that in practice, if we are only waiting
          for ||tasks|| machines to respond we might as well just send the
          tasks randomly. """
      probe_times = self.probe_times()
      num_tasks = len(self.__tasks)
      if len(probe_times) < num_tasks:
        self.__logger.warn("Fewer probes send than tasks for task %s."
                           % self.__id)

      if len(probe_times) == 0:
        return 0
      return sorted(probe_times)[min(num_tasks - 1, len(probe_times) - 1)]

    def response_time(self, incorporate_skew=True):
        """ Returns the time from when the task arrived to when it completed.

        Returns -1 if we don't have completion information on the task.  Note
        that we may have information about when the task completed, but not
        complete information about the task (e.g. we don't know when the task
        was launched).

        Arguments:
            incorporate_skew: Boolean specifying whether to incorporate the
                perceived skew in the response time.
        """
        if self.__arrival_time == INVALID_TIME:
            self.__logger.debug("Request %s missing arrival time" % self.__id)
            return -1
        completion_time = self.__arrival_time
        for task_id, task in self.__tasks.items():
            if task.completion_time == INVALID_TIME:
                self.__logger.debug(("Task %s in request %s missing completion "
                                   "time") % (task_id, self.__id))
                return INVALID_TIME_DELTA
            task_completion_time = task.completion_time
            if incorporate_skew:
                task_completion_time -= task.clock_skew
                # Here we compare two event times: the completion time, as
                # observed the the node monitor, minus the clock skew; and the
                # job arrival time, as observed by the scheduler.  If the
                # adjusted completion time is before the arrival time, we know
                # we've made an error in calculating the clock skew.clock_skew
                if task_completion_time < self.__arrival_time:
                    self.__logger.warn(("Task %s in request %s has estimated "
                                        "completion time before arrival time, "
                                        "indicating inaccuracy in clock skew "
                                        "computation.") % (task_id, self.__id))
            else:
             	if task.scheduler_launch_time > task.node_monitor_launch_time:
								self.__logger.warn("Task %s suggests clock skew: " % task_id)
            completion_time = max(completion_time, task_completion_time)

        if (completion_time - self.__arrival_time) > 2000:
          pass
          """
          print "TRUE: %s" % (completion_time - self.__arrival_time)
          print self.network_delays()
          print self.service_times()
          print self.probing_time()
          print "EST: %s" % (max(self.service_times()) + max(self.network_delays()) + self.probing_time())
          """
        return completion_time - self.__arrival_time

    def complete(self):
        """ Returns whether we have complete info for the request.

        Due to incomplete log files, it's possible that we'll have completion
        information but not start information for a job. """
        if (self.__num_tasks == 0 or
            self.__arrival_time == 0 or
            self.__num_tasks != len(self.__tasks)):
            print ("Expected to find %s tasks; found %s" %
                   (self.__num_tasks, len(self.__tasks)))
            return False
        for task in self.__tasks.values():
            if not task.complete():
                return False
        if len(self.__probes) == 0:
          return False # Don't consider non-probing requests
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

    def earliest_time(self):
        return self.__earliest_time

    def get_requests(self):
        return self.__requests

    def parse_file(self, filename):
        file = open(filename, "r")
        for line in file:
            # Strip off the newline at the end of the line.
            items = line[:-1].split("\t")

            # Time is expressed in epoch milliseconds.
            time = int(items[self.TIME_INDEX])
            self.__earliest_time = min(self.__earliest_time, time)

            audit_event_params = " ".join(items[self.AUDIT_EVENT_INDEX:]).split(":")
            if audit_event_params[0] == "arrived":
                request = self.__get_request(audit_event_params[1])
                request.add_arrival(time, audit_event_params[2],
                                    audit_event_params[3], audit_event_params[5],
                                    audit_event_params[6], audit_event_params[7])
            elif audit_event_params[0] == "probe_launch":
                request = self.__get_request(audit_event_params[1])
                request.add_probe_launch(audit_event_params[2], time)
            elif audit_event_params[0] == "probe_received":
                request = self.__get_request(audit_event_params[1])
                request.add_probe_received(audit_event_params[2], time)
            elif audit_event_params[0] == "probe_completion":
                request = self.__get_request(audit_event_params[1])
                request.add_probe_completion(audit_event_params[2],
                                             audit_event_params[3], time)
            elif audit_event_params[0] == "scheduler_launch":
                self.__add_scheduler_task_launch(audit_event_params[1],
                                                 audit_event_params[2], time)
            elif audit_event_params[0] == "nodemonitor_task_runnable":
                self.__add_node_monitor_task_launch(audit_event_params[1],
                                                    audit_event_params[2],
                                                    audit_event_params[3],
                                                    time)
            elif audit_event_params[0] == "nodemonitor_task_submitted":
                self.__add_node_monitor_task_submit(audit_event_params[1],
                                                    audit_event_params[2],
                                                    audit_event_params[3],
																										time)
            elif audit_event_params[0] == "nodemonitor_task_completed":
                # First param = ip address is not used
                self.__add_task_completion(audit_event_params[1],
                                           audit_event_params[2], time)
            else:
              print "Unexpected event: " + audit_event_params[0]

    def output_results(self, file_prefix, constrained=""):
        # Response time is the time from when the job arrived at a scheduler
        # to when it completed.
        response_times = []
        # Network/processing delay for each task.
        network_delays = []
        service_times = []
        queue_times = []
        probe_times = []
        probing_times = []
        queue_lengths = []
        rcv_probing_times = []
        worst_probe_times = []
        overheads = []
        # Store clock skews as a map of pairs of addresses to a list of
        # (clock skew, time) pairs. Store addresses in tuple in increasing
        # order, so that we get the clock skew calculated in both directions.
        clock_skews = {}
        start_time = self.__earliest_time + (START_SEC * 1000)
        end_time = self.__earliest_time + (END_SEC * 1000)
        for request in self.__requests.values():
          request.set_clock_skews()

        complete_requests = filter(lambda k: k.complete(),
                                   self.__requests.values())
        print "%s complete requests" % len(complete_requests)
        considered_requests = filter(lambda k: k.arrival_time() >= start_time and
                                     k.arrival_time() <= end_time and
                                     k.complete(),
                                     self.__requests.values())
        if constrained == "constrained":
          considered_requests = filter(lambda x: x.constrained, self.__requests.values())
        elif constrained == "unconstrained":
          considered_requests = filter(lambda x: not x.constrained, self.__requests.values())
        print "Included %s requests" % len(considered_requests)
        print "Excluded %s requests" % (len(self.__requests.values()) - len(considered_requests))
        for request in considered_requests:
            scheduler_address = request.scheduler_address()
            for address, probe_skew in request.clock_skews().items():
                if address > scheduler_address:
                    address_pair = (scheduler_address, address)
                    skew = probe_skew
                else:
                    address_pair = (address, scheduler_address)
                    skew = -probe_skew
                if address_pair not in clock_skews:
                    clock_skews[address_pair] = []
                clock_skews[address_pair].append((skew,
                                                  request.arrival_time()))

            network_delays.extend(request.network_delays())
            service_times.extend(request.service_times())
            queue_times.extend(request.queue_times())
            response_time = request.response_time()
            probe_times.extend(request.probe_times())
            probing_times.append(request.probing_time())
            queue_lengths.extend(request.queue_lengths())
            rcv_probing_times.append(request.receive_and_probing_time())
            worst_probe_times.append(request.worst_necessary_probe_time())
            response_times.append(response_time)
            overheads.append(response_time - request.optimal_response_time())

        # Output data for response time and network delay CDFs.
        results_filename = "%s_results%s.data" % (file_prefix, constrained)
        file = open(results_filename, "w")
        file.write("%ile\tResponseTime\tNetworkDelay\tServiceTime\tQueuedTime\tProbeTime\tRcvProbingTime\tProbingTime\tWorstProbeTime\tQueueLength\tOverhead\n")
        num_data_points = 100
        response_times.sort()
        network_delays.sort()
        service_times.sort()
        queue_times.sort()
        queue_lengths.sort()
        probe_times.sort()
        probing_times.sort()
        rcv_probing_times.sort()
        worst_probe_times.sort()
        overheads.sort()


        for i in range(100):
            i = float(i) / 100
            file.write("%f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" % (i,
                get_percentile(response_times, i),
                get_percentile(network_delays, i),
                get_percentile(service_times, i),
                get_percentile(queue_times, i),
                get_percentile(probe_times, i),
                get_percentile(rcv_probing_times, i),
                get_percentile(probing_times, i),
                get_percentile(worst_probe_times, i),
                get_percentile(queue_lengths, i),
                get_percentile(overheads, i)))

        file.close()
        return
        self.plot_response_time_cdf(results_filename, file_prefix)
        """
        # Output data about clock skews.  Currently this writes a different
        # file for each pair of machines; we may want to change this when
        # we do larger experiments.
        skew_filenames = []
        for address_pair, skews in clock_skews.items():
            skews.sort(key=lambda x: x[0])
            filename = "%s_%s_%s_skew.data" % (file_prefix, address_pair[0],
                                               address_pair[1])
            skew_filenames.append(filename)
            file = open(filename, "w")
            stride = max(1, len(skews) / num_data_points)
            for i, (skew, time) in enumerate(skews[::stride]):
                percentile = (i + 1) * stride * 1.0 / len(skews)
                file.write("%f\t%d\t%d\n" % (percentile, skew, time))
        self.plot_skew_cdf(skew_filenames, file_prefix)
        """
        summary_file = open("%s_response_time_summary" % file_prefix, 'w')
        summary_file.write("%s %s %s" % (get_percentile(response_times, .5),
                                         get_percentile(response_times, .95),
                                         get_percentile(response_times, .99)))
        summary_file.close()


        wait_times_per_queue_len = {}
        for request in considered_requests:
          for task in request._Request__tasks.values():
            wait_time = task.queued_time()
            if task.address not in request._Request__probes:
              print "Excluding"
              continue
            queue_length = request._Request__probes[task.address].queue_length
            arr = wait_times_per_queue_len.get(queue_length, [])
            arr.append(wait_time)
            wait_times_per_queue_len[queue_length] = arr

        # Queue length vs response time
        files = [] # (file name, queue length, # items)
        for (queue_len, waits) in wait_times_per_queue_len.items():
          fname = "queue_waits_%s.txt" % queue_len
          files.append((fname, queue_len, len(waits)))
          f = open(fname, 'w')
          waits.sort()
          for (i, wait) in enumerate(waits):
            f.write("%s\t%s\n" % (float(i)/len(waits), wait))
          f.close()
        plot_fname = "wait_time.gp"
        plot_file = open(plot_fname, 'w')
        plot_file.write("set terminal postscript color\n")
        plot_file.write("set output 'wait_time.ps'\n")
        plot_file.write("set xrange [0:500]\n")
        parts = map(lambda x: "'%s' using 2:1 with lines lw 3 title '%s (n=%s)'"
          % (x[0], x[1], x[2]), files)
        plot = "plot " + ",\\\n".join(parts)
        plot_file.write(plot + "\n")
        plot_file.close()

    def plot_skew_cdf(self, skew_filenames, file_prefix):
        gnuplot_file = open("%s_skew_cdf.gp" % file_prefix, "w")
        gnuplot_file.write("set terminal postscript color\n")
        gnuplot_file.write("set output '%s_skew_cdf.ps'\n" %
                           file_prefix)
        gnuplot_file.write("set xlabel 'Clock Skew (ms)'\n")
        gnuplot_file.write("set ylabel 'Cumulative Probability'\n")
        gnuplot_file.write("set y2label 'Arrival Time (ms)'\n")
        gnuplot_file.write("set yrange [0:1]\n")
        gnuplot_file.write("set ytics nomirror\n")
        gnuplot_file.write("set y2tics\n")
        gnuplot_file.write("plot ")
        for i, results_filename in enumerate(skew_filenames):
            if i > 0:
                gnuplot_file.write(",\\\n")
            gnuplot_file.write(("'%s' using 2:1 lw 4 with lp axis "
                                "x1y1,\\\n") % results_filename)
            gnuplot_file.write("'%s' using 2:3 with p axis x1y2" %
                               results_filename)
        gnuplot_file.close()


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

    def __add_scheduler_task_launch(self, request_id, task_id, time):
        request = self.__get_request(request_id)
        request.add_scheduler_task_launch(task_id, time)

    def __add_node_monitor_task_submit(self, address, request_id, task_id, time):
        request = self.__get_request(request_id)
        request.add_node_monitor_task_submit(address, task_id, time)

    def __add_node_monitor_task_launch(self, address, request_id, task_id,
                                       time):
        request = self.__get_request(request_id)
        request.add_node_monitor_task_launch(address, task_id, time)

    def __add_task_completion(self, request_id, task_id, time):
        request = self.__get_request(request_id)
        request.add_task_completion(task_id, time)

def main(argv):
    PARAMS = ["log_dir", "output_file", "start_sec", "end_sec"]
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
        elif kv[0] == PARAMS[2]:
            global START_SEC
            START_SEC = int(kv[1])
        elif kv[0] == PARAMS[3]:
            global END_SEC
            END_SEC = int(kv[1])
        else:
            print "Warning: ignoring parameter %s" % kv[0]

    if len(log_files) == 0:
        print "No valid log files found!"
        return

    logging.basicConfig(level=logging.DEBUG)

    for filename in log_files:
        log_parser.parse_file(filename)

    log_parser.output_results(output_filename)
    log_parser.output_results(output_filename, "constrained")
    log_parser.output_results(output_filename, "unconstrained")


if __name__ == "__main__":
    main(sys.argv[1:])
