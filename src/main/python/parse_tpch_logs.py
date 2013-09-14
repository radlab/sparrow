import logging
import os
import sys

import parse_logs
import parse_per_task_logs

def main(argv):
    PARAMS = ["log_dir", "output_dir", "start_sec", "end_sec"]
    if "help" in argv[0]:
        print ("Usage: python parse_logs.py " +
               " ".join(["[%s=v]" % k for k in PARAMS]))
        return

    #log_parser = parse_per_task_logs.LogParser()
    log_parser = parse_logs.LogParser()
    
    # Use 5 minutes in the middle of the experiment, by default.
    start_sec = 400
    end_sec = 500

    log_files = []
    output_dir = "experiment"
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
            start_sec = int(kv[1])
        elif kv[0] == PARAMS[3]:
            end_sec = int(kv[1])
        else:
            print "Warning: ignoring parameter %s" % kv[0]

    if len(log_files) == 0:
        print "No valid log files found!"
        return

    if not os.path.exists(output_dir):
      os.mkdir(output_dir)

    logging.basicConfig(level=logging.DEBUG)

    for filename in log_files:
        print "Parsing log %s" % filename
        log_parser.parse_file(filename)

    requests = log_parser.get_requests()

    # Separate out results for each query and each type of query, and use only
    # the results between START_TIME and END_TIME (relative to the beginning of the experiment).
    # TPCH query ID : particular query : requests map!\
    earliest_time = log_parser.earliest_time()
    last_time = max([x.arrival_time() for x in requests.values()])
    print last_time
    print earliest_time
    print "Last was %s after earliest" % (last_time - earliest_time)
    time_min = earliest_time + start_sec * 1000
    time_max = earliest_time + end_sec * 1000
    print "Min time: %s, max time: %s" % (time_min, time_max)
    query_type_to_queries = {}
    incomplete = 0
    for r in requests.values():
        #r.set_clock_skews()
        if not r.complete():
          incomplete += 1
          continue
        if r.tpch_id not in query_type_to_queries:
          query_type_to_queries[r.tpch_id] = {}
        queries = query_type_to_queries[r.tpch_id]

        # This assumes that only one shark frontend is submitting queries to
        # each scheduler.
        query_id = (r.shark_id, r.scheduler_address())
        if query_id not in queries:
          queries[query_id] = []
        queries[query_id].append(r)

    print "%s incomplete requests" % incomplete

    too_early = 0
    too_late = 0
    used_requests = 0
    total_time = 0
    total_tasks = 0
    total_jobs = 0
    constrained_jobs = 0
    for tpch_id, queries in query_type_to_queries.iteritems():
        print "Parsing queries for %s" % tpch_id
        optimals = []
        actuals = []
        fulls = []
        for id_tuple, requests in queries.iteritems():
            requests.sort(key = lambda x: x.arrival_time())
            # Check whether the query started within the right interval.
            query_arrival = requests[0].arrival_time()
            if query_arrival < time_min:
              too_early += 1
              continue
            elif query_arrival > time_max:
              too_late += 1
              continue
            used_requests += 1
            optimal = 0
            actual = 0
            start = -1
            end = 0
            total_jobs += len(requests)
            for request in requests:
                if request.constrained:
                   constrained_jobs += 1
                #print request
                service_times = request.service_times()
                total_time += sum(service_times)
                total_tasks += len(service_times)
                opt = request.optimal_response_time()
                optimal += opt
                act = request.response_time()
                actual += act
                #print ("Request from %s (Shark %s, stage %s, arrival %s, constrained: %s): optimal %s, actual %s" %
                #  (id_tuple[1], id_tuple[0], request.stage_id, request.arrival_time(), request.constrained, opt, act))
                if start == -1:
                  #print "setting start %s" % request.arrival_time()
                  start = request.arrival_time()
                end = request.arrival_time() + request.response_time()
            #print "For query %s from %s: optimal %s actual %s full %s" % (id_tuple[1], id_tuple[0], optimal, actual, end - start)
            #print "end: %s" % end
            optimals.append(optimal)
            actuals.append(actual)
            fulls.append(end - start)

        optimals.sort()
        actuals.sort()
        file = open("%s/results_%s" % (output_dir, tpch_id), "w")
        file.write("optimal\tactual\n")
        NUM_DATA_POINTS = 100
        for i in range(NUM_DATA_POINTS):
            i = float(i) / NUM_DATA_POINTS
            file.write("%f\t%d\t%d\n" % (i, parse_logs.get_percentile(optimals, i), parse_logs.get_percentile(actuals, i)))
        file.close()
    total_slots = 400.00
    load = total_time / (total_slots * (end_sec - start_sec) * 1000)
    file = open("%s/summary" % output_dir, 'w')
    file.write("Load %s (total time: %s)\n" % (load, total_time))
    file.write("%s too early, %s too late, %s total used\n" % (too_early, too_late, used_requests))
    file.write("%s total jobs" % (total_jobs))
    file.write("%s constrained jobs" % constrained_jobs)
    print "Completed TPCH analysis, results in %s" % output_dir

if __name__ == "__main__":
     main(sys.argv[1:])
