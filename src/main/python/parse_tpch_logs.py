import logging
import os
import sys

import parse_logs

def main(argv):
    PARAMS = ["log_dir", "output_dir", "start_sec", "end_sec"]
    if "help" in argv[0]:
        print ("Usage: python parse_logs.py " +
               " ".join(["[%s=v]" % k for k in PARAMS]))
        return

    log_parser = parse_logs.LogParser()

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

    requests = log_parser.get_requests()

    # Separate out results for each query and each type of query. TPCH query ID : particular query : requests map!\
    query_type_to_queries = {}
    for r in requests:
        if r.tpch_id not in query_type_to_queries:
          query_type_to_queries[r.tpch_id] = []
        queries = query_type_to_queries[r.tpch_id]

        # This assumes that only one shark frontend is submitting queries to
        # each scheduler.
        query_id = (r.shark_id, r.scheduler_address())
        if query_id not in queries:
          queries[query_id] = []
        queries[query_id].append(r)

    query_type_to_optimal = {}
    query_type_to_actual = {}
    for tpch_id, queries in query_type_to_queries.iteritems():
        print "Parsing queries for " + tpch_id
        optimals = []
        actuals = []
        for id_tuple, requests in queries.iteritems():
            print "%s requests" % len(requests)
            requests.sort(key = lambda x: x.arrival_time())
            optimal = 0
            actual = 0
            start = -1
            end = 0
            for request in requests:
                opt = sum(r.optimal_response_time() for r in requests)
                act = sum(r.response_time() for r in requests)
                print ("Request from %s (Shark %s, stage %s, constrained: %s): optimal %s, actual %s" %
                  (id_tuple[1], id_tuple[0], request.stage_id, request.constrained, opt, act))
                optimal += opt
                actual += act
                if start == -1:
                  start = r.arrival_time()
                end = r.arrival_time() + r.response_time()
            optimals.append(optimal)
            actuals.append(actual)
            correct_actual
        query_type_to_optimal[tpch_id] = optimals
        query_type_to_actual[tpch_id] = actuals

if __name__ == "__main__":
     main(sys.argv[1:])
