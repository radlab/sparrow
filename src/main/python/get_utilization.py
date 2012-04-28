# Quick-and-dirty script to get the cluster utilization over time
import sys
import os
import subprocess

def main(argv):
  log_dir = argv[1]
  util_min_s = float(argv[2])
  util_max_s = float(argv[3])

  unqualified_log_files = filter(lambda x: "sparrow_audit" in x,
                                 os.listdir(log_dir))
  log_files = [os.path.join(log_dir, filename) for \
               filename in unqualified_log_files]

  events = []
  events_per_node = {}
  RUNNABLE = "runnable"
  START_INT = 1
  SUBMITTED = "task_submit"
  COMPLETED = "task_completed"
  END_INT = -1

  task_to_node = {}

  for filename in log_files:
    for line in open(filename):
      items = line[:-1].split("\t")
      time = int(items[1]) 

      if SUBMITTED in items[2]:
        parts = items[2].split(":")
        node = parts[1]
        task = parts[2]
        task_to_node[task] = node
        arr = events_per_node.get(node, [])
        arr.append((time, START_INT))
        events_per_node[node] = arr

      if RUNNABLE in items[2]:
        events.append((time, START_INT))
        task = items[2].split(":")[2]
        node = task_to_node[task]
        events_per_node[node].append((time, END_INT))

        
      if COMPLETED in items[2]:
        events.append((time, END_INT))
         
  events = sorted(events, key = lambda x: x[0])
  
  out_file = open("utilization_vs_time.txt", 'w')
  count = 0
  init_time = events[0][0]

  total_utilization = 0.0
  total_time = (util_max_s - util_min_s) * 1000
  last_time = -1
  total_starts = 0  

  for event in events:
    time_s = (float(event[0]) - init_time) / 1000
    if time_s >= util_min_s and time_s <= util_max_s:
      if event[1] == 1:
        total_starts = total_starts + 1

      if last_time is -1:
        last_time = event[0]
      time_diff = event[0] - last_time
      last_time = event[0]
      total_utilization = total_utilization + (time_diff * count)
    count = count + event[1]
    out_file.write("%s\t%s\n" % (time_s, count))
  out_file.close()


  for (node, events) in events_per_node.items():
    out_file = open("utilization_vs_time_%s.txt" % node, 'w')
    count = 0
    init_time = events[0][0]
    for event in events:
      count = count + event[1]
      out_file.write("%s\t%s\n" % (float(event[0] - init_time) / 1000, count))
    out_file.close()

  graph_file = open("utilization_vs_time.gp", 'w')
  graph_file.write("set terminal postscript\n")
  graph_file.write("set output 'utilization_vs_time.ps'\n")
  graph_file.write("set ylabel 'Utilization'\n")
  graph_file.write("set xlabel 'Time (s)'\n")
  graph_file.write("plot 'utilization_vs_time.txt' using 1:2 with lines lw 3 lc 1 notitle")
  for node in events_per_node.keys():
    graph_file.write(",\\\n'utilization_vs_time_%s.txt' using 1:2 with lines lw 1 notitle" % (node))
  graph_file.close()
  subprocess.check_call("gnuplot utilization_vs_time.gp", shell=True, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
  print "Average utilization: %s" % (total_utilization / total_time)
  print "Tasks per second: %s" % (total_starts / (total_time / 1000))
  print "Init time: %s" % init_time
  #subprocess.check_call("rm utilization_vs*.txt", shell=True)

if __name__ == "__main__":
  main(sys.argv)
