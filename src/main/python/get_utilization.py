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
  RUNNING = "nodemonitor_task_runnable"
  #RUNNING="node_monitor_task_launch"
  START_INT = 1
  COMPLETED = "nodemonitor_task_completed"
  #COMPLETED = "task_completed"
  END_INT = -1

  task_to_node = {}

  for filename in log_files:
    print filename
    for line in open(filename):
      items = line.strip().split("\t")
      if len(items) < 2:
        continue
      time = int(items[1]) 
      
      if RUNNING in items[2]:
        events.append((time, START_INT))
        
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

  graph_file = open("utilization_vs_time.gp", 'w')
  graph_file.write("set terminal postscript\n")
  graph_file.write("set output 'utilization_vs_time.ps'\n")
  graph_file.write("set ylabel 'Utilization'\n")
  graph_file.write("set xlabel 'Time (s)'\n")
  graph_file.write("plot 'utilization_vs_time.txt' using 1:2 with lines lw 3 lc 1 notitle")

  print "Average utilization: %s" % (total_utilization / total_time)
  print "Tasks per second: %s" % (total_starts / (total_time / 1000))
  print "Init time: %s" % init_time
  #subprocess.check_call("rm utilization_vs*.txt", shell=True)

if __name__ == "__main__":
  main(sys.argv)
