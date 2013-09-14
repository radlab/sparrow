import sys

for f in sys.argv[1:]:
  start_times = {}
  service_times = []
  for line in open(f): 
    if "task_runnable" in line:
      parts = line.split("\t")
      time = int(parts[1])
      task = parts[2].split(":")[2] + ":" + parts[2].split(":")[3]
      start_times[task] = time
    if "task_completed" in line:
      parts = line.split("\t")
      time = int(parts[1])
      task = parts[2].split(":")[1] + ":" + parts[2].split(":")[2]
      service_times.append(time - start_times[task])

  if len(service_times) == 0:
    continue
  
  avg = sum(service_times) * 1.0 / (len(service_times) * 1000)
  print "%.3f\t%s" % (avg, f)
