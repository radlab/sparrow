import sys
import re
import time
import datetime

START_SEC = 200
END_SEC = 300
id_counter = 0
min_date_seen = datetime.datetime(2030, 1, 1)

def match_or_die(regex, line):
  match = re.match(regex, line)
  if match is None:
    print "ERROR matching re: %s to line %s" % (regex, line)
    sys.exit(-1)
  return match

class Trial:
  def __init__(self, trial_id, query_id, start_date):
    self.trial_id = trial_id
    self.query_id = query_id
    self.start_date = start_date
    self.phases = {}
  def add_phase(self, phase_id, phase):
    "Adding phase %s" % phase_id
    self.phases[phase_id] = phase
  def get_phase(self, phase_id):
    return self.phases[phase_id]
  def is_complete(self):
    for phase in self.phases.values():
      if not phase.is_complete(): return False
    return True
  def get_phases(self): return self.phases.keys()
  def get_response_time(self):
    return sum([p.get_response_time() for p in self.phases.values()])
  def __str__(self):
    out = "Trial id=%s q=%s time=%s:" % (
      self.trial_id, self.query_id, self.get_response_time())
    for phase in self.phases.values():
      out = out + "\n  %s" % phase 
    return out

class Phase:
  def __init__(self, phase_id, absolute_phase_id):
    self.phase_id = phase_id
    self.absolute_phase_id = absolute_phase_id
    self.tasks = []
  def add_task(self, task):
    self.tasks.append(task)
  def get_tasks(self): return self.tasks
  # TODO actually check expected number of tasks
  def is_complete(self): return len(self.tasks) > 0
  def get_response_time(self):
    return max([t.get_response_time() for t in self.tasks])
  def __str__(self):
    out = "Phase: id=%s time=%s" % (self.phase_id, self.get_response_time())
    for task in self.tasks:
      out = out + "\n    %s" % task
    return out

class Task:
  def __init__(self, wait_time, service_time):
    self.wait_time = wait_time
    self.service_time = service_time
  def get_response_time(self):
    return self.wait_time + self.service_time
  def __repr__(self):
    return "Task: response time  %s (%s/%s)" % (
      self.get_response_time(), self.wait_time, self.service_time)

trials_per_file = {}
skipped_trials = 0

# Get minimum date
print "Finding min date:"
for f in sys.argv[1:]:
  for line in open(f):
    if "QUERY" in line:
      str_date = line.split(" INFO")[0].replace("OK", "")
      date = datetime.datetime.strptime(str_date, "%y/%m/%d %H:%M:%S")
      if (date < min_date_seen):
        min_date_seen = date
print "Min date: %s" % min_date_seen


delta_buckets = {} # Queries per ten-second window
for f in sys.argv[1:]:
  curr_phase_per_thread = {} 
  curr_trial_per_thread = {}
  trials = {}
  stages_to_thread = {}
  enabled = False
  bad_stages = {} # For some reason we always miss print out for some stages

  for line in open(f):
    # A query started
    if "QUERY" in line:
      if len(line) < 100:
        print line
        continue
      enabled = True
      str_date = line.split(" INFO")[0].replace("OK", "")
      thread_match = match_or_die(r".*THREAD:(\d+).*", line)
      thread = thread_match.group(1)
      date = datetime.datetime.strptime(str_date, "%y/%m/%d %H:%M:%S")
      query_match = match_or_die(r".*--(\d+)--.*", line)
      qnum = query_match.group(1)
      curr_phase_per_thread[thread] = 0
      id_counter = id_counter + 1
      curr_trial = id_counter
      curr_trial_per_thread[thread] = curr_trial
      trials[curr_trial] = Trial(curr_trial, qnum, date)

    if not enabled:
      continue

    # A new task set was submitted
    if "Submitting" in line and "tasks" in line:
      thread_match = match_or_die(r".*thread (\d+).*", line)
      thread = thread_match.group(1)
      stage_match = match_or_die(r".*Stage (\d+).*", line)
      stage = stage_match.group(1)
      stages_to_thread[stage] = thread
      curr_trial = curr_trial_per_thread[thread]
      curr_phase = curr_phase_per_thread[thread] + 1
#      if curr_phase != 1:
#        s_1 = stage
#        s_2 = trials[curr_trial].get_phase(curr_phase - 1).absolute_phase_id
#        if int(s_2) - int(s_1) != 1:
#          print "Adjacent stages %s and %s in thread %s" % (s_1, s_2, thread)
      curr_phase_per_thread[thread] = curr_phase # now incremented
      trials[curr_trial].add_phase(curr_phase, Phase(curr_phase, stage))

    # A task finished
    if "Task" in line and "finished" in line and "service" in line:
      stage_match = match_or_die(r".*\((\d+), (\d+)\).*", line)
      stage = stage_match.group(1)
      if stage not in stages_to_thread:
        bad_stages[stage] = ''
        continue
      thread = stages_to_thread[stage]

      match = match_or_die(r".*\(wait: (\d+)ms, service: (\d+)ms\).*", line)
      wait_time = int(match.group(1))
      service_time = int(match.group(2))
      curr_trial = curr_trial_per_thread[thread]
      curr_phase = curr_phase_per_thread[thread]

      trials[curr_trial].get_phase(curr_phase).add_task(
        Task(wait_time, service_time))

    # A Job finished
    if "Job" in line and "finished" in line:
      match = match_or_die(r".*(\d+\.\d+) s.*", line)
      job_time = float(match.group(1)) * 1000 # convert to ms

  for (tid, trial) in trials.items():
    delta = (trial.start_date - min_date_seen).seconds
    adjusted = delta - delta % 30
    delta_buckets[adjusted] = delta_buckets.get(adjusted, 0) + 1
    if delta < START_SEC or delta > END_SEC:
      del trials[tid]
      skipped_trials = skipped_trials + 1
    elif not trial.is_complete():
      del trials[tid]
  if len(bad_stages) > 0:
    print "BAD Stages: %s" % bad_stages.keys()
  trials_per_file[f] = trials


for (bucket, count) in sorted(delta_buckets.items(), key = lambda k: k[0]):
  print "%s\t%s" % (bucket, count)
### Output query summaries per frontend
def add_to_dict_of_lists(key, value, d):
  if key not in d.keys():
    d[key] = []
  d[key].append(value)

all_trials = []
for (f, trials) in trials_per_file.items():
  all_trials = all_trials + trials.values()

print "Including %s of %s trials" % (len(all_trials), 
  len(all_trials) + skipped_trials)

"""
for (f, trials) in sorted(trials_per_file.items(), key=lambda k: k[0]):
  times_per_query = {} # key = query_id
  times_per_phase = {} # key = (query_id, phase_id)
  print "%s:" % f
  for trial in trials.values():
    add_to_dict_of_lists(
      trial.query_id, trial.get_response_time(), times_per_query)
    for phase_id in trial.get_phases():
      phase_key = (trial.query_id, phase_id)
      add_to_dict_of_lists(phase_key, 
        trial.get_phase(phase_id).get_response_time(), times_per_phase)

  print "q_num\tmed_ms\t05_ms\t95_ms\tn"
  for (query, values) in times_per_query.items():
    values.sort()
    print "%s\t%s\t%s\t%s\t%s" % (
     query, values[len(values)/2], 
     min(values), max(values), len(values)) 
    phases = filter(lambda k: k[0][0] == query, times_per_phase.items())
    phases.sort(key = lambda k: k[0][1])

    for (phase_id, values) in phases:
      values.sort()
      print "%s.%s\t%s\t%s\t%s\t%s" % (phase_id[0], phase_id[1], 
      values[len(values)/2],
      values[int(len(values) * .05)],
      values[int(len(values) * .95)],
      len(values))
 
  print "Total time: %s s" % (
    sum([x.get_response_time() for x in trials.values()]) / 1000)
"""

### Print out statistics per task
trials_per_query = {}
for trial in all_trials:
  add_to_dict_of_lists(trial.query_id, trial, trials_per_query)

print "qnum\tmed_ms\t05_ms\t95_ms"
for (query, trials) in trials_per_query.items():
  resp_times = [x.get_response_time() for x in trials]
  resp_times.sort()
  index_95 = int(.95 * len(resp_times))
  index_50 = int(.5 * len(resp_times))
  index_5 = int(.05 * len(resp_times))
  print "%s\t%s\t%s\t%s" % (query, resp_times[index_50], 
                        resp_times[index_5], 
                        resp_times[index_95])

### Create plot of response time vs time
trials_per_time = {}
for trials in trials_per_file.values():
  for trial in trials.values():
    delta = (trial.start_date - min_date_seen).seconds
    add_to_dict_of_lists(delta, trial.get_response_time(), trials_per_time)
f = open("tpch_trials_per_time.txt", 'w')
for (time, trials) in trials_per_time.items():
  for t in trials:
    f.write("%s\t%s\n" % (time, t))
f.close()

### Create CDF's across all fe's/trials
all_wait_times = []
all_service_times = []
all_response_times = []

for trials in trials_per_file.values():
  for trial in trials.values():
    for phase in trial.get_phases():
      for task in trial.get_phase(phase).get_tasks():
        all_wait_times.append(task.wait_time)
        all_service_times.append(task.service_time)
        all_response_times.append(task.get_response_time())
all_wait_times.sort()
all_service_times.sort()
all_response_times.sort()

wait_file = open("tpch_wait_times.txt", 'w')
for x in range(99):
  fl = float(x) / 100
  index = int(fl * len(all_wait_times))
  wait_file.write("%s\t%s\n" % (fl, all_wait_times[index]))
wait_file.close()

service_file = open("tpch_service_times.txt", 'w')
for x in range(99):
  fl = float(x) / 100
  index = int(fl * len(all_service_times))
  service_file.write("%s\t%s\n" % (fl, all_service_times[index]))
service_file.close()

service_file = open("tpch_response_times.txt", 'w')
for x in range(99):
  fl = float(x) / 100
  index = int(fl * len(all_response_times))
  service_file.write("%s\t%s\n" % (fl, all_response_times[index]))
service_file.close()
