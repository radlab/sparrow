import sys
import re
import time

NUM_TO_EXCLUDE = 4 * 25
id_counter = 0

def match_or_die(regex, line):
  match = re.match(regex, line)
  if match is None:
    print "ERROR matching re: %s to line %s" % (regex, line)
    sys.exit(-1)
  return match

class Trial:
  def __init__(self, trial_id, query_id):
    self.trial_id = trial_id
    self.query_id = query_id
    self.phases = {}
  def add_phase(self, phase_id, phase):
    "Adding phase %s" % phase_id
    self.phases[phase_id] = phase
  def get_phase(self, phase_id):
    return self.phases[phase_id]
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
  def __init__(self, phase_id):
    self.phase_id = phase_id
    self.tasks = []
  def add_task(self, task):
    self.tasks.append(task)
  def get_tasks(self): return self.tasks
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
for f in sys.argv[1:]:
  curr_q = None
  curr_phase = 0 
  curr_trial = ""
  trials = {}

  for line in open(f):
    # A query started
    if "select" in line and "--" in line:
      match = match_or_die(r".*--(\d+)--.*", line)
      qnum = match.group(1)
      if curr_q == qnum: ## HACK because we get two log messages for each q
        continue
      curr_q = qnum
      curr_phase = 0
      id_counter = id_counter + 1
      curr_trial = id_counter
      trials[curr_trial] = Trial(curr_trial, qnum)

    if curr_trial == "":
      continue

    # A new task set was submitted
    if "submit taskSet" in line:
      curr_phase = curr_phase + 1
      trials[curr_trial].add_phase(curr_phase, Phase(curr_phase))

    # A task finished
    if "Task" in line and "finished" in line and "service" in line:
      match = match_or_die(r".*\(wait: (\d+)ms, service: (\d+)ms\).*", line)
      wait_time = int(match.group(1))
      service_time = int(match.group(2))
      trials[curr_trial].get_phase(curr_phase).add_task(
        Task(wait_time, service_time))

    # A Job finished
    if "Job" in line and "finished" in line:
      match = match_or_die(r".*(\d+\.\d+) s.*", line)
      job_time = float(match.group(1)) * 1000 # convert to ms
  trials_per_file[f] = trials


### Output query summaries per frontend
def add_to_dict_of_lists(key, value, d):
  if key not in d.keys():
    d[key] = []
  d[key].append(value)

### PRUNE
pruned_trials_per_file = {}
for (f, trials) in trials_per_file.items():
  pruned_trials_per_file[f] = {}
  print "Excluding first %s of %s" % (NUM_TO_EXCLUDE, len(trials_per_file[f]))
  keys = sorted(trials_per_file[f].keys())
  keys = keys[NUM_TO_EXCLUDE:]
  for k in keys:
    pruned_trials_per_file[f][k] = trials_per_file[f][k]
  
trials_per_file = pruned_trials_per_file

all_trials = []
for (f, trials) in trials_per_file.items():
  all_trials = all_trials + trials.values()

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

  print "q_num\tmed_ms\tmin_ms\tmax_ms\tn"
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
      min(values), max(values), len(values))
 
  print "Total time: %s s" % (
    sum([x.get_response_time() for x in trials.values()]) / 1000)


### Print out statistics per task
trials_per_query = {}
for trial in all_trials:
  add_to_dict_of_lists(trial.query_id, trial, trials_per_query)
for (query, trials) in trials_per_query.items():
  resp_times = [x.get_response_time() for x in trials]
  resp_times.sort()
  index_95 = int(.95 * len(resp_times))
  index_50 = int(.5 * len(resp_times))
  index_5 = int(.05 * len(resp_times))
  print "%s\t%s\t%s\t%s" % (query, resp_times[index_50], 
                        resp_times[index_5], 
                        resp_times[index_95])

### Create CDF's across all fe's/trials
all_wait_times = []
all_service_times = []
all_response_times = []

for trials in trials_per_file.values():
  for trial in trials.values():
#    if trial.query_id == '3' and trial.get_response_time() < 450 and trial.get_response_time() > 440:
#      print trial
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
