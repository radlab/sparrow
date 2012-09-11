import sys
import re
import time

NUM_TO_EXCLUDE = 25 # For warmup
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
    return "Task: wait %s, service %s" % (self.wait_time, self.service_time)

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

def add_to_dict_of_lists(key, value, d):
  if key not in d.keys():
    d[key] = []
  d[key].append(value)

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
    values = values[NUM_TO_EXCLUDE:] # PRUNE WARMP-UP
    values.sort()
    print "%s\t%s\t%s\t%s\t%s" % (
     query, values[len(values)/2], 
     min(values), max(values), len(values)) 
    phases = filter(lambda k: k[0][0] == query, times_per_phase.items())
    phases.sort(key = lambda k: k[0][1])

    for (phase_id, values) in phases:
      values = values[NUM_TO_EXCLUDE:]
      values.sort()
      print "%s.%s\t%s\t%s\t%s\t%s" % (phase_id[0], phase_id[1], 
      values[len(values)/2],
      min(values), max(values), len(values))
 
  print "Total time: %s s" % (
    sum([x.get_response_time() for x in trials.values()]) / 1000)
