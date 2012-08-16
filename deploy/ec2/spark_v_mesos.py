import os
import subprocess
import time

def run_cmd(cmd):
  print cmd
  subprocess.check_call(cmd, shell=True)

trial_length = 400
probe_ratio = (2, 2)
query_nums = [7000]
query_par = 10
scheds = ["sparrow"]

# STOP ALL THE THINGS
run_cmd("./ec2-exp.sh stop-spark -i ~/.ssh/eastkey.pem")
run_cmd("./ec2-exp.sh stop-sparrow -i ~/.ssh/eastkey.pem")
run_cmd("./ec2-exp.sh stop-mesos -i ~/.ssh/eastkey.pem")

for task_length in query_nums:
  rate = float(1000) / task_length # WAS 2000! but went from 5->10 nodes
  for sched in scheds:
    run_cmd("./ec2-exp.sh deploy -g better-policies -s sparrow -k eastkey -i ~/.ssh/eastkey.pem -p %s -q %s" % (probe_ratio[0], probe_ratio[1]))
    run_cmd("./ec2-exp.sh start-%s -i ~/.ssh/eastkey.pem" % sched)
#    time.sleep(30)
    max_queries = int(trial_length * rate)
    run_cmd("./ec2-exp.sh start-spark -i ~/.ssh/eastkey.pem "
            "-v %s -j %s -o %s -r %s -m %s" % (rate, max_queries, 
                                                 task_length, query_par, sched))
    dirname = "race/%s_%s" % (sched, task_length)
    if not os.path.exists(dirname):
      os.mkdir(dirname)
    time.sleep(trial_length + 20) 
    run_cmd("./ec2-exp.sh collect-logs -i ~/.ssh/eastkey.pem --log-dir=%s/" % 
    dirname)
    run_cmd("cd %s && gunzip *.gz && cd -" % dirname)
    run_cmd("./ec2-exp.sh stop-spark -i ~/.ssh/eastkey.pem")
    run_cmd("./ec2-exp.sh stop-%s -i ~/.ssh/eastkey.pem" % sched)


