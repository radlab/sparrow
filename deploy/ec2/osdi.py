import os
import subprocess
import time

def run_cmd(cmd):
  subprocess.check_call(cmd, shell=True)

trial_length = 300
ratios = [(0, 0)]
rates = [3.3]
tpch_query = 1
par_level = 23

for ratio in ratios:
  for rate in rates:
    run_cmd("./ec2-exp.sh stop-sparrow -i ~/.ssh/eastkey.pem")
    run_cmd("./ec2-exp.sh stop-spark -i ~/.ssh/eastkey.pem")
    run_cmd("./ec2-exp.sh deploy -g better-policies -s sparrow -k eastkey -i ~/.ssh/eastkey.pem -p %s -q %s" % (ratio[0], ratio[1]))
    run_cmd("./ec2-exp.sh start-sparrow -i ~/.ssh/eastkey.pem")
    time.sleep(30)
    max_queries = int(trial_length * rate)
    run_cmd("./ec2-exp.sh start-spark -i ~/.ssh/eastkey.pem -m "
            "sparrow -v %s -j %s -o %s -r %s" % (rate, max_queries, 
                                                 tpch_query, par_level))
    dirname = "%s_%s_%s" % (ratio[0], ratio[1], rate)
    if not os.path.exists(dirname):
      os.mkdir(dirname)
    time.sleep(trial_length + 120) 
    run_cmd("./ec2-exp.sh collect-logs -i ~/.ssh/eastkey.pem --log-dir=%s/" % 
    dirname)
    run_cmd("cd %s && gunzip *.gz && cd -" % dirname)


