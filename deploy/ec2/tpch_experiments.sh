import os
import subprocess
import time

num_nodes = 18
wait_delay = 24 * 60
results_dirname = "results"
partitions = 6
ratios = [(2, 2), (1, 1)] #-p, -q
rates = [76]
backend_mem = "10g"

def run_cmd(cmd):
  subprocess.check_call(cmd, shell=True)

restart_cmd = "./ec2-exp.sh -i ~/.ssh/patkey.pem restart-spark-shark -m sparrow"
start_cmd = "./ec2-exp.sh -i ~/.ssh/patkey.pem start-shark-tpch"

for rate in rates:
  for (p, q) in ratios:
    dep_cmd = "./ec2-exp.sh deploy -g nsdi-patrick -s dev-sparrow-newcode " +\
      "-i ~/.ssh/patkey.pem -p %s -q %s -u %s -v %s --spark-backend-mem=%s" % (
      p, q, partitions, rate, backend_mem)
    run_cmd(dep_cmd)
    run_cmd(restart_cmd)
    run_cmd(start_cmd)
    time.sleep(wait_delay)

    collect_dir = "%s/%s_%s_%s_%s" % (results_dirname, num_nodes, rate, p, q)
    if not os.path.exists(collect_dir):
      os.mkdir(collect_dir)
    collect_cmd = "./ec2-exp.sh -i ~/.ssh/patkey.pem collect-logs --log-dir=%s" % \
      collect_dir
    run_cmd(collect_cmd)
