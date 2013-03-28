import os
import subprocess
import time

num_nodes = 100
wait_delay = 20 * 60
results_dirname = "results"
partitions = 30
reducers = 8
ratios = [(2, 2)] #-p, -q
rates = [275]
backend_mem = "5g"
cluster_name = "sparrow"
sparrow_branch = "per_task_old_code"

def run_cmd(cmd):
  subprocess.check_call(cmd, shell=True)

restart_cmd = "./ec2-exp.sh -i ~/.ssh/patkey.pem restart-spark-shark %s -m sparrow" % cluster_name
start_cmd = "./ec2-exp.sh -i ~/.ssh/patkey.pem start-shark-tpch %s" % cluster_name

for rate in rates:
  for (p, q) in ratios:
    dep_cmd = "./ec2-exp.sh deploy %s -g %s -s dev-sparrow-newcode -i ~/.ssh/patkey.pem --reduce-tasks=%s -p %s -q %s -u %s -v %s --spark-backend-mem=%s" % (
      cluster_name, sparrow_branch, reducers, p, q, partitions, rate, backend_mem)
    run_cmd(dep_cmd)
    run_cmd(restart_cmd)
    run_cmd(start_cmd)
    time.sleep(wait_delay)

    collect_dir = "%s/%s_%s_%s_%s" % (results_dirname, num_nodes, rate, p, q)
    if not os.path.exists(collect_dir):
      os.mkdir(collect_dir)
    collect_cmd = "./ec2-exp.sh -i ~/.ssh/patkey.pem collect-logs %s --log-dir=%s" % (cluster_name, collect_dir)
    run_cmd(collect_cmd)

    analysis_dir = "%s/analysis" % collect_dir
    if not os.path.exists(analysis_dir):
      os.mkdir(analysis_dir)

    run_cmd("gunzip %s/*.log.gz" % collect_dir)
    run_cmd("python /home/ubuntu/sparrow/src/main/python/parse_tpch_logs.py 600 800 " \
           "/home/ubuntu/sparrow/deploy/ec2/%s/shark_* > /home/ubuntu/sparrow/deploy/ec2/%s/tpch_results" % (collect_dir, analysis_dir))

    run_cmd("/home/ubuntu/sparrow_old/src/main/python/parse_logs.sh start_sec=600 " \
           "end_sec=800 log_dir=/home/ubuntu/sparrow/deploy/ec2/%s/ output_dir=/home/ubuntu/sparrow/deploy/ec2/%s/ > /home/ubuntu/sparrow/deploy/ec2/%s/parse_results" % (
               collect_dir, analysis_dir, analysis_dir))
    run_cmd("gzip %s/*.log" % collect_dir)
