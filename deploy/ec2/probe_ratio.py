import boto
import os
import subprocess
import time

import ec2_exp

def run_cmd(cmd):
    subprocess.check_call(cmd, shell=True)

utilizations = [1.5]
sample_ratios = [2.0]
sample_ratio_constrained = 2

# Amount of time it takes each task to run in isolation
#TODO: There are issues here...alone, takes more like 145
task_duration_ms = 100
tasks_per_job = 10
private_ssh_key = "/root/patkey.pem"
sparrow_branch = "kay_nsdi"
num_backends = 100
num_frontends = 10
cores_per_backend = 4
# Run each trial for 5 minutes.
trial_length = 400
num_preferred_nodes = 0
num_users = 3

# Warmup information
warmup_s = 20
post_warmup_s = 60
warmup_arrival_rate_s = 0.1* (float(num_backends * cores_per_backend * 1000) /
                         (task_duration_ms * tasks_per_job * num_frontends))

print "********Launching instances..."
#run_cmd("./ec2-exp.sh launch -f %s -b %s -i %s" % (num_frontends, num_backends, private_ssh_key))
#time.sleep(10)

for sample_ratio in sample_ratios:
    for utilization in utilizations:
        # Number of jobs that should be generated at each frontend per millisecond.
        arrival_rate_ms = (float(utilization * num_backends * cores_per_backend) /
                           (task_duration_ms * tasks_per_job * num_frontends))
        arrival_rate_s = arrival_rate_ms * 1000

        # This is a little bit of a hacky way to pass args to the ec2 script.
        (opts, args) = ec2_exp.parse_args(False)
        opts.identity_file = private_ssh_key
        opts.arrival_rate = arrival_rate_s
        opts.branch = sparrow_branch
        opts.sample_ratio  = sample_ratio
        opts.sample_ratio_constrained = sample_ratio_constrained
        opts.tasks_per_job = tasks_per_job
        opts.num_preferred_nodes = num_preferred_nodes

        conn = boto.connect_ec2()
        frontends, backends = ec2_exp.find_existing_cluster(conn, opts)

        print ("********Launching experiment at utilization %s with sample ratio %s..." %
               (utilization, sample_ratio))

        print ("********Deploying with arrival rate %s and warmup arrival rate %s"
               % (arrival_rate_s, warmup_arrival_rate_s))
        ec2_exp.deploy_cluster(frontends, backends, opts, warmup_arrival_rate_s, warmup_s,
                               post_warmup_s, num_users)
        ec2_exp.start_sparrow(frontends, backends, opts)

        print "*******Sleeping after starting Sparrow"
        time.sleep(10)
        print "********Starting prototype frontends and backends"
        ec2_exp.start_proto(frontends, backends, opts)
        time.sleep(trial_length)

        log_dirname = "/disk1/sparrow/fairness_%s_%s" % (utilization, sample_ratio)
        while os.path.exists(log_dirname):
            log_dirname = "%s_a" % log_dirname
        os.mkdir(log_dirname)

        ec2_exp.execute_command(frontends, backends, opts, "./find_bugs.sh")

        print "********Stopping prototypes and Sparrow"
        ec2_exp.stop_proto(frontends, backends, opts)
        ec2_exp.stop_sparrow(frontends, backends, opts)

        print "********Collecting logs and placing in %s" % log_dirname
        opts.log_dir = log_dirname
        ec2_exp.collect_logs(frontends, backends, opts)
        run_cmd("gunzip %s/*.gz" % log_dirname)

        print "********Parsing logs"
        run_cmd(("cd ../../src/main/python/ && ./parse_logs.sh log_dir=%s "
                 "output_dir=%s/results start_sec=190 end_sec=310 && cd -") %
                (log_dirname, log_dirname))

