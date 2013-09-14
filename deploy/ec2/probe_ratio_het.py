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

import boto
import os
import subprocess
import sys
import time

import ec2_exp

def run_cmd(cmd):
    subprocess.check_call(cmd, shell=True)

def main(argv):
    launch_instances = False
    if len(argv) >= 1 and argv[0] == "True":
        launch_instances = True

    utilizations = [0.8, 0.9]
    sample_ratios = [1.1, 1.2, 1.5, 2.0, 3.0]
    sample_ratio_constrained = 2

    # Amount of time it takes each task to run in isolation
    task_duration_ms = 100
    tasks_per_job = 10
    private_ssh_key = "patkey.pem"
    sparrow_branch = "master"
    nm_task_scheduler = "fifo"
    num_backends = 100
    num_frontends = 10
    cores_per_backend = 8
    # Run each trial for 5 minutes.
    trial_length = 400
    num_preferred_nodes = 0
    num_users = 1
    cluster = "probe"

    full_utilization_rate_s = (float(num_backends * cores_per_backend * 1000) /
                               (task_duration_ms * tasks_per_job * num_frontends))

    # Warmup information
    warmup_s = 120
    post_warmup_s = 30
    warmup_arrival_rate_s = 0.4 * full_utilization_rate_s

    if launch_instances:
        print "********Launching instances..."
        run_cmd("./ec2-exp.sh launch %s -f %s -b %s -i %s" % # --spot-price %s" %
                (cluster, num_frontends, num_backends, private_ssh_key))
        time.sleep(10)

    for sample_ratio in sample_ratios:
        for utilization in utilizations:
            arrival_rate_s = utilization * full_utilization_rate_s

            # This is a little bit of a hacky way to pass args to the ec2 script.
            (opts, args) = ec2_exp.parse_args(False)
            opts.identity_file = private_ssh_key
            opts.arrival_rate = arrival_rate_s
            opts.branch = sparrow_branch
            opts.sample_ratio  = sample_ratio
            opts.sample_ratio_constrained = sample_ratio_constrained
            opts.tasks_per_job = tasks_per_job
            opts.num_preferred_nodes = num_preferred_nodes
            opts.cpus = cores_per_backend
            opts.benchmark_iterations = 100

            conn = boto.connect_ec2()
            frontends, backends = ec2_exp.find_existing_cluster(conn, opts, cluster)

            print ("********Launching experiment at utilization %s with sample ratio %s..." %
                   (utilization, sample_ratio))

            print ("********Deploying with arrival rate %s and warmup arrival rate %s"
                   % (arrival_rate_s, warmup_arrival_rate_s))
            ec2_exp.deploy_cluster(frontends, backends, opts, warmup_arrival_rate_s, warmup_s,
                                   post_warmup_s, nm_task_scheduler)
            # Redeploy on half of the frontends with a longer duration for each task and a correspondingly
            # lower arrival rate.
            opts.arrival_rate = arrival_rate_s / 100.
            opts.benchmark_iterations = 10000
            ec2_exp.redeploy_sparrow(frontends[:5], backends, opts, warmup_arrival_rate_s / 100., warmup_s, post_warmup_s, nm_task_scheduler) 
            ec2_exp.start_sparrow(frontends, backends, opts)

            print "*******Sleeping after starting Sparrow"
            time.sleep(10)
            print "********Starting prototype frontends and backends"
            ec2_exp.start_proto(frontends, backends, opts)
            time.sleep(trial_length)

            log_dirname = "/home/ec2-user/sparrow/deploy/ec2/probe_ratio_%s_%s" % (utilization, sample_ratio)
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
            #run_cmd("gunzip %s/*.gz" % log_dirname)

            #print "********Parsing logs"
            #run_cmd(("cd ../../src/main/python/ && ./parse_logs.sh log_dir=%s "
            #         "output_dir=%s/results start_sec=350 end_sec=450 && cd -") %
            #        (log_dirname, log_dirname))

if __name__ == "__main__":
    main(sys.argv[1:])

