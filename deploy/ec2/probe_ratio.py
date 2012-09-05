import os
import subprocess
import time

def run_cmd(cmd):
    subprocess.check_call(cmd, shell=True)

utilizations = [0.9]
sample_ratios = [2.0]

# Amount of time it takes each task to run in isolation
#TODO: There are issues here...alone, takes more like 145
task_duration_ms = 160
tasks_per_job = 10
private_ssh_key = "patkey.pem"
sparrow_branch = "experimental0903"
num_backends = 4 #90
num_frontends = 1 #0
cores_per_backend = 4
# Run each trial for 5 minutes.
trial_length = 300

print "********Launching instances..."
#run_cmd("./ec2-exp.sh launch -f %s -b %s -i %s" % (num_frontends, num_backends, private_ssh_key))
#time.sleep(10)

for sample_ratio in sample_ratios:
    for utilization in utilizations:
        print ("********Launching experiment at utilization %s with sample ratio %s..." %
               (utilization, sample_ratio))

        # Number of jobs that should be generated at each frontend per millisecond.
        arrival_rate_ms = (utilization * num_backends * cores_per_backend /
                           (task_duration_ms * tasks_per_job * num_frontends))
        arrival_rate_s = arrival_rate_ms * 1000
        print "********Deploying with arrival rate %s" % arrival_rate_s
        run_cmd("./ec2-exp.sh deploy -g %s -i %s -n %s -p %s -l %s" %
                (sparrow_branch, private_ssh_key, tasks_per_job, sample_ratio, arrival_rate_s))
        run_cmd("./ec2-exp.sh start-sparrow -i %s" % private_ssh_key)
        print "*******Sleeping after starting Sparrow"
        time.sleep(10)
        print "********Starting prototype frontends and backends"
        run_cmd("./ec2-exp.sh start-proto -i %s" % private_ssh_key)
        time.sleep(trial_length)

        log_dirname = "%s_%s" % (utilization, sample_ratio)
        if not os.path.exists(log_dirname):
            os.mkdir(log_dirname)
        else:
            print "********Error: log directory (%s) already exists. Exiting." % log_dirname
            exit(1)

        print "********Stopping prototypes and Sparrow"
        run_cmd("./ec2-exp.sh stop-proto -i %s" % private_ssh_key)
        run_cmd("./ec2-exp.sh stop-sparrow -i %s" % private_ssh_key)

        print "********Collecting logs and placing in %s" % log_dirname
        run_cmd("./ec2-exp.sh collect-logs -i %s --log-dir=%s/" % (private_ssh_key, log_dirname))
        run_cmd("gunzip %s/*.gz" % log_dirname)

        print "********Parsing logs"
        run_cmd(("cd ../../src/main/python/ && ./parse_logs.sh log_dir=../../../deploy/ec2/%s "
                 "output_dir=../../../deploy/ec2/%s/results start_sec=60 end_sec=180 && cd -") %
                (log_dirname, log_dirname))

