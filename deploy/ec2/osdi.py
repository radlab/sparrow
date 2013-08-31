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

import os
import subprocess
import time

def run_cmd(cmd):
  subprocess.check_call(cmd, shell=True)

# 50-node: q1, 3.5, par 10, sf .5 = 60% utilization
# 50-node: q6, 4, par 10, sf .5 = 60% utilization
# 50-node: q3, 2, par 10, sf .5 = 50% utilization
# 100-node: q6 (think I ran at 4ps)
# 100-node: q3

trial_length = 300
ratios = [(2, 2)]
rates = [1]
tpch_query = 1
par_level = 20

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
    time.sleep(trial_length + 130) 
    run_cmd("./ec2-exp.sh collect-logs -i ~/.ssh/eastkey.pem --log-dir=%s/" % 
    dirname)
    run_cmd("cd %s && gunzip *.gz && cd -" % dirname)


