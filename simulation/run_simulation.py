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

import simulation_random
import simulation_batch
import simulation_centralized
import simulation_multi
import simulation_cancellation
import util

def get_percentile(N, percent, key=lambda x:x):
    if not N:
        return 0
    k = (len(N) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0 + d1

NUM_JOBS = 10000
DISTRIBUTION = util.TaskDistributions.EXP_JOBS

loads = [0.1, 0.3, 0.5, 0.7, 0.8, 0.9, 0.95]
#loads = [0.95]
loads.reverse()
for load in loads:
    print "Running simulations at %s load" % load

    print "******Multiget"
    s = simulation_multi.Simulation(NUM_JOBS, "multi_tasks_%s" % load, load, DISTRIBUTION)
    s.run()


    print "Sparrow"
    simulation_cancellation.WORK_STEALING = False
    simulation_cancellation.CANCELLATION = False
    s = simulation_cancellation.Simulation(NUM_JOBS, "sparrow_%s" % load, load, DISTRIBUTION)
    s.run()

    print "Cancellation"
    simulation_cancellation.WORK_STEALING = False
    simulation_cancellation.CANCELLATION = True
    s = simulation_cancellation.Simulation(NUM_JOBS, "cancellation_%s" % load, load, DISTRIBUTION)
    s.run()

    print "Centralized"
    s = simulation_centralized.Simulation(NUM_JOBS, "centralized_%s" % load, load, DISTRIBUTION)
    s.run()

    print "Random"
    s = simulation_random.Simulation(NUM_JOBS, "random_%s" % load, load, DISTRIBUTION)
    s.run()

    print "Per task"
    simulation_batch.PER_TASK = True
    s = simulation_batch.Simulation(NUM_JOBS, "per_task_%s" % load, load, DISTRIBUTION)
    s.run()

    print "Batch sampling"
    simulation_batch.PER_TASK = False
    s = simulation_batch.Simulation(NUM_JOBS, "batch_%s" % load, load, DISTRIBUTION)
    s.run()

    continue
    print "Work Stealing (10000 schedulers)"
    simulation_cancellation.WORK_STEALING = True
    simulation_cancellation.NUM_SCHEDULERS = 10000
    simulation_cancellation.CANCELLATION = False
    s = simulation_cancellation.Simulation(NUM_JOBS, "stealing_s10000_%s" % load, load, DISTRIBUTION)
    s.run()

    continue


