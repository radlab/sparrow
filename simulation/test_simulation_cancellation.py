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

import unittest

import simulation_cancellation
import util

class TestMultiGetSimulation(unittest.TestCase):
    def setUp(self):
        simulation_cancellation.CANCELLATION = True
        self.simulation = simulation_cancellation.Simulation(5, "", 0.9,
                                                             util.TaskDistributions.CONSTANT)

    def test_workers_tracked_correctly(self):
        job_id = 13
        job_start = 10
        num_tasks = 2
        job = util.Job(num_tasks, job_start, util.TaskDistributions.CONSTANT, 100)
        self.assertEqual(job.num_tasks, num_tasks)
        self.assertEqual(len(job.unscheduled_tasks), num_tasks)

        probe_events = self.simulation.send_probes(job, job_start)
        self.assertEqual(len(job.probed_workers), 2*num_tasks)

        # Run the first probe event.
        time, first_probe = probe_events[0]
        # All workers are idle, so should get a task end event back.
        events = first_probe.run(time)
        self.assertEquals(len(events), 1)
        self.assertEquals(len(job.probed_workers), 2*num_tasks - 1)

        time, second_probe = probe_events[1]
        events = second_probe.run(time)
        # This time, should get cancellation events back (in addition to task end event).
        self.assertEquals(len(events), 1 + num_tasks)
        self.assertEquals(len(job.probed_workers), num_tasks)

        # Make sure everything works fine if a worker replies to a probe before realizing it was
        # cancelled.
        time, third_probe = probe_events[2]
        events = third_probe.run(time)
        # Should just get a no-op event back.
        self.assertEquals(len(events), 1)

if __name__ == '__main__':
    unittest.main()