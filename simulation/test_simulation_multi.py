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

import simulation_multi

class TestMultiGetSimulation(unittest.TestCase):
    def setUp(self):
        simulation_multi.TOTAL_WORKERS = 10
        self.simulation = simulation_multi.Simulation(5, "", 0.9)

    def test_basic(self):
        TASKS_PER_JOB = 3

        # ADd a job.
        JOB_ID = 8
        JOB_START = 10
        job = simulation_multi.Job(TASKS_PER_JOB, JOB_START)
        self.simulation.jobs[JOB_ID] = job
        self.assertEqual(job.num_tasks, TASKS_PER_JOB)
        self.assertEqual(len(job.unscheduled_tasks), TASKS_PER_JOB)

        # At this point, all workers are idle.
        worker = self.simulation.workers[0]

        # Add a probe for job 0 at 10 millis in.
        events = worker.add_probe(JOB_ID, JOB_START)
        # Should return 4 events: one task ending event for each slot and one no-op.
        self.assertEqual(len(events), 4)
        self.assertEqual(worker.num_free_slots, 0)
        # Run the nop-op event.
        events[-1][1].run(JOB_START + 2)
        self.assertEqual(worker.num_free_slots, 1)

        JOB_ID_2 = 15
        JOB_START_2 = 15
        job2 = simulation_multi.Job(TASKS_PER_JOB, JOB_START_2)
        self.simulation.jobs[JOB_ID_2] = job2

        events = worker.add_probe(JOB_ID_2, JOB_START_2)
        self.assertEqual(worker.num_free_slots, 0)
        # One job start event
        self.assertEqual(len(events), 1)

        # Fake the completion of the first task some time later;
        # should launch the second one.
        new_events = events[0][1].run(JOB_START_2 + 5)
        # Don't keep the job around at this point.
        self.assertEqual(len(new_events), 0)

    def test_multiple_slots_released(self):
        """Ensures that multiple slots are released when a noop comes back."""
        JOB_ID = 20
        JOB_START= 50
        job = simulation_multi.Job(2, JOB_START)
        self.simulation.jobs[JOB_ID] = job

        worker = self.simulation.workers[1]
        self.assertEqual(worker.num_free_slots, 4)
        events = worker.add_probe(JOB_ID, JOB_START)
        self.assertEqual(worker.num_free_slots, 0)
        # The events shoudl include 2 task end events and 1 noop.
        self.assertEqual(len(events), 3)
        # Run the noop event.
        events[-1][1].run(events[-1][0])
        self.assertEqual(worker.num_free_slots, 2)

if __name__ == '__main__':
    unittest.main()