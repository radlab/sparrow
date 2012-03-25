""" Tests for simulation code. """

import unittest
import simulation

class TestServer(unittest.TestCase):
    def setUp(self):
        self.stats_manager = simulation.StatsManager()
        simulation.set_param("network_delay", 10)
        
    def test_probe_load_estimates(self):
        server = simulation.Server("test", self.stats_manager, 1)
        simulation.set_param("load_metric", "estimate")
        # Ensure that estimates are being incorporated into load.
        self.assertEquals(0, server.probe_load(0, 0))
        self.assertEquals(1, server.probe_load(0, 2))
        self.assertEquals(2, server.probe_load(0, 14))
        self.assertEquals(2, server.probe_load(0, 20))
        
        # Now ensure that actual queue length is being incorporated as well.
        server.queued_tasks = 5
        self.assertEquals(7, server.probe_load(0, 25))
        
    def test_probe_load_per_user(self):
        simulation.set_param("relative_weights", "1,1,1,1,1")
        server = simulation.Server("test", self.stats_manager, 5)
        server.current_user = 3
        server.running_tasks = 1
        server.task_count = 1
        simulation.set_param("load_metric", "per_user_estimate")
        
        # Add fake jobs to the queues on the server (ok to just use strings
        # in the queue, since probe load only looks at the queue length).
        num_jobs_per_user = [1, 3, 2, 5, 4]
        for user, num_jobs in enumerate(num_jobs_per_user):
            server.queued_tasks += num_jobs
            for i in range(num_jobs):
                server.queues[user].append("foo")
        
        current_time = 0
        self.assertEqual(7, server.probe_load(0, current_time))
        self.assertEqual(14, server.probe_load(1, current_time))
        self.assertEqual(12, server.probe_load(2, current_time))
        self.assertEqual(16, server.probe_load(3, current_time))
        self.assertEqual(15, server.probe_load(4, current_time))
        
    def test_probe_load_per_user_weighted_simple(self):
        simulation.set_param("load_metric", "per_user_estimate")
        simulation.set_param("relative_weights", "2,2")
        server = simulation.Server("test", self.stats_manager, 2)
        server.current_user = 0
        server.task_count = 1
        server.running_tasks = 1
        server.queued_tasks = 2
        
        num_jobs_per_user= [1,1]
        for user, num_jobs in enumerate(num_jobs_per_user):
            for i in range(num_jobs):
                server.queues[user].append("foo")
                
        current_time = 0
        self.assertEqual(3, server.probe_load(0, current_time))
        self.assertEqual(3, server.probe_load(1, current_time))
        
    def test_probe_load_per_user_weighted_incorporates_task_count(self):
        simulation.set_param("load_metric", "per_user_estimate")
        simulation.set_param("relative_weights", "2,2")
        server = simulation.Server("test", self.stats_manager, 2)
        server.current_user = 0
        server.task_count = 2
        server.queued_tasks = 2
        server.running_tasks = 1
        
        num_jobs_per_user= [1,1]
        for user, num_jobs in enumerate(num_jobs_per_user):
            for i in range(num_jobs):
                server.queues[user].append("foo")
                
        current_time = 0
        self.assertEqual(3, server.probe_load(0, current_time))
        self.assertEqual(2, server.probe_load(1, current_time))
        
    def test_probe_load_per_user_weighted(self):
        simulation.set_param("load_metric", "per_user_estimate")
        simulation.set_param("relative_weights", "2,1,1,4,5")
        server = simulation.Server("test", self.stats_manager, 5)
        server.current_user = 3
        server.task_count = 2
        server.running_tasks = 1
        server.queued_tasks = 15
        
        # Add fake jobs to the queues on the server (ok to just use strings
        # in the queue, since probe load only looks at the queue length).
        num_jobs_per_user = [1, 3, 2, 5, 4]
        for user, num_jobs in enumerate(num_jobs_per_user):
            for i in range(num_jobs):
                server.queues[user].append("foo")
        
        current_time = 0
        self.assertEqual(8, server.probe_load(0, current_time))
        self.assertEqual(16, server.probe_load(1, current_time))
        self.assertEqual(16, server.probe_load(2, current_time))
        self.assertEqual(13, server.probe_load(3, current_time))
        self.assertEqual(7, server.probe_load(4, current_time))

class TestFrontEnd(unittest.TestCase):
    def setUp(self):
        self.stats_manager = simulation.StatsManager()
        # Just use a dummy string in place of the servers, since the servers
        # themselves aren't touched during queue selection.
        self.servers = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
        self.queue_lengths = [10, 6, 1, 3, 8, 10, 22, 4, 0, 15]
        self.queues = []
        for server, length in zip(self.servers, self.queue_lengths):
            self.queues.append((server, length))
        self.front_end = simulation.FrontEnd(self.servers,"test",
                                             self.stats_manager)
        
    def assert_lists_equal(self, expected_list, resulting_list):
        self.assertEqual(len(expected_list), len(resulting_list),
                         "Expect list to have %d items; found %d" %
                         (len(expected_list), len(resulting_list)))
        for item in expected_list:
            self.assertTrue(item in resulting_list,
                            "Expect %s to be in resulting list" % str(item))
        
        
    def test_get_best_n_queues_greedy_empty_queues(self):
        """ Tests greedy queue placement when all queues are empty. """
        simulation.set_param("queue_selection", "greedy")
        queues = []
        for server in self.servers:
            queues.append((server, 0))
        placement = self.front_end.get_best_n_queues(queues, 3)
        expected_placement = [("a", 0), ("b", 0), ("c", 0)]
        self.assert_lists_equal(expected_placement, placement)
        
        
    def test_get_best_n_queues_pack_simple(self):
        """ Tests "pack" queue placement in a simple case. """
        simulation.set_param("queue_selection", "pack")
        
        # This should result in placing 4 tasks on "i", 3 tasks on "c", and 1
        # task on "d"
        placement = self.front_end.get_best_n_queues(self.queues, 8)
        expected_placement = [("i", 0), ("i", 1), ("i", 2), ("i", 3), ("c", 1),
                              ("c", 2), ("c", 3), ("d", 3)]
        self.assert_lists_equal(expected_placement, placement)
        
    def test_get_best_n_queues_pack_complex(self):
        """ Tests "pack" placement in a more complex scenario.
        
        For this test, not all queues have the same length at the end.
        """
        simulation.set_param("queue_selection", "pack")
        # This should result in placing 7 tasks on "i", 5 tasks on "c", and 3
        # task on "d".
        placement = self.front_end.get_best_n_queues(self.queues, 15)
        expected_placement = [("i", 0), ("i", 1), ("i", 2), ("i", 3), ("i", 4),
                              ("i", 5), ("c", 1), ("c", 2), ("c", 3), ("c", 4),
                              ("c", 5), ("d", 3), ("d", 4), ("d", 5), ("h", 4)]
        self.assert_lists_equal(expected_placement, placement)
        
    def test_get_best_n_queues_reverse_pack(self):
        """ Tests "reverse_pack" queue placement. """
        simulation.set_param("queue_selection", "reverse_pack")
        placement = self.front_end.get_best_n_queues(self.queues, 2)
        expected_placement = [("i", 0), ("c", 1)]
        self.assert_lists_equal(expected_placement, placement)
        
if __name__ == "__main__":
    unittest.main()