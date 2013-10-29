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

import random

class TaskDistributions:
    EXP_TASKS, EXP_JOBS, CONSTANT = range(3)

class Job(object):
    job_count = 0
    def __init__(self, num_tasks, start_time, task_distribution, median_task_duration, scheduler=0):
        self.id = Job.job_count
        Job.job_count += 1
        self.num_tasks = num_tasks
        self.completed_tasks_count = 0
        self.start_time = start_time
        self.end_time = start_time
        self.unscheduled_tasks = []
        # TODO: This won't be correctly populated when tasks are stolen.
        self.time_all_tasks_scheduled = 0
        self.last_probe_reply_time= 0
        # Change this line to change to distribution of task durations.
        if task_distribution == TaskDistributions.EXP_TASKS:
            self.exponentially_distributed_tasks(median_task_duration)
        elif task_distribution == TaskDistributions.EXP_JOBS:
            self.exponentially_distributed_jobs(median_task_duration)
        elif task_distribution == TaskDistributions.CONSTANT:
            self.constant_distributed_tasks(median_task_duration)
        self.longest_task = max(self.unscheduled_tasks)

        self.scheduler = scheduler
        self.probed_workers = set()

    def add_probe_response(self, worker, current_time):
        self.probed_workers.remove(worker.id)
        assert current_time >= self.last_probe_reply_time
        self.last_probe_reply_time = current_time
        if len(self.unscheduled_tasks) > 0:
            assert current_time >= self.time_all_tasks_scheduled
            self.time_all_tasks_scheduled = current_time

    def task_completed(self, completion_time):
        """ Returns true if the job has completed, and false otherwise. """
        self.completed_tasks_count += 1
        self.end_time = max(completion_time, self.end_time)
        assert self.completed_tasks_count <= self.num_tasks
        return self.num_tasks == self.completed_tasks_count

    def exponentially_distributed_tasks(self, median_task_duration):
        while len(self.unscheduled_tasks) < self.num_tasks:
            # Exponentially distributed task durations.
            self.unscheduled_tasks.append(random.expovariate(1.0 / median_task_duration))

    def exponentially_distributed_jobs(self, median_task_duration):
        # Choose one exponentially-distributed task duration for all tasks in the job.
        task_duration = random.expovariate(1.0 / median_task_duration)
        while len(self.unscheduled_tasks) < self.num_tasks:
            self.unscheduled_tasks.append(task_duration)

    def constant_distributed_tasks(self, median_task_duration):
        while len(self.unscheduled_tasks) < self.num_tasks:
            self.unscheduled_tasks.append(median_task_duration)
