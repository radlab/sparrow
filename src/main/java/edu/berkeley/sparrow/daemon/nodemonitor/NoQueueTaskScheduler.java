/*
 * Copyright 2013 The Regents of The University California
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.daemon.nodemonitor;




/**
 * A {@link TaskScheduler} which makes tasks instantly available for launch.
 *
 * This does not perform any resource management or queuing. It can be used for
 * applications which do not want Sparrow to perform any explicit resource management
 * but still want Sparrow to launch tasks.
 */
public class NoQueueTaskScheduler extends TaskScheduler {

  @Override
  int handleSubmitTaskReservation(TaskSpec taskReservation) {
    // Make this task instantly runnable
    makeTaskRunnable(taskReservation);
    return 0;
  }

  @Override
  int cancelTaskReservations(String requestId) {
    // Do nothing. No reservations cancelled.
    return 0;
  }

  @Override
  protected void handleTaskFinished(String requestId, String taskId) {
    // Do nothing.

  }

  @Override
  protected void handleNoTaskForReservation(TaskSpec taskSpec) {
    // Do nothing.

  }


  @Override
  int getMaxActiveTasks() {
    return -1;
  }

}
