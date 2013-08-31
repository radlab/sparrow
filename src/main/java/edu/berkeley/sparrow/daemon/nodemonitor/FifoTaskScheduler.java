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

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and uses a FIFO queue to determine the order to launch
 * tasks whenever outstanding tasks exceed this amount.
 */
public class FifoTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);

  public int maxActiveTasks;
  public Integer activeTasks;
  public LinkedBlockingQueue<TaskSpec> taskReservations =
      new LinkedBlockingQueue<TaskSpec>();

  public FifoTaskScheduler(int max) {
    maxActiveTasks = max;
    activeTasks = 0;
  }

  @Override
  synchronized int handleSubmitTaskReservation(TaskSpec taskReservation) {
    // This method, cancelTaskReservations(), and handleTaskCompleted() are synchronized to avoid
    // race conditions between updating activeTasks and taskReservations.
    if (activeTasks < maxActiveTasks) {
      if (taskReservations.size() > 0) {
        String errorMessage = "activeTasks should be less than maxActiveTasks only " +
                              "when no outstanding reservations.";
        LOG.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      }
      makeTaskRunnable(taskReservation);
      ++activeTasks;
      LOG.debug("Making task for request " + taskReservation.requestId + " runnable (" +
                activeTasks + " of " + maxActiveTasks + " task slots currently filled)");
      return 0;
    }
    LOG.debug("All " + maxActiveTasks + " task slots filled.");
    int queuedReservations = taskReservations.size();
    try {
      LOG.debug("Enqueueing task reservation with request id " + taskReservation.requestId +
                " because all task slots filled. " + queuedReservations +
                " already enqueued reservations.");
      taskReservations.put(taskReservation);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return queuedReservations;
  }

  @Override
  synchronized int cancelTaskReservations(String requestId) {
    int numReservationsCancelled = 0;
    Iterator<TaskSpec> reservationsIterator = taskReservations.iterator();
    while (reservationsIterator.hasNext()) {
      TaskSpec reservation = reservationsIterator.next();
      if (reservation.requestId == requestId) {
        reservationsIterator.remove();
        ++numReservationsCancelled;
      }
    }
    return numReservationsCancelled;
  }

  @Override
  protected void handleTaskFinished(String requestId, String taskId) {
    attemptTaskLaunch(requestId, taskId);
  }

  @Override
  protected void handleNoTaskForReservation(TaskSpec taskSpec) {
    attemptTaskLaunch(taskSpec.previousRequestId, taskSpec.previousTaskId);
  }

  /**
   * Attempts to launch a new task.
   *
   * The parameters {@code lastExecutedRequestId} and {@code lastExecutedTaskId} are used purely
   * for logging purposes, to determine how long the node monitor spends trying to find a new
   * task to execute. This method needs to be synchronized to prevent a race condition with
   * {@link handleSubmitTaskReservation}.
   */
  private synchronized void attemptTaskLaunch(
      String lastExecutedRequestId, String lastExecutedTaskId) {
    TaskSpec reservation = taskReservations.poll();
    if (reservation != null) {
      reservation.previousRequestId = lastExecutedRequestId;
      reservation.previousTaskId = lastExecutedTaskId;
      makeTaskRunnable(reservation);
    } else {
      activeTasks -= 1;
    }
  }

  @Override
  int getMaxActiveTasks() {
    return maxActiveTasks;
  }
}
