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
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * A {@link TaskScheduler} that launches tasks in strict priority order. Priorities are
 * integral values specified in the scheduling request.
 *
 * TODO: Would be better to pre-configure priorities -- users shouldn't be able to specify
 * whatever priority they want in the scheduling request.
 */
public class PriorityTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(PriorityTaskScheduler.class);

  /** Queue of tasks of each priority. */
  private TreeMap<Integer, Queue<TaskSpec>> priorityQueues = Maps.newTreeMap();
  int numQueuedReservations;

  public int maxActiveTasks;
  public Integer activeTasks;

  public PriorityTaskScheduler(int maxActiveTasks) {
    this.maxActiveTasks = maxActiveTasks;
    activeTasks = 0;
    numQueuedReservations = 0;
  }

  @Override
  synchronized int handleSubmitTaskReservation(TaskSpec taskReservation) {
     /* Because of the need to check the free resources and then, depending on the result, start a
      * new task, this method must be synchronized.
      */
    int priority = taskReservation.user.getPriority();
    if (!priorityQueues.containsKey(priority)) {
      priorityQueues.put(priority, new LinkedList<TaskSpec>());
    }

    if (activeTasks < maxActiveTasks) {
      if (numQueuedReservations > 0) {
        String errorMessage = "activeTasks should be less than maxActiveTasks only " +
            "when no outstanding reservations.";
        LOG.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      }
      makeTaskRunnable(taskReservation);
      ++activeTasks;
      LOG.debug("Making task for request " + taskReservation.requestId + " with priority " +
                priority + " runnable (" + activeTasks + " of " + maxActiveTasks +
                " task slots currently filled)");
      return 0;
    }

    LOG.debug("All " + maxActiveTasks + " task slots filled.");
    Queue<TaskSpec> reservations = priorityQueues.get(priority);
    LOG.debug("Adding reservation for priority " + priority + ". " + reservations.size() +
              " reservations already queued for that priority, and " + numQueuedReservations +
              " total reservations queued.");
    reservations.add(taskReservation);
    return ++numQueuedReservations;
  }

  @Override
  synchronized int cancelTaskReservations(String requestId) {
    int numReservationsCancelled = 0;
    for (Queue<TaskSpec> taskSpecs : priorityQueues.values()) {
      Iterator<TaskSpec> iterator = taskSpecs.iterator();
      while (iterator.hasNext()) {
        TaskSpec reservation = iterator.next();
        if (reservation.requestId == requestId) {
          ++numReservationsCancelled;
          iterator.remove();
        }
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
    if (numQueuedReservations != 0) {
      // Launch a task for the lowest valued priority with queued tasks.
      for (Entry<Integer, Queue<TaskSpec>> entry : priorityQueues.entrySet()) {
        TaskSpec nextTask = entry.getValue().poll();
        if (nextTask != null) {
          LOG.debug("Launching task for request " + nextTask.requestId + " (priority " +
                    entry.getKey() + ")");
          nextTask.previousRequestId = lastExecutedRequestId;
          nextTask.previousTaskId = lastExecutedTaskId;
          makeTaskRunnable(nextTask);
          numQueuedReservations--;
          return;
        }
      }
      String errorMessage = ("numQueuedReservations=" + numQueuedReservations +
          " but no queued tasks found.");
      throw new IllegalStateException(errorMessage);
    } else {
      LOG.debug("No queued tasks, so not launching anything.");
      activeTasks -= 1;
    }
  }

  @Override
  int getMaxActiveTasks() {
    return maxActiveTasks;
  }

}
