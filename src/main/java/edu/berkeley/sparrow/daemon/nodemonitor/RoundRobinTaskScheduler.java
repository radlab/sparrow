package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceUsage;

/**
 * A {@link TaskScheduler} which round-robins requests over backlogged per-app queues.
 *
 * NOTE: This currently round-robins over users, rather than applications. Not sure
 * what we want here going forward.
 */
public class RoundRobinTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(RoundRobinTaskScheduler.class);

  private HashMap<String, Queue<TaskSpec>> userQueues = Maps.newHashMap();
  int numQueuedReservations;

  public int maxActiveTasks;
  public Integer activeTasks;

  private ArrayList<String> users = new ArrayList<String>();
  private int currentIndex = 0; // Round robin index, always used (mod n) where n is
                                // the number of apps.

  public RoundRobinTaskScheduler(int maxActiveTasks) {
    this.maxActiveTasks = maxActiveTasks;
    activeTasks = 0;
    numQueuedReservations = 0;
  }

  @Override
  synchronized int handleSubmitTaskReservation(TaskSpec taskReservation) {
     /* Because of the need to check the free resources and then, depending on the result, start a
      * new task, this method must be synchronized.
      */
    String user = taskReservation.user.getUser();
    if (!userQueues.containsKey(user)) {
      users.add(user);
      userQueues.put(user, new LinkedList<TaskSpec>());
    }

    if (activeTasks < maxActiveTasks) {
      if (numQueuedReservations > 0) {
        String errorMessage = "activeTasks should be less than maxActiveTasks only " +
            "when no outstanding reservations.";
        LOG.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      }
      makeTaskRunnable(taskReservation);
      // Set current Index to immediately after this user, to reflect the fact that a task for this
      // user was the most recent one to run.
      currentIndex = (users.indexOf(taskReservation.user.getUser()) + 1) % users.size();
      ++activeTasks;
      LOG.debug("Making task for request " + taskReservation.requestId + " runnable (" +
                activeTasks + " of " + maxActiveTasks + " task slots currently filled)");
      return 0;
    }

    LOG.debug("All " + maxActiveTasks + " task slots filled.");
    Queue<TaskSpec> reservations = userQueues.get(user);
    LOG.debug("Adding reservation for user " + user + ". " + reservations.size() +
              " reservations already queued for user, and " + numQueuedReservations +
              " total reservations queued.");
    reservations.add(taskReservation);
    return ++numQueuedReservations;
  }

  @Override
  protected synchronized void handleTaskCompleted(
      String requestId, String lastExecutedTaskRequestId, String lastExecutedTaskId) {
    if (numQueuedReservations != 0) {
      /* Scan through the list of apps (starting at currentIndex) and find the first
       * one with a pending task. If we find a pending task, make that task runnable
       * and update the round robin index.
       *
       * Note that this implementation assumes that we can take an arbitrary task and,
       * by virtue of a task having just finished, have enough resources to execute it.
       * This makes sense for scheduling similarly sized tasks (e.g. just scheduling cores)
       * but will not be the case if tasks take different amounts of resources. */
      for (int offset = 0; offset < users.size(); offset++) {
        String user = users.get((currentIndex + offset) % users.size());
        Queue<TaskSpec> considering = userQueues.get(user);
        TaskSpec nextTask = considering.poll();
        if (nextTask != null) {
          LOG.debug("Task for user " + user + ", request " + nextTask.requestId +
                    " now runnable.");
          nextTask.previousRequestId = lastExecutedTaskRequestId;
          nextTask.previousTaskId = lastExecutedTaskId;
          makeTaskRunnable(nextTask);
          currentIndex = (currentIndex + offset + 1) % users.size();
          numQueuedReservations--;
          /* Never remove users from the queue, because it will make currentIndex no longer right,
           * and we don't expect user churn right now. */
          return;
        }
        LOG.debug("Skipping user " + user + " that has no runnable tasks.");
      }
      String errorMessage = ("numQueuedReservations=" + numQueuedReservations +
                             " but no queued tasks found.");
      throw new IllegalStateException(errorMessage);
    } else {
      LOG.debug("No queued tasks, so not launching anything.");
      activeTasks -= 1;
    }
  }

  /**
   * THIS DOES NOT CURRENTLY WORK, because we don't track resource usage per-app.
   */
  @Override
  public synchronized TResourceUsage getResourceUsage(String appId) {
    TResourceUsage out = new TResourceUsage();
    out.resources = TResources.subtract(capacity, getFreeResources());
    // We use one shared queue for all apps here
    if (userQueues.containsKey(appId)) {
      out.queueLength = userQueues.get(appId).size();
    } else {
      LOG.info("Got resource request for application I've never seen: " + appId);
      out.queueLength = 0;
    }
    return out;
  }

}
