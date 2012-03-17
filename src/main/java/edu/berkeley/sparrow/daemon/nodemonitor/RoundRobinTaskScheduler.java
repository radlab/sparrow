package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;

/**
 * A {@link TaskScheduler} which round-robins requests over backlogged per-user queues.
 */
public class RoundRobinTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(RoundRobinTaskScheduler.class);

  private HashMap<String, Queue<TaskDescription>> userQueues = 
      new HashMap<String, Queue<TaskDescription>>();

  private ArrayList<String> users = new ArrayList<String>();
  private int currentIndex = 0; // Round robin index, always used (mod n) where n is
                                // the number of users.

  @Override
  void handleSubmitTask(TaskDescription task) {
    if (TResources.isLessThanOrEqualTo(task.estimatedResources, getFreeResources())) {
      try {
        runnableTaskQueue.put(task);
      } catch (InterruptedException e) {
      }
    } else {
      addTaskToUserQueue(task.user.user, task);
    }
  }
  
  void addTaskToUserQueue(String user, TaskDescription task) {
    synchronized(userQueues) {
      if (!userQueues.containsKey(user)) {
        userQueues.put(user, new LinkedList<TaskDescription>());
        users.add(user);
      }
      userQueues.get(user).add(task);
    }
  }
  
  void removeTaskFromUserQueue(String user, TaskDescription task) {
    synchronized(userQueues) {
      userQueues.get(user).remove(task);
      if (userQueues.get(user).size() == 0) {
        userQueues.remove(user);
        users.remove(user);
      }
    }
  }

  @Override
  protected void handleTaskCompleted(String taskId) {
    synchronized(userQueues) {
      /* Scan through the list of users (starting at currentIndex) and find the first
       * one with a pending tasks. If we find a pending task, make that task runnable
       * and update the round robin index.
       * 
       * Note that this implementation assumes that we can take an arbitrary task and,
       * by virtue of a task having just finished, have enough resources to execute it. 
       * This makes sense for scheduling similar sized tasks (e.g. just scheduling cores)
       * but will not be the case if tasks take different amounts of resources. */
      for (int i = 0; i < users.size(); i++) {
        String user = users.get((currentIndex + i) % users.size());
        Queue<TaskDescription> considering = userQueues.get(user);
        TaskDescription nextTask = considering.poll();
        if (nextTask == null) {
          // Shouldn't get here if we are removing non-empty queues
          continue;
        }
        else {
          try {
            runnableTaskQueue.put(nextTask);
            removeTaskFromUserQueue(user, nextTask);
            currentIndex = currentIndex + i + 1;
            return;
          } catch (InterruptedException e) {
            LOG.fatal(e);
          }
        }
      }
      // No one had a task, so do nothing.
    }
  }

}
