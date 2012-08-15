package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;

/**
 * A {@link TaskScheduler} which round-robins requests over backlogged per-app queues.
 * 
 * NOTE: This current round-robins over applications, rather than users. Not sure
 * what we want here going forward.
 */
public class RoundRobinTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(RoundRobinTaskScheduler.class);

  private HashMap<String, Queue<TaskDescription>> appQueues = 
      new HashMap<String, Queue<TaskDescription>>();

  private ArrayList<String> apps = new ArrayList<String>();
  private int currentIndex = 0; // Round robin index, always used (mod n) where n is
                                // the number of apps.

  @Override
  void handleSubmitTask(TaskDescription task, String appId) {
    if (TResources.isLessThanOrEqualTo(task.estimatedResources, getFreeResources())) {
      LOG.info("Task: " + task.taskId + " instantly runnable. " 
        + task.estimatedResources + "<=" + getFreeResources());
      makeTaskRunnable(task);
    } else {
      addTaskToAppQueue(appId, task);
    }
  }
  
  void addTaskToAppQueue(String app, TaskDescription task) {
    synchronized(appQueues) {
      if (!appQueues.containsKey(app)) {
        appQueues.put(app, new LinkedList<TaskDescription>());
        apps.add(app);
      }
      appQueues.get(app).add(task);
    }
  }
  
  void removeTaskFromAppQueue(String app, TaskDescription task) {
    synchronized(appQueues) {
      appQueues.get(app).remove(task);
      if (appQueues.get(app).size() == 0) {
        appQueues.remove(app);
        apps.remove(app);
      }
    }
  }

  @Override
  protected void handleTaskCompleted(TFullTaskId taskId) {
    synchronized(appQueues) {
      /* Scan through the list of apps (starting at currentIndex) and find the first
       * one with a pending task. If we find a pending task, make that task runnable
       * and update the round robin index.
       * 
       * Note that this implementation assumes that we can take an arbitrary task and,
       * by virtue of a task having just finished, have enough resources to execute it. 
       * This makes sense for scheduling similar sized tasks (e.g. just scheduling cores)
       * but will not be the case if tasks take different amounts of resources. */
      for (int i = 0; i < apps.size(); i++) {
        String app = apps.get((currentIndex + i) % apps.size());
        Queue<TaskDescription> considering = appQueues.get(app);
        TaskDescription nextTask = considering.poll();
        if (nextTask == null) {
          // Shouldn't get here if we are removing non-empty queues
          continue;
        }
        else {
          LOG.info("Task: " + nextTask.taskId + " now runnable");
          makeTaskRunnable(nextTask);
          removeTaskFromAppQueue(app, nextTask);
          currentIndex = currentIndex + i + 1;
          return;
        }
      }
      // No one had a task, so do nothing.
    }
  }

  @Override
  TResourceUsage getResourceUsage(String appId) {
    TResourceUsage out = new TResourceUsage();
    out.resources = TResources.subtract(capacity, getFreeResources());
    // We use one shared queue for all apps here
    if (appQueues.containsKey(appId)) {
      out.queueLength = appQueues.get(appId).size();
    } else {
      LOG.info("Got resource request for application I've never seen: " + appId);
      out.queueLength = 0;
    }
    return out;
  }

}
