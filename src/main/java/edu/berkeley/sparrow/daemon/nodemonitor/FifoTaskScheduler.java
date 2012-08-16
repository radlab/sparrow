package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceUsage;

/**
 * This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and uses a FIFO queue to determine the order to launch
 * tasks whenever outstanding tasks exceed this amount.
 */
public class FifoTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);
  public int maxActiveTasks = 4;
  public Integer activeTasks = 0;
  public LinkedBlockingQueue<TaskReservation> taskReservations =
      new LinkedBlockingQueue<TaskReservation>();

  public void setMaxActiveTasks(int max) {
    this.maxActiveTasks = max;
  }
  
  @Override
  synchronized void handleSubmitTaskReservation(TaskReservation taskReservation) {
    synchronized(activeTasks) {
      if (activeTasks < maxActiveTasks) {
        makeTaskRunnable(taskReservation);
        ++activeTasks;
        LOG.debug("Launching task for request " + taskReservation.requestId + " (" + activeTasks +
                  " of " + maxActiveTasks + " task slots currently filled)");
        return;
      } else {
        LOG.debug("All " + maxActiveTasks + " task slots filled.");
      }
    }
    try {
      LOG.debug("Enqueueing task reservation with request id " + taskReservation.requestId +
               " because all task slots filled.");
      taskReservations.put(taskReservation);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
  }

  @Override
  protected void handleTaskCompleted(String requestId) {
    if (!taskReservations.isEmpty()) {
      makeTaskRunnable(taskReservations.poll());
    } else {
      synchronized(activeTasks) {
        activeTasks -= 1;
      }
    }
  }

  @Override
  TResourceUsage getResourceUsage(String appId) {
    TResourceUsage out = new TResourceUsage();
    out.resources = TResources.subtract(capacity, getFreeResources());
    // We use one shared queue for all apps here
    out.queueLength = taskReservations.size();
    return out;
  }

}
