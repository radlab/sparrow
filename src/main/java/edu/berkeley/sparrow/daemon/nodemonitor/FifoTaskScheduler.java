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
  public LinkedBlockingQueue<TaskSpec> taskReservations =
      new LinkedBlockingQueue<TaskSpec>();

  public void setMaxActiveTasks(int max) {
    this.maxActiveTasks = max;
  }

  @Override
  synchronized int handleSubmitTaskReservation(TaskSpec taskReservation) {
    // This method and handleTaskCompleted() are synchronized to avoid race conditions between
    // updating activeTasks and taskReservations.
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
    } else {
      LOG.debug("All " + maxActiveTasks + " task slots filled.");
    }
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
  synchronized protected void handleTaskCompleted(
      String requestId, String lastExecutedTaskRequestId, String lastExecutedTaskId) {
    TaskSpec reservation = taskReservations.poll();
    if (reservation != null) {
      reservation.previousRequestId = lastExecutedTaskRequestId;
      reservation.previousTaskId = lastExecutedTaskId;
      makeTaskRunnable(reservation);
    } else {
      activeTasks -= 1;
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
