package edu.berkeley.sparrow.daemon.nodemonitor;

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
