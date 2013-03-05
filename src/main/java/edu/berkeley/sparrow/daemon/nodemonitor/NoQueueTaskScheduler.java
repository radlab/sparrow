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
  protected void handleTaskCompleted(String requestId, String lastExecutedTaskRequestId,
                                     String lastExecutedTaskId) {
    // Do nothing
  }


  @Override
  int getMaxActiveTasks() {
    return -1;
  }

}
