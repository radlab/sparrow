package edu.berkeley.sparrow.daemon.nodemonitor;

import org.apache.log4j.Logger;


/**
 * A {@link TaskScheduler} which makes tasks instantly available for launch.
 * 
 * This does not perform any resource management or queuing. It can be used for 
 * applications which do not want Sparrow to perform any explicit resource management 
 * but still want Sparrow to launch tasks.
 */
public class NoQueueTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(NoQueueTaskScheduler.class);

  @Override
  void handleSubmitTask(TaskDescription task) {
    // Make this task instantly runnable
    try {
      runnableTaskQueue.put(task);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
  }

  @Override
  protected void handleTaskCompleted(String taskId) {
    // Do nothing
  }

}
