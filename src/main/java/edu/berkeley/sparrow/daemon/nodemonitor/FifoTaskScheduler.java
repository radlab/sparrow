package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;

/** This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and FIFO's whenever outstanding tasks exceed 
 * this amount.
 */
public class FifoTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);
  public int maxActiveTasks = 4;
  public AtomicInteger activeTasks = new AtomicInteger(0);
  public LinkedBlockingQueue<TaskDescription> tasks = new LinkedBlockingQueue<TaskDescription>();

  public void setMaxActiveTasks(int max) {
    this.maxActiveTasks = max;
  }
  
  @Override
  synchronized void handleSubmitTask(TaskDescription task, String appId) {
    if (activeTasks.get() < maxActiveTasks) {
      makeTaskRunnable(task);
      activeTasks.incrementAndGet();
    } else {
      try {
        tasks.put(task);
      } catch (InterruptedException e) {
        LOG.fatal(e);
      }
    }
  }

  @Override
  protected void handleTaskCompleted(TFullTaskId taskId) {
    activeTasks.decrementAndGet();
    if (!tasks.isEmpty()) {
      makeTaskRunnable(tasks.poll());
      activeTasks.incrementAndGet();
    }
  }

  @Override
  TResourceUsage getResourceUsage(String appId) {
    TResourceUsage out = new TResourceUsage();
    out.resources = TResources.subtract(capacity, getFreeResources());
    // We use one shared queue for all apps here
    out.queueLength = tasks.size();
    return out;
  }

}
