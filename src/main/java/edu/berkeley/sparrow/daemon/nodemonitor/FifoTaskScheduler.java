package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;
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
      try {
        runnableTaskQueue.put(task);
      } catch (InterruptedException e) {
        LOG.fatal(e);
      }
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
  protected synchronized void handleTaskCompleted(String taskId) {
    activeTasks.decrementAndGet();
    if (!tasks.isEmpty()) {
      try {
        runnableTaskQueue.put(tasks.poll());
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.exit(0);
      }
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
