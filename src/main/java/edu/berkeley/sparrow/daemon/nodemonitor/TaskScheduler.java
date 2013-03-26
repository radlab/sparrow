package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import edu.berkeley.sparrow.daemon.util.Hostname;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * A TaskScheduler is a buffer that holds tasks between when they are launched on a
 * {@link NodeMonitor} and when they are passed to application backends.
 *
 * Each scheduler will implement a different policy determining when to launch tasks.
 *
 * Schedulers are required to be thread safe, as they will be accessed concurrently from
 * multiple threads.
 */
public abstract class TaskScheduler {
  private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(
      TaskScheduler.class);
  private String ipAddress;
  class TaskDescription {
    ByteBuffer message;
    public TFullTaskId taskId;
    public TResourceVector estimatedResources;
    public TUserGroupInfo user;
    public InetSocketAddress backendSocket;

    public TaskDescription(TFullTaskId taskId, ByteBuffer message,
        TResourceVector estimatedResources, TUserGroupInfo user, InetSocketAddress addr) {
      this.message = message;
      this.taskId = taskId;
      this.estimatedResources = estimatedResources;
      this.user = user;
      this.backendSocket = addr;
    }
  }

  protected TResourceVector capacity;
  protected Configuration conf;
  protected TResourceVector inUse = TResources.clone(TResources.none());
  private final BlockingQueue<TaskDescription> runnableTaskQueue =
      new LinkedBlockingQueue<TaskDescription>();
  // Task ids of tasks that have been made runnable at some point
  private final HashSet<TFullTaskId> runnableTaskIds = Sets.newHashSet();
  private HashMap<TFullTaskId, TResourceVector> resourcesPerTask = new
      HashMap<TFullTaskId, TResourceVector>();

  /** Initialize the task scheduler, passing it the current available resources
   *  on the machine. */
  void initialize(TResourceVector capacity, Configuration conf) {
    this.capacity = capacity;
    this.conf = conf;
    this.ipAddress = Hostname.getIPAddress(conf);
  }

  /**
   * Get the next task available for launching. This will block until a task is available.
   */
  TaskDescription getNextTask() {
    TaskDescription task = null;
    try {
      task = runnableTaskQueue.take();
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    TResourceVector singleCoreResourceUsage = new TResourceVector(0, 1);
    addResourceInUse(singleCoreResourceUsage);
    return task;
  }

  /**
   * Returns the current number of runnable tasks (for testing).
   */
  int runnableTasks() {
    return runnableTaskQueue.size();
  }

  synchronized void tasksFinished(List<TFullTaskId> finishedTasks) {
    for (TFullTaskId t : finishedTasks) {
      taskCompleted(t);
    }
  }

  protected void makeTaskRunnable(TaskDescription task) {
    runnableTaskIds.add(task.taskId);
    AUDIT_LOG.info(
        Logging.auditEventString("nodemonitor_task_runnable", ipAddress,
            task.taskId.requestId, task.taskId.taskId));
    try {
      runnableTaskQueue.put(task);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
  }

  protected void taskCompleted(TFullTaskId taskId) {
    AUDIT_LOG.info(
        Logging.auditEventString("nodemonitor_task_completed", taskId.requestId,
            taskId.taskId));
    TResourceVector res = resourcesPerTask.get(taskId);
    if (res == null) {
      LOG.debug("Missing resources for task :" + taskId);
      res = TResources.createResourceVector(0, 1);
    }
    resourcesPerTask.remove(taskId);
    freeResourceInUse(res);
    handleTaskCompleted(taskId);
  }

   void submitTask(TaskDescription task, String appId) {
    AUDIT_LOG.info(Logging.auditEventString("nodemonitor_task_submitted", ipAddress,
        task.taskId.requestId, task.taskId.taskId));
    TResourceVector singleCoreResourceUsage = new TResourceVector(0, 1);
    resourcesPerTask.put(task.taskId, singleCoreResourceUsage);
    handleSubmitTask(task, appId);
  }

  protected synchronized void addResourceInUse(TResourceVector nowInUse) {
    TResources.addTo(inUse, nowInUse);
  }

  protected synchronized void freeResourceInUse(TResourceVector nowFreed) {
    TResources.subtractFrom(inUse, nowFreed);
  }

  /**
   * Return the quantity of free resources on the node. Free resources are determined
   * by subtracting the currently used resources and currently runnable resources from
   * the node's capacity.
   */
  protected TResourceVector getFreeResources() {
    TResourceVector free = TResources.subtract(capacity, inUse);
    TResourceVector reserved = TResources.none();
    for (TaskDescription t: runnableTaskQueue) {
      reserved = TResources.add(reserved, t.estimatedResources);
    }
    return TResources.subtract(free, reserved);
  }

  // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING

  /**
   * Submit a task to the scheduler.
   */
  abstract void handleSubmitTask(TaskDescription task, String appId);

  /**
   * Signal that a given task has completed.
   */
  protected abstract void handleTaskCompleted(TFullTaskId taskId);

  /**
   * Returns the current resource usage. If the resource usage is equal to the
   * machines capacity, this will include the queue length for appId.
   */
  abstract TResourceUsage getResourceUsage(String appId);
}
