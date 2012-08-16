package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
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
  protected class TaskReservation {
    public String appId;
    public TUserGroupInfo user;
    public String requestId;
    public TResourceVector estimatedResources;
    public InetSocketAddress schedulerAddress;
    public InetSocketAddress appBackendAddress;
    
    public TaskReservation (TEnqueueTaskReservationsRequest request,
                            InetSocketAddress appBackendAddress) {
      appId = request.getAppId();
      user = request.getUser();
      requestId = request.getRequestId();
      estimatedResources = request.getEstimatedResources();
      schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(),
                                               request.getSchedulerAddress().getPort());
      this.appBackendAddress = appBackendAddress;
    }
  }

  private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(
      TaskScheduler.class);
  private String ipAddress;
  
  protected TResourceVector capacity;
  protected Configuration conf;
  protected TResourceVector inUse = TResources.clone(TResources.none());
  private final BlockingQueue<TaskReservation> runnableTaskQueue = 
      new LinkedBlockingQueue<TaskReservation>();
  private HashMap<String, TResourceVector> resourcesPerRequest = new
      HashMap<String, TResourceVector>();
  
  /** Initialize the task scheduler, passing it the current available resources 
   *  on the machine. */
  void initialize(TResourceVector capacity, Configuration conf) {
    this.capacity = capacity;
    this.conf = conf;
    this.ipAddress = Network.getIPAddress(conf);
  }
  
  /**
   * Get the next task available for launching. This will block until a task is available.
   */
  TaskReservation getNextTask() {
    TaskReservation task = null;
    try {
      task = runnableTaskQueue.take();
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    addResourceInUse(task.estimatedResources);
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
      taskCompleted(t.getRequestId());
    }
  }

  /**
   * Signals that a task associated with the given requestId has completed.
   * 
   * TODO: Currently, this may be called by TaskLauncherService to signal that a task was never
   * launched. We may want to separate out the case where a task actually started and finished,
   * and the case where a task was never launched (which occurs if calling getTask() on the
   * scheduler that scheduled the task returns null, likely because all tasks for the request were
   * already scheduled).
   */
  synchronized void taskCompleted(String requestId) {
    AUDIT_LOG.info(
        Logging.auditEventString("nodemonitor_task_completed", requestId));
    TResourceVector res = resourcesPerRequest.get(requestId);
    if (res == null) {
      LOG.error("Missing resources for request: " + requestId);
      res = TResources.createResourceVector(0, 1);
    }
    resourcesPerRequest.remove(requestId);
    freeResourceInUse(res);
    handleTaskCompleted(requestId);
  }
  
  
  protected void makeTaskRunnable(TaskReservation taskReservation) {
    AUDIT_LOG.info(
        Logging.auditEventString("nodemonitor_task_runnable", taskReservation.requestId));
    try {
      runnableTaskQueue.put(taskReservation);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
  }
   void submitTaskReservations(TEnqueueTaskReservationsRequest request,
                               InetSocketAddress appBackendAddress) {
    AUDIT_LOG.info(Logging.auditEventString("nodemonitor_task_submitted", ipAddress.toString(),
        request.getRequestId(), request.getNumTasks()));
    resourcesPerRequest.put(request.getRequestId(), request.getEstimatedResources());
    for (int i = 0; i < request.getNumTasks(); ++i) {
      LOG.debug("Creating reservation " + i + " for request " + request.getRequestId());
      TaskReservation reservation = new TaskReservation(request, appBackendAddress);
      handleSubmitTaskReservation(reservation);
    }
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
    for (TaskReservation t: runnableTaskQueue) {
      reserved = TResources.add(reserved, t.estimatedResources);
    }
    return TResources.subtract(free, reserved);
  }
  
  // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING
  
  /**
   * Submit a task to the scheduler.
   */
  abstract void handleSubmitTaskReservation(TaskReservation taskReservation);
  
  /**
   * Signal that a given task has completed.
   */
  protected abstract void handleTaskCompleted(String requestId);
  
  /**
   * Returns the current resource usage. If the resource usage is equal to the
   * machines capacity, this will include the queue length for appId.
   */
  abstract TResourceUsage getResourceUsage(String appId);
}
