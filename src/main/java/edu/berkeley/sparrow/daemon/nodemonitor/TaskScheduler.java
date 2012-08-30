package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Network;
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

  protected class ResourceInfo {
    /** Unlaunched tasks for which this resource information applies. */
    public int remainingTasks;

    /** Estimated resources. */
    public TResourceVector resources;

    public ResourceInfo(int tasks, TResourceVector resources) {
     remainingTasks = tasks;
     this.resources = resources;
    }
  }

  private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
  private String ipAddress;

  protected TResourceVector capacity;
  protected Configuration conf;
  protected TResourceVector inUse = TResources.clone(TResources.none());
  private final BlockingQueue<TaskReservation> runnableTaskQueue =
      new LinkedBlockingQueue<TaskReservation>();
  private HashMap<String, ResourceInfo> resourcesPerRequest = Maps.newHashMap();

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
      AUDIT_LOG.info(Logging.auditEventString("task_completed", t.getRequestId(), t.getTaskId()));
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
    LOG.debug(Logging.functionCall(requestId));
    ResourceInfo resourceInfo = resourcesPerRequest.get(requestId);
    if (resourceInfo == null) {
      LOG.error("Missing resources for request: " + requestId);
      resourceInfo = new ResourceInfo(1, TResources.createResourceVector(0, 1));
    }
    resourceInfo.remainingTasks--;
    if (resourceInfo.remainingTasks == 0) {
      resourcesPerRequest.remove(requestId);
    }
    freeResourceInUse(resourceInfo.resources);
    handleTaskCompleted(requestId);
  }

  protected void makeTaskRunnable(TaskReservation taskReservation) {
    LOG.debug("Making task for request " + taskReservation.requestId + " runnable");
    try {
      runnableTaskQueue.put(taskReservation);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
  }

  void submitTaskReservations(TEnqueueTaskReservationsRequest request,
                              InetSocketAddress appBackendAddress) {
    ResourceInfo resourceInfo = new ResourceInfo(request.getNumTasks(),
                                                 request.getEstimatedResources());
    resourcesPerRequest.put(request.getRequestId(), resourceInfo);
    for (int i = 0; i < request.getNumTasks(); ++i) {
      LOG.debug("Creating reservation " + i + " for request " + request.getRequestId());
      TaskReservation reservation = new TaskReservation(request, appBackendAddress);
      int queuedReservations = handleSubmitTaskReservation(reservation);
      AUDIT_LOG.info(Logging.auditEventString("reservation_enqueued", ipAddress, request.requestId,
                                              queuedReservations));
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
   * Handles a task reservation. Returns the number of queued reservations.
   */
  abstract int handleSubmitTaskReservation(TaskReservation taskReservation);

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
