package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;
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
  
  class TaskDescription {
    ByteBuffer message;
    public String requestId;
    public String taskId;
    public TResourceVector estimatedResources;
    public TUserGroupInfo user;
    public InetSocketAddress backendSocket;
    
    public TaskDescription(String taskId, String requestId, ByteBuffer message, 
        TResourceVector estimatedResources, TUserGroupInfo user, InetSocketAddress addr) {
      this.taskId = taskId;
      this.requestId = requestId;
      this.message = message;
      this.estimatedResources = estimatedResources;
      this.user = user;
      this.backendSocket = addr;
    }
  }
  
  protected TResourceVector capacity;
  protected TResourceVector inUse = TResources.none();
  protected final BlockingQueue<TaskDescription> runnableTaskQueue = 
      new LinkedBlockingQueue<TaskDescription>();
  protected final static TResourceVector ONE_CORE = TResources.createResourceVector(0, 1);
  private HashMap<String, TResourceVector> resourcesPerTask = new 
      HashMap<String, TResourceVector>();
  
  /** Initialize the task scheduler, passing it the current available resources 
   *  on the machine. */
  void initialize(TResourceVector capacity) {
    this.capacity = capacity;
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
    addResourceInUse(task.estimatedResources);
    return task;
  }  
  
  /**
   * Returns the current number of runnable tasks (for testing).
   */
  int runnableTasks() {
    return runnableTaskQueue.size();
  }

  void taskCompleted(String taskId) {
    freeResourceInUse(ONE_CORE);
    handleTaskCompleted(taskId);
  }
  
  void submitTask(TaskDescription task) {
    resourcesPerTask.put(task.taskId, task.estimatedResources);
    handleSubmitTask(task);
  }
  
  protected synchronized void addResourceInUse(TResourceVector nowInUse) {
    inUse = TResources.add(inUse, nowInUse);
  }  
  protected synchronized void freeResourceInUse(TResourceVector nowFreed) {
    TResources.subtractFrom(inUse, nowFreed);
  }
  protected synchronized TResourceVector getUnAssignedResources() {
    TResourceVector free = TResources.subtract(capacity, inUse);
    TResourceVector reserved = TResources.none();
    for (TaskDescription t: runnableTaskQueue) {
      reserved = TResources.add(reserved, t.estimatedResources);
    }
    return TResources.subtract(free, reserved);
  }
  
  /**
   * Submit a task to the scheduler.
   */
  abstract void handleSubmitTask(TaskDescription task);
  
  /**
   * Signal that a given task has completed.
   */
  protected abstract void handleTaskCompleted(String taskId);
  
}
