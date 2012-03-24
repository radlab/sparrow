package edu.berkeley.sparrow.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.thrift.TException;

import com.google.protobuf.ByteString;

import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * A Sparrow implementation of a Mesos Executor driver. NOTE: This is experimental.
 * 
 * The interface between Mesos slave daemons and application backends (termed Executors) 
 * is via an ExecutorDriver. The driver is capable of launching tasks on an executor 
 * process and sending messages from that process to frontends. The general API is mostly
 * compatible with Sparrow's notion of an application backend.  
 * 
 * This is a version of an ExecutorDriver which is also a Sparrow backend. Existing
 * applications can use this ExecutorDriver as a drop-in substitue for a Mesos 
 * ExecutorDriver, and launch tasks via Sparrow rather than Mesos.
 */
public class SparrowExecutorDriver implements ExecutorDriver, BackendService.Iface {
  private Executor executor;
  private NodeMonitorService.Client client;
  private HashMap<String, String> taskIdToRequestId = 
      new HashMap<String, String>();
  private List<String> activeTaskIds = new ArrayList<String>();
  
  private boolean isRunning = false;
  private Status stopStatus = Status.OK;
  private Lock runLock = new ReentrantLock();
  final Condition stopped  = runLock.newCondition(); 
  
  public SparrowExecutorDriver(Executor executor) {
    this.executor = executor;
  }
  
  @Override
  public Status join() {
    if (!isRunning) { return Status.DRIVER_NOT_RUNNING; }
    runLock.lock();
    try {
      stopped.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    runLock.unlock();

    return stopStatus;
  }

  @Override
  public Status run() {
    start();
    return join();
  }

  @Override
  public Status sendFrameworkMessage(byte[] arg0) {
    System.err.println("Arbitrary message passing not yet supported in Sparrow.");
    return abort();
  }

  @Override
  public synchronized Status sendStatusUpdate(TaskStatus status) {
    if (status.getState() == TaskState.TASK_FINISHED) {
      activeTaskIds.remove(status.getTaskId().getValue());
      // TODO deal with removing task ID's
      try {
        client.updateResourceUsage("spark", 
            new HashMap<TUserGroupInfo, TResourceVector>(), activeTaskIds);
      } catch (TException e) {
        e.printStackTrace();
      }
    }
    String requestId = taskIdToRequestId.get(status.getTaskId().getValue());

    try {
      client.sendFrontendMessage("spark", requestId, 
          ByteBuffer.wrap(status.toByteArray()));
    } catch (TException e) {
      e.printStackTrace();
      return abort();
    }
    return Status.OK;
  }

  @Override
  public Status start() {
    if (isRunning) { 
      return Status.DRIVER_ALREADY_RUNNING; 
    }
    try {
      client = TClients.createBlockingNmClient("localhost", 20501);
    } catch (IOException e) {
      return abort();
    }
    try {
      client.registerBackend("spark", "localhost:4310");
    } catch (TException e) {
      return abort();
    }
    BackendService.Processor<BackendService.Iface> processor =
        new BackendService.Processor<BackendService.Iface>(this);
    try {
      TServers.launchThreadedThriftServer(4310, 4, processor);
    } catch (IOException e) {
      return Status.DRIVER_NOT_RUNNING;
    }
    executor.init(this, null);
    isRunning = true;
    return Status.OK;
  }

  @Override
  public Status stop() {
    client.getOutputProtocol().getTransport().close(); // TODO: close server
    synchronized (runLock) {
      stopStatus = Status.DRIVER_STOPPED;
      stopped.signalAll();
    }
    return stopStatus;
  }

  @Override
  public Status stop(boolean arg0) {
    client.getOutputProtocol().getTransport().close(); // TODO: close server
    synchronized (runLock) {
      stopStatus = Status.DRIVER_STOPPED;
      stopped.signalAll();
    }
    return stopStatus;
  }

  @Override
  public Status abort() {
    client.getOutputProtocol().getTransport().close(); // TODO: close server
    synchronized (runLock) {
      stopStatus = Status.DRIVER_ABORTED;
      stopped.signalAll();
    }
    return stopStatus;
  }

  @Override
  public void updateResourceLimits(
      Map<TUserGroupInfo, TResourceVector> resources) throws TException {
    // Ignored, we don't care about resource limits from Sparrow
  }

  @Override
  public void launchTask(ByteBuffer message, String requestId, String taskId,
      TUserGroupInfo user, TResourceVector estimatedResources)
      throws TException {
    activeTaskIds.add(taskId);
    taskIdToRequestId.put(taskId, requestId);
    TaskID id = TaskID.newBuilder().setValue(taskId).build();
    SlaveID sId = SlaveID.newBuilder().setValue("slave").build();
    TaskDescription task = TaskDescription.newBuilder()
        .setTaskId(id)
        .setData(ByteString.copyFrom(message))
        .setSlaveId(sId)
        .setName("task")
        .build();
    executor.launchTask(this, task);
  }
}
