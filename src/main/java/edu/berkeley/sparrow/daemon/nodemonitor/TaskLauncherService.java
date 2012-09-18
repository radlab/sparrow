package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.nodemonitor.TaskScheduler.TaskSpec;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.THostPort;

/**
 * TaskLauncher service consumes TaskReservations produced by {@link TaskScheduler.getNextTask}.
 * For each TaskReservation, the TaskLauncherService attempts to fetch the task specification from
 * the scheduler that send the reservation using the {@code getTask} RPC; if it successfully
 * fetches a task, it launches the task on the appropriate backend.
 */
public class TaskLauncherService {
  private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskLauncherService.class);

  /* The number of threads we use to launch tasks on backends. We also use this
   * to determine how many thrift connections to keep open to each backend, so that
   * in the limit case where all threads are talking to the same backend, we don't run
   * out of connections.*/
  public final static int CLIENT_POOL_SIZE = 10;

  private THostPort nodeMonitorInternalAddress;

  private TaskScheduler scheduler;

  /** Cache of thrift clients pools for each backends. Clients are removed from the pool
   *  when in use. */
  private HashMap<InetSocketAddress, BlockingQueue<BackendService.Client>> backendClients =
      new HashMap<InetSocketAddress, BlockingQueue<BackendService.Client>>();

  /** A runnable which spins in a loop asking for tasks to launch and launching them. */
  private class TaskLaunchRunnable implements Runnable {
    @Override
    public void run() {
      while (true) {
        TaskSpec task = scheduler.getNextTask(); // blocks until task is ready
        LOG.debug("Tring to launch task for request " + task.requestId);

        AUDIT_LOG.info(Logging.auditEventString("node_monitor_task_launch",
            task.requestId,
            nodeMonitorInternalAddress.getHost(),
            task.taskSpec.getTaskId(),
            task.previousRequestId,
            task.previousTaskId));

        // Launch the task on the backend.
        BackendService.Client client = null;
        if (!backendClients.containsKey(task.appBackendAddress)) {
          createThriftClients(task.appBackendAddress);
        }

        try {
          // Blocks until a client becomes available.
          client = backendClients.get(task.appBackendAddress).take();
        } catch (InterruptedException e) {
          LOG.fatal("Error when trying to get a client for " + task.appId
              + "backend at " + task.appBackendAddress.toString() + ":" +
              e);
        }

        THostPort schedulerHostPort = Network.socketAddressToThrift(
            task.schedulerAddress);
        TFullTaskId taskId = new TFullTaskId(task.taskSpec.getTaskId(), task.requestId,
            task.appId, schedulerHostPort);
        try {
          client.launchTask(task.taskSpec.bufferForMessage(), taskId, task.user,
              task.estimatedResources);
        } catch (TException e) {
          LOG.fatal("Unable to launch task on backend " + task.appBackendAddress + ":" +
              e);
        }

        try {
          backendClients.get(task.appBackendAddress).put(client);
        } catch (InterruptedException e) {
          LOG.fatal("Error while attempting to return client for " +
              task.appBackendAddress.toString() +
              " to the set of backend clients: " + e);
        }

        LOG.debug("Launched task " + taskId.taskId + " for request " + task.requestId +
            " on application backend at system time " + System.currentTimeMillis());
      }

    }
  }

  public void initialize(Configuration conf, TaskScheduler scheduler,
      int nodeMonitorPort) {
    this.scheduler = scheduler;
    nodeMonitorInternalAddress = new THostPort(Network.getHostName(conf), nodeMonitorPort);
    ExecutorService service = Executors.newFixedThreadPool(CLIENT_POOL_SIZE);
    for (int i = 0; i < CLIENT_POOL_SIZE; i++) {
      service.submit(new TaskLaunchRunnable());
    }
  }

  /** Creates a set of thrift clients and adds them to the client pool. */
  public void createThriftClients(InetSocketAddress backendAddr) {
    BlockingQueue<BackendService.Client> clients = new
        LinkedBlockingDeque<BackendService.Client>();
    for (int i = 0; i < CLIENT_POOL_SIZE; i++) {
      try {
        clients.put(TClients.createBlockingBackendClient(
            backendAddr.getAddress().getHostAddress(), backendAddr.getPort()));
      } catch (InterruptedException e) {
        LOG.error("Interrupted creating thrift clients", e);
      } catch (IOException e) {
        LOG.error("Error creating thrift client", e);
      }
    }

    backendClients.put(backendAddr, clients);
  }


}