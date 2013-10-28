/*
 * Copyright 2013 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.nodemonitor.TaskScheduler.TaskSpec;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.daemon.util.Resources;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.GetTaskService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;

/**
 * TaskLauncher service consumes TaskReservations produced by {@link TaskScheduler.getNextTask}.
 * For each TaskReservation, the TaskLauncherService attempts to fetch the task specification from
 * the scheduler that sent the reservation using the {@code getTask} RPC; if it successfully
 * fetches a task, it launches the task on the appropriate backend.
 *
 * TaskLauncherService uses multiple threads to launch tasks. Each thread keeps a client for
 * each scheduler, and a client for each application backend, and the TaskLauncherService uses
 * a number of threads equal to the number of slots available for running tasks on the machine.
 */
public class TaskLauncherService {
  private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskLauncherService.class);

  /* The number of threads used by the service. */
  private int numThreads;

  private THostPort nodeMonitorInternalAddress;

  private TaskScheduler scheduler;

  /** A runnable that spins in a loop asking for tasks to launch and launching them. */
  private class TaskLaunchRunnable implements Runnable {

    /** Client to use to communicate with each scheduler (indexed by scheduler hostname). */
    private HashMap<String, GetTaskService.Client> schedulerClients = Maps.newHashMap();

    /** Client to use to communicate with each application backend (indexed by backend address). */
    private HashMap<InetSocketAddress, BackendService.Client> backendClients = Maps.newHashMap();

    @Override
    public void run() {
      while (true) {
        TaskSpec task = scheduler.getNextTask(); // blocks until task is ready

        List<TTaskLaunchSpec> taskLaunchSpecs = executeGetTaskRpc(task);
        AUDIT_LOG.info(Logging.auditEventString("node_monitor_get_task_complete", task.requestId,
            nodeMonitorInternalAddress.getHost()));

        if (taskLaunchSpecs.isEmpty()) {
          LOG.debug("Didn't receive a task for request " + task.requestId);
          scheduler.noTaskForReservation(task);
          continue;
        }
        if (taskLaunchSpecs.size() > 1) {
          LOG.warn("Received " + taskLaunchSpecs +
                   " task launch specifications; ignoring all but the first one.");
        }
        task.taskSpec = taskLaunchSpecs.get(0);
        LOG.debug("Received task for request " + task.requestId + ", task " +
                  task.taskSpec.getTaskId());

        // Launch the task on the backend.
        AUDIT_LOG.info(Logging.auditEventString("node_monitor_task_launch",
            task.requestId,
            nodeMonitorInternalAddress.getHost(),
            task.taskSpec.getTaskId(),
            task.previousRequestId,
            task.previousTaskId));
        executeLaunchTaskRpc(task);
        LOG.debug("Launched task " + task.taskSpec.getTaskId() + " for request " + task.requestId +
            " on application backend at system time " + System.currentTimeMillis());
      }

    }

    /** Uses a getTask() RPC to get the task specification from the appropriate scheduler. */
    private List<TTaskLaunchSpec> executeGetTaskRpc(TaskSpec task) {
      String schedulerAddress = task.schedulerAddress.getAddress().getHostAddress();
      if (!schedulerClients.containsKey(schedulerAddress)) {
        try {
          schedulerClients.put(schedulerAddress,
              TClients.createBlockingGetTaskClient(
                  task.schedulerAddress.getAddress().getHostAddress(),
                  SchedulerThrift.DEFAULT_GET_TASK_PORT));
        } catch (IOException e) {
          LOG.error("Error creating thrift client: " + e.getMessage());
          List<TTaskLaunchSpec> emptyTaskLaunchSpecs = Lists.newArrayList();
          return emptyTaskLaunchSpecs;
        }
      }
      GetTaskService.Client getTaskClient = schedulerClients.get(schedulerAddress);

      long startTimeMillis = System.currentTimeMillis();
      long startGCCount = Logging.getGCCount();

      LOG.debug("Attempting to get task for request " + task.requestId);
      AUDIT_LOG.debug(Logging.auditEventString("node_monitor_get_task_launch", task.requestId,
          nodeMonitorInternalAddress.getHost()));
      List<TTaskLaunchSpec> taskLaunchSpecs;
      try {
        taskLaunchSpecs = getTaskClient.getTask(task.requestId, nodeMonitorInternalAddress);
      } catch (TException e) {
        LOG.error("Error when launching getTask RPC:" + e.getMessage());
        List<TTaskLaunchSpec> emptyTaskLaunchSpecs = Lists.newArrayList();
        return emptyTaskLaunchSpecs;
      }

      long rpcTime = System.currentTimeMillis() - startTimeMillis;
      long numGarbageCollections = Logging.getGCCount() - startGCCount;
      LOG.debug("GetTask() RPC for request " + task.requestId + " completed in " +  rpcTime +
                "ms (" + numGarbageCollections + "GCs occured during RPC)");
      return taskLaunchSpecs;
    }

    /** Executes an RPC to launch a task on an application backend. */
    private void executeLaunchTaskRpc(TaskSpec task) {
      if (!backendClients.containsKey(task.appBackendAddress)) {
        try {
          backendClients.put(task.appBackendAddress,
              TClients.createBlockingBackendClient(task.appBackendAddress));
        } catch (IOException e) {
          LOG.error("Error creating thrift client: " + e.getMessage());
          return;
        }
      }
      BackendService.Client backendClient = backendClients.get(task.appBackendAddress);
      THostPort schedulerHostPort = Network.socketAddressToThrift(task.schedulerAddress);
      TFullTaskId taskId = new TFullTaskId(task.taskSpec.getTaskId(), task.requestId,
          task.appId, schedulerHostPort);
      try {
        backendClient.launchTask(task.taskSpec.bufferForMessage(), taskId, task.user);
      } catch (TException e) {
        LOG.error("Unable to launch task on backend " + task.appBackendAddress + ":" + e);
      }
    }
  }

  public void initialize(Configuration conf, TaskScheduler scheduler,
      int nodeMonitorPort) {
    numThreads = scheduler.getMaxActiveTasks();
    if (numThreads <= 0) {
      // If the scheduler does not enforce a maximum number of tasks, just use a number of
      // threads equal to the number of cores.
      numThreads = Resources.getSystemCPUCount(conf);
    }
    this.scheduler = scheduler;
    nodeMonitorInternalAddress = new THostPort(Network.getIPAddress(conf), nodeMonitorPort);
    ExecutorService service = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      service.submit(new TaskLaunchRunnable());
    }
  }
}
