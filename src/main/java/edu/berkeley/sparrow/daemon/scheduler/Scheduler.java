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

package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.FrontendService.AsyncClient.frontendMessage_call;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.enqueueTaskReservations_call;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TPlacementPreference;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * This class implements the Sparrow scheduler functionality.
 */
public class Scheduler {
  private final static Logger LOG = Logger.getLogger(Scheduler.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(Scheduler.class);

  /** Used to uniquely identify requests arriving at this scheduler. */
  private AtomicInteger counter = new AtomicInteger(0);

  /** How many times the special case has been triggered. */
  private AtomicInteger specialCaseCounter = new AtomicInteger(0);

  private THostPort address;

  /** Socket addresses for each frontend. */
  HashMap<String, InetSocketAddress> frontendSockets =
      new HashMap<String, InetSocketAddress>();

  /**
   * Service that handles cancelling outstanding reservations for jobs that have already been
   * scheduled.  Only instantiated if {@code SparrowConf.CANCELLATION} is set to true.
   */
  private CancellationService cancellationService;
  private boolean useCancellation;

  /** Thrift client pool for communicating with node monitors */
  ThriftClientPool<InternalService.AsyncClient> nodeMonitorClientPool =
      new ThriftClientPool<InternalService.AsyncClient>(
          new ThriftClientPool.InternalServiceMakerFactory());

  /** Thrift client pool for communicating with front ends. */
  private ThriftClientPool<FrontendService.AsyncClient> frontendClientPool =
      new ThriftClientPool<FrontendService.AsyncClient>(
          new ThriftClientPool.FrontendServiceMakerFactory());

  /** Information about cluster workload due to other schedulers. */
  private SchedulerState state;

  /** Probe ratios to use if the probe ratio is not explicitly set in the request. */
  private double defaultProbeRatioUnconstrained;
  private double defaultProbeRatioConstrained;

  /**
   * For each request, the task placer that should be used to place the request's tasks. Indexed
   * by the request ID.
   */
  private ConcurrentMap<String, TaskPlacer> requestTaskPlacers;

  /**
   * When a job includes SPREAD_EVENLY in the description and has this number of tasks,
   * Sparrow spreads the tasks evenly over machines to evenly cache data. We need this (in
   * addition to the SPREAD_EVENLY descriptor) because only the reduce phase -- not the map
   * phase -- should be spread.
   */
  private int spreadEvenlyTaskSetSize;

  private Configuration conf;

  public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {
    address = Network.socketAddressToThrift(socket);
    String mode = conf.getString(SparrowConf.DEPLYOMENT_MODE, "unspecified");
    this.conf = conf;
    if (mode.equals("standalone")) {
      state = new StandaloneSchedulerState();
    } else if (mode.equals("configbased")) {
      state = new ConfigSchedulerState();
    } else {
      throw new RuntimeException("Unsupported deployment mode: " + mode);
    }

    state.initialize(conf);

    defaultProbeRatioUnconstrained = conf.getDouble(SparrowConf.SAMPLE_RATIO,
        SparrowConf.DEFAULT_SAMPLE_RATIO);
    defaultProbeRatioConstrained = conf.getDouble(SparrowConf.SAMPLE_RATIO_CONSTRAINED,
        SparrowConf.DEFAULT_SAMPLE_RATIO_CONSTRAINED);

    requestTaskPlacers = Maps.newConcurrentMap();

    useCancellation = conf.getBoolean(SparrowConf.CANCELLATION, SparrowConf.DEFAULT_CANCELLATION);
    if (useCancellation) {
      LOG.debug("Initializing cancellation service");
      cancellationService = new CancellationService(nodeMonitorClientPool);
      new Thread(cancellationService).start();
    } else {
      LOG.debug("Not using cancellation");
    }

    spreadEvenlyTaskSetSize = conf.getInt(SparrowConf.SPREAD_EVENLY_TASK_SET_SIZE,
    				SparrowConf.DEFAULT_SPREAD_EVENLY_TASK_SET_SIZE);
  }

  public boolean registerFrontend(String appId, String addr) {
    LOG.debug(Logging.functionCall(appId, addr));
    Optional<InetSocketAddress> socketAddress = Serialization.strToSocket(addr);
    if (!socketAddress.isPresent()) {
      LOG.error("Bad address from frontend: " + addr);
      return false;
    }
    frontendSockets.put(appId, socketAddress.get());
    return state.watchApplication(appId);
  }

  /**
   * Callback for enqueueTaskReservations() that returns the client to the client pool.
   */
  private class EnqueueTaskReservationsCallback
  implements AsyncMethodCallback<enqueueTaskReservations_call> {
    String requestId;
    InetSocketAddress nodeMonitorAddress;
    long startTimeMillis;

    public EnqueueTaskReservationsCallback(String requestId, InetSocketAddress nodeMonitorAddress) {
      this.requestId = requestId;
      this.nodeMonitorAddress = nodeMonitorAddress;
      this.startTimeMillis = System.currentTimeMillis();
    }

    public void onComplete(enqueueTaskReservations_call response) {
      AUDIT_LOG.debug(Logging.auditEventString(
          "scheduler_complete_enqueue_task", requestId,
          nodeMonitorAddress.getAddress().getHostAddress()));
      long totalTime = System.currentTimeMillis() - startTimeMillis;
      LOG.debug("Enqueue Task RPC to " + nodeMonitorAddress.getAddress().getHostAddress() +
                " for request " + requestId + " completed in " + totalTime + "ms");
      try {
        nodeMonitorClientPool.returnClient(nodeMonitorAddress, (AsyncClient) response.getClient());
      } catch (Exception e) {
        LOG.error("Error returning client to node monitor client pool: " + e);
      }
      return;
    }

    public void onError(Exception exception) {
      // Do not return error client to pool
      LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
    }
  }

  /** Adds constraints such that tasks in the job will be spread evenly across the cluster.
   *
   *  We expect three of these special jobs to be submitted; 3 sequential calls to this
   *  method will result in spreading the tasks for the 3 jobs across the cluster such that no
   *  more than 1 task is assigned to each machine.
   */
  private TSchedulingRequest addConstraintsToSpreadTasks(TSchedulingRequest req)
  				throws TException {
    LOG.info("Handling spread tasks request: " + req);
    int specialCaseIndex = specialCaseCounter.incrementAndGet();
    if (specialCaseIndex < 1 || specialCaseIndex > 3) {
      LOG.error("Invalid special case index: " + specialCaseIndex);
    }

    // No tasks have preferences and we have the magic number of tasks
    TSchedulingRequest newReq = new TSchedulingRequest();
    newReq.user = req.user;
    newReq.app = req.app;
    newReq.probeRatio = req.probeRatio;

    List<InetSocketAddress> allBackends = Lists.newArrayList();
    List<InetSocketAddress> backends = Lists.newArrayList();
    // We assume the below always returns the same order (invalid assumption?)
    for (InetSocketAddress backend : state.getBackends(req.app)) {
      allBackends.add(backend);
    }

    // Each time this is called, we restrict to 1/3 of the nodes in the cluster
    for (int i = 0; i < allBackends.size(); i++) {
      if (i % 3 == specialCaseIndex - 1) {
        backends.add(allBackends.get(i));
      }
    }
    Collections.shuffle(backends);

    if (!(allBackends.size() >= (req.getTasks().size() * 3))) {
      LOG.error("Special case expects at least three times as many machines as tasks.");
      return null;
    }
    LOG.info(backends);
    for (int i = 0; i < req.getTasksSize(); i++) {
      TTaskSpec task = req.getTasks().get(i);
      TTaskSpec newTask = new TTaskSpec();
      newTask.message = task.message;
      newTask.taskId = task.taskId;
      newTask.preference = new TPlacementPreference();
      newTask.preference.addToNodes(backends.get(i).getHostName());
      newReq.addToTasks(newTask);
    }
    LOG.info("New request: " + newReq);
    return newReq;
  }

  /** Checks whether we should add constraints to this job to evenly spread tasks over machines.
   *
   * This is a hack used to force Spark to cache data in 3 locations: we run 3 select * queries
   * on the same table and spread the tasks for those queries evenly across the cluster such that
   * the input data for the query is triple replicated and spread evenly across the cluster.
   *
   * We signal that Sparrow should use this hack by adding SPREAD_TASKS to the job's description.
   */
  private boolean isSpreadTasksJob(TSchedulingRequest request) {
  	if ((request.getDescription() != null) &&
  			(request.getDescription().indexOf("SPREAD_EVENLY") != -1)) {
  		// Need to check to see if there are 3 constraints; if so, it's the map phase of the
  		// first job that reads the data from HDFS, so we shouldn't override the constraints.
  		for (TTaskSpec t: request.getTasks()) {
        if (t.getPreference() != null && (t.getPreference().getNodes() != null)  &&
            (t.getPreference().getNodes().size() == 3)) {
        	LOG.debug("Not special case: one of request's tasks had 3 preferences");
          return false;
        }
      }
      if (request.getTasks().size() != spreadEvenlyTaskSetSize) {
      	LOG.debug("Not special case: job had " + request.getTasks().size() +
            " tasks rather than the expected " + spreadEvenlyTaskSetSize);
        return false;
      }
    	if (specialCaseCounter.get() >= 3) {
    		LOG.error("Not using special case because special case code has already been " +
    	      " called 3 more more times!");
    		return false;
    	}
      LOG.debug("Spreading tasks for job with " + request.getTasks().size() + " tasks");
  		return true;
  	}
  	LOG.debug("Not special case: description did not contain SPREAD_EVENLY");
  	return false;
  }

  public void submitJob(TSchedulingRequest request) throws TException {
    // Short-circuit case that is used for liveness checking
    if (request.tasks.size() == 0) { return; }
    if (isSpreadTasksJob(request)) {
      handleJobSubmission(addConstraintsToSpreadTasks(request));
    } else {
      handleJobSubmission(request);
    }
  }

  public void handleJobSubmission(TSchedulingRequest request) throws TException {
    LOG.debug(Logging.functionCall(request));

    long start = System.currentTimeMillis();

    String requestId = getRequestId();

    String user = "";
    if (request.getUser() != null && request.getUser().getUser() != null) {
      user = request.getUser().getUser();
    }
    String description = "";
    if (request.getDescription() != null) {
    	description = request.getDescription();
    }

    String app = request.getApp();
    List<TTaskSpec> tasks = request.getTasks();
    Set<InetSocketAddress> backends = state.getBackends(app);
    LOG.debug("NumBackends: " + backends.size());
    boolean constrained = false;
    for (TTaskSpec task : tasks) {
      constrained = constrained || (
          task.preference != null &&
          task.preference.nodes != null &&
          !task.preference.nodes.isEmpty());
    }
    // Logging the address here is somewhat redundant, since all of the
    // messages in this particular log file come from the same address.
    // However, it simplifies the process of aggregating the logs, and will
    // also be useful when we support multiple daemons running on a single
    // machine.
    AUDIT_LOG.info(Logging.auditEventString("arrived", requestId,
                                            request.getTasks().size(),
                                            address.getHost(), address.getPort(),
                                            user, description, constrained));

    TaskPlacer taskPlacer;
    if (constrained) {
      if (request.isSetProbeRatio()) {
        taskPlacer = new ConstrainedTaskPlacer(requestId, request.getProbeRatio());
      } else {
        taskPlacer = new ConstrainedTaskPlacer(requestId, defaultProbeRatioConstrained);
      }
    } else {
      if (request.isSetProbeRatio()) {
        taskPlacer = new UnconstrainedTaskPlacer(requestId, request.getProbeRatio());
      } else {
        taskPlacer = new UnconstrainedTaskPlacer(requestId, defaultProbeRatioUnconstrained);
      }
    }
    requestTaskPlacers.put(requestId, taskPlacer);

    Map<InetSocketAddress, TEnqueueTaskReservationsRequest> enqueueTaskReservationsRequests;
    enqueueTaskReservationsRequests = taskPlacer.getEnqueueTaskReservationsRequests(
        request, requestId, backends, address);

    // Request to enqueue a task at each of the selected nodes.
    for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry :
      enqueueTaskReservationsRequests.entrySet())  {
      try {
        InternalService.AsyncClient client = nodeMonitorClientPool.borrowClient(entry.getKey());
        LOG.debug("Launching enqueueTask for request " + requestId + "on node: " + entry.getKey());
        AUDIT_LOG.debug(Logging.auditEventString(
            "scheduler_launch_enqueue_task", entry.getValue().requestId,
            entry.getKey().getAddress().getHostAddress()));
        client.enqueueTaskReservations(
            entry.getValue(), new EnqueueTaskReservationsCallback(requestId, entry.getKey()));
      } catch (Exception e) {
        LOG.error("Error enqueuing task on node " + entry.getKey().toString() + ":" + e);
      }
    }

    long end = System.currentTimeMillis();
    LOG.debug("All tasks enqueued for request " + requestId + "; returning. Total time: " +
              (end - start) + " milliseconds");
  }

  public List<TTaskLaunchSpec> getTask(
      String requestId, THostPort nodeMonitorAddress) {
    /* TODO: Consider making this synchronized to avoid the need for synchronization in
     * the task placers (although then we'd lose the ability to parallelize over task placers). */
    LOG.debug(Logging.functionCall(requestId, nodeMonitorAddress));
    TaskPlacer taskPlacer = requestTaskPlacers.get(requestId);
    if (taskPlacer == null) {
      LOG.debug("Received getTask() request for request " + requestId + ", which had no more " +
          "unplaced tasks");
      return Lists.newArrayList();
    }

    synchronized(taskPlacer) {
      List<TTaskLaunchSpec> taskLaunchSpecs = taskPlacer.assignTask(nodeMonitorAddress);
      if (taskLaunchSpecs == null || taskLaunchSpecs.size() > 1) {
        LOG.error("Received invalid task placement for request " + requestId + ": " +
                  taskLaunchSpecs.toString());
        return Lists.newArrayList();
      } else if (taskLaunchSpecs.size() == 1) {
        AUDIT_LOG.info(Logging.auditEventString("scheduler_assigned_task", requestId,
            taskLaunchSpecs.get(0).taskId,
            nodeMonitorAddress.getHost()));
      } else {
        AUDIT_LOG.info(Logging.auditEventString("scheduler_get_task_no_task", requestId,
                                                nodeMonitorAddress.getHost()));
      }

      if (taskPlacer.allTasksPlaced()) {
        LOG.debug("All tasks placed for request " + requestId);
        requestTaskPlacers.remove(requestId);
        if (useCancellation) {
          Set<THostPort> outstandingNodeMonitors =
              taskPlacer.getOutstandingNodeMonitorsForCancellation();
          for (THostPort nodeMonitorToCancel : outstandingNodeMonitors) {
            cancellationService.addCancellation(requestId, nodeMonitorToCancel);
          }
        }
      }
      return taskLaunchSpecs;
    }
  }

  /**
   * Returns an ID that identifies a request uniquely (across all Sparrow schedulers).
   *
   * This should only be called once for each request (it will return a different
   * identifier if called a second time).
   *
   * TODO: Include the port number, so this works when there are multiple schedulers
   * running on a single machine.
   */
  private String getRequestId() {
    /* The request id is a string that includes the IP address of this scheduler followed
     * by the counter.  We use a counter rather than a hash of the request because there
     * may be multiple requests to run an identical job. */
    return String.format("%s_%d", Network.getIPAddress(conf), counter.getAndIncrement());
  }

  private class sendFrontendMessageCallback implements
  AsyncMethodCallback<frontendMessage_call> {
    private InetSocketAddress frontendSocket;
    private FrontendService.AsyncClient client;
    public sendFrontendMessageCallback(InetSocketAddress socket, FrontendService.AsyncClient client) {
      frontendSocket = socket;
      this.client = client;
    }

    public void onComplete(frontendMessage_call response) {
      try { frontendClientPool.returnClient(frontendSocket, client); }
      catch (Exception e) { LOG.error(e); }
    }

    public void onError(Exception exception) {
      // Do not return error client to pool
      LOG.error("Error sending frontend message callback: " + exception);
    }
  }

  public void sendFrontendMessage(String app, TFullTaskId taskId,
      int status, ByteBuffer message) {
    LOG.debug(Logging.functionCall(app, taskId, message));
    InetSocketAddress frontend = frontendSockets.get(app);
    if (frontend == null) {
      LOG.error("Requested message sent to unregistered app: " + app);
    }
    try {
      FrontendService.AsyncClient client = frontendClientPool.borrowClient(frontend);
      client.frontendMessage(taskId, status, message,
          new sendFrontendMessageCallback(frontend, client));
    } catch (IOException e) {
      LOG.error("Error launching message on frontend: " + app, e);
    } catch (TException e) {
      LOG.error("Error launching message on frontend: " + app, e);
    } catch (Exception e) {
      LOG.error("Error launching message on frontend: " + app, e);
    }
  }
}
