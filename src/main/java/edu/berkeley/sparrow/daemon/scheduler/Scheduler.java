package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import edu.berkeley.sparrow.thrift.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.daemon.util.Hostname;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.FrontendService.AsyncClient;
import edu.berkeley.sparrow.thrift.FrontendService.AsyncClient.frontendMessage_call;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.launchTask_call;

/**
 * This class implements the Sparrow scheduler functionality.
 */
public class Scheduler {
  private final static Logger LOG = Logger.getLogger(Scheduler.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(Scheduler.class);

  /** Used to uniquely identify requests arriving at this scheduler. */
  private int counter = 0;
  private InetSocketAddress address;

  /** Socket addresses for each frontend. */
  HashMap<String, InetSocketAddress> frontendSockets =
      new HashMap<String, InetSocketAddress>();

  /** Thrift client pool for communicating with node monitors */
  ThriftClientPool<InternalService.AsyncClient> nodeMonitorClientPool =
      new ThriftClientPool<InternalService.AsyncClient>(
      new ThriftClientPool.InternalServiceMakerFactory());

  /** Thrift client pool for communicating with front ends. */
  private ThriftClientPool<FrontendService.AsyncClient> frontendClientPool =
      new ThriftClientPool<FrontendService.AsyncClient>(
          new ThriftClientPool.FrontendServiceMakerFactory());

  /** Information about cluster workload due to other schedulers. */
  SchedulerState state;

  /** Logical task assignment. */
  // TODO: NOTE - this is a hack - we need to modify constrainedPlacer to have more
  // advanced features like waiting for some probes and configurable probe ratio.
  TaskPlacer constrainedPlacer;
  TaskPlacer unconstrainedPlacer;

  /** How many times the special case has been triggered. */
  private AtomicInteger specialCaseCounter = new AtomicInteger(0);
  
  /**
   * When a job includes SPREAD_EVENLY in the description and has this number of tasks,
   * Sparrow spreads the tasks evenly over machines to evenly cache data. We need this (in
   * addition to the SPREAD_EVENLY descriptor) because only the reduce phase -- not the map
   * phase -- should be spread.
   */
  private int spreadEvenlyTaskSetSize;

  private Configuration conf;

  /**
   * A callback handler for asynchronous task launches.
   *
   * We use the thrift event-based interface for launching tasks. In parallel, we launch
   * several tasks, then we return when all have finished launching.
   */
  private class TaskLaunchCallback implements AsyncMethodCallback<launchTask_call> {
    private CountDownLatch latch;
    private InternalService.AsyncClient client;
    private InetSocketAddress socket;

    // Note that the {@code client} must come from the Scheduler's {@code clientPool}.
    public TaskLaunchCallback(CountDownLatch latch, InternalService.AsyncClient client,
        InetSocketAddress socket) {
      this.latch = latch;
      this.client = client;
      this.socket = socket;
    }

    public void onComplete(launchTask_call response) {
      try {
        nodeMonitorClientPool.returnClient(socket, client);
      } catch (Exception e) {
        LOG.error(e);
      }
      latch.countDown();
    }

    public void onError(Exception exception) {
      LOG.error("Error launching task: " + exception);
      // TODO We need to have a story here, regarding the failure model when the
      //      task launch doesn't succeed.
      latch.countDown();
    }
  }

  public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {
    address = socket;
    String mode = conf.getString(SparrowConf.DEPLYOMENT_MODE, "unspecified");

    this.conf = conf;
    if (mode.equals("configbased")) {
      state = new ConfigSchedulerState();
      constrainedPlacer = new ConstraintObservingProbingTaskPlacer();
      unconstrainedPlacer = new ProbingTaskPlacer();
    } else {
      throw new RuntimeException("Unsupported deployment mode: " + mode);
    }

    state.initialize(conf);
    constrainedPlacer.initialize(conf, nodeMonitorClientPool);
    unconstrainedPlacer.initialize(conf, nodeMonitorClientPool);
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

  public boolean submitJob(TSchedulingRequest request) throws TException {
    if (isSpreadTasksJob(request)) {
      return handleJobSubmission(addConstraintsToSpreadTasks(request));
    } else {
      return handleJobSubmission(request);
    }
  }

  public boolean handleJobSubmission(TSchedulingRequest req) throws TException {
    LOG.debug(Logging.functionCall(req));
    long start = System.currentTimeMillis();

    String requestId = getRequestId();
    // Logging the address here is somewhat redundant, since all of the
    // messages in this particular log file come from the same address.
    // However, it simplifies the process of aggregating the logs, and will
    // also be useful when we support multiple daemons running on a single
    // machine.
    String user = "";
    if (req.getUser() != null) {
    	user = req.getUser().getUser();
    }
    
    String description = "";
    if (req.getDescription() != null) {
    	description = req.getDescription();
    }
    AUDIT_LOG.info(Logging.auditEventString("arrived", requestId,
                                            req.getTasks().size(),
                                            address.getAddress().getHostAddress(),
                                            address.getPort(), user, description,
                                            isConstrained(req)));
    Collection<TaskPlacementResponse> placement = null;
    try {
      placement = getJobPlacementResp(req, requestId);
    } catch (IOException e) {
      LOG.error(e);
      return false;
    }
    long probeFinish = System.currentTimeMillis();

    // Launch tasks.
    CountDownLatch latch = new CountDownLatch(placement.size());
    for (TaskPlacementResponse response : placement) {
      LOG.debug("Attempting to launch task " + response.getTaskSpec().getTaskId()
          + " on " + response.getNodeAddr());

      InternalService.AsyncClient client;
      try {
        long t0 = System.currentTimeMillis();
        client = nodeMonitorClientPool.borrowClient(response.getNodeAddr());
        long t1 = System.currentTimeMillis();
        if (t1 - t0 > 100) {
          LOG.error("Took more than 100ms to create client for: " +
            response.getNodeAddr());
        }
      } catch (Exception e) {
        LOG.error(e);
        return false;
      }
      String taskId = response.getTaskSpec().taskId;

      AUDIT_LOG.info(Logging.auditEventString("scheduler_launch", requestId, taskId));
      TFullTaskId id = new TFullTaskId();
      id.appId = req.getApp();
      id.frontendSocket = address.getHostName() + ":" + address.getPort();
      id.requestId = requestId;
      id.taskId = taskId;
      client.launchTask(response.getTaskSpec().message, id,
          req.getUser(), response.getTaskSpec().getEstimatedResources(),
          new TaskLaunchCallback(latch, client, response.getNodeAddr()));
    }
    // NOTE: Currently we just return rather than waiting for all tasks to launch
    /*
    try {
      LOG.debug("Waiting for " + placement.size() + " tasks to finish launching");
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    */
    long end = System.currentTimeMillis();
    LOG.debug("All tasks launched, returning. Total time: " + (end - start) +
        "Probe time: " + (probeFinish - start));
    return true;
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
  				  (t.getPreference().getNodes().size() > 0)) {
  				return false;
  			}
  		}
  		if (request.getTasks().size() != spreadEvenlyTaskSetSize) {
  			return false;
  		}

      LOG.debug("Spreading tasks for job with (" + request.getTasks().size() + " tasks)");
      return true;
    }
    return false;
  }

  /** Handles special case. */
  private TSchedulingRequest addConstraintsToSpreadTasks(TSchedulingRequest req) throws TException {
    LOG.info("Handling special case request: " + req);
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
      newTask.estimatedResources = task.estimatedResources;
      newTask.message = task.message;
      newTask.taskId = task.taskId;
      newTask.preference = new TPlacementPreference();
      newTask.preference.addToNodes(backends.get(i).getHostName());
      newReq.addToTasks(newTask);
    }
    LOG.info("New request: " + newReq);
    return newReq;
  }

  public Collection<TTaskPlacement> getJobPlacement(TSchedulingRequest req)
      throws IOException {
    LOG.debug(Logging.functionCall(req));
    // Get placement
    Collection<TaskPlacementResponse> placements = getJobPlacementResp(req,
                                                                       getRequestId());

    // Massage into correct Thrift output type
    Collection<TTaskPlacement> out = new HashSet<TTaskPlacement>(placements.size());
    for (TaskPlacementResponse placement : placements) {
      TTaskPlacement tPlacement = new TTaskPlacement();
      tPlacement.node = placement.getNodeAddr().toString();
      tPlacement.taskID = placement.getTaskSpec().getTaskId();
      out.add(tPlacement);
    }
    LOG.debug("Returning task placement: " + out);
    return out;
  }
  
  private boolean isConstrained(TSchedulingRequest req) {
    boolean constrained = false;
    for (TTaskSpec task : req.getTasks()) {
      constrained = constrained || (
          task.preference != null &&
          task.preference.nodes != null &&
          !task.preference.nodes.isEmpty());
    }
    return constrained;
  }

  /**
   * Internal method called by both submitJob() and getJobPlacement().
   */
  private Collection<TaskPlacementResponse> getJobPlacementResp(TSchedulingRequest req,
      String requestId) throws IOException {
    LOG.debug(Logging.functionCall(req));
    String app = req.getApp();
    List<TTaskSpec> tasks = req.getTasks();
    Set<InetSocketAddress> backends = state.getBackends(app);
    List<InetSocketAddress> backendList = new ArrayList<InetSocketAddress>(backends.size());
    for (InetSocketAddress backend : backends) {
      backendList.add(backend);
    }
    boolean constrained = isConstrained(req);

    // Fill in the resources in all tasks (if it's missing).
    for (TTaskSpec task : tasks) {
      if (task.estimatedResources == null) {
        task.estimatedResources = new TResourceVector(0, 1);
      }
    }

    if (constrained) {
      return constrainedPlacer.placeTasks(app, requestId, backendList, tasks);
    } else {
      return unconstrainedPlacer.placeTasks(app, requestId, backendList, tasks);
    }
  }

  /**
   * Returns an ID that identifies a request uniquely (across all Sparrow schedulers).
   *
   * This should only be called once for each request (it will return a different
   * identifier if called a second time).
   *
   * TODO: Include the port number, so this works when there are multiple schedulers
   * running on a single machine (as there will be when we do large scale testing).
   */
  private String getRequestId() {
    /* The request id is a string that includes the IP address of this scheduler followed
     * by the counter.  We use a counter rather than a hash of the request because there
     * may be multiple requests to run an identical job. */
    return String.format("%s_%d", Hostname.getIPAddress(conf), counter++);
  }

  private class sendFrontendMessageCallback implements
      AsyncMethodCallback<frontendMessage_call> {
    private InetSocketAddress frontendSocket;
    private AsyncClient client;
    public sendFrontendMessageCallback(InetSocketAddress socket, AsyncClient client) {
      frontendSocket = socket;
      this.client = client;
    }

    public void onComplete(frontendMessage_call response) {
      try { frontendClientPool.returnClient(frontendSocket, client); }
      catch (Exception e) { LOG.error(e); }
    }

    public void onError(Exception exception) {
      // Do not return error client to pool
      LOG.error(exception);
    }
  }

  public void sendFrontendMessage(String app, TFullTaskId taskId,
      int status, ByteBuffer message) {
    LOG.debug(Logging.functionCall(app, taskId, message));
    InetSocketAddress frontend = frontendSockets.get(app);
    if (frontend == null) {
      LOG.error("Requested message sent to unregistered app: " + app);
      return;
    }
    try {
      AsyncClient client = frontendClientPool.borrowClient(frontend);
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
