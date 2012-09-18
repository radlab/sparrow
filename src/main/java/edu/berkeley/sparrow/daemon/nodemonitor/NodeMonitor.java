package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Resources;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.SchedulerService.AsyncClient;
import edu.berkeley.sparrow.thrift.SchedulerService.AsyncClient.sendFrontendMessage_call;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * A Node Monitor which is responsible for communicating with application
 * backends. This class is wrapped by multiple thrift servers, so it may
 * be concurrently accessed when handling multiple function calls
 * simultaneously.
 */
public class NodeMonitor {
  private final static Logger LOG = Logger.getLogger(NodeMonitor.class);

  private static NodeMonitorState state;
  private HashMap<String, InetSocketAddress> appSockets =
      new HashMap<String, InetSocketAddress>();
  private HashMap<String, List<TFullTaskId>> appTasks =
      new HashMap<String, List<TFullTaskId>>();
  // Map to scheduler socket address for each request id.
  private ConcurrentMap<String, InetSocketAddress> requestSchedulers =
      Maps.newConcurrentMap();
  private ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool =
      new ThriftClientPool<SchedulerService.AsyncClient>(
          new ThriftClientPool.SchedulerServiceMakerFactory());

  private TResourceVector capacity;
  private FifoTaskScheduler scheduler;
  private TaskLauncherService taskLauncherService;

  public void initialize(Configuration conf, int nodeMonitorInternalPort)
      throws UnknownHostException {
    String mode = conf.getString(SparrowConf.DEPLYOMENT_MODE, "unspecified");
    if (mode.equals("standalone")) {
      state = new StandaloneNodeMonitorState();
    } else if (mode.equals("configbased")) {
      state = new ConfigNodeMonitorState();
    } else if (mode.equals("production")) {
      state = new StateStoreNodeMonitorState();
    } else {
      throw new RuntimeException("Unsupported deployment mode: " + mode);
    }
    try {
      state.initialize(conf);
    } catch (IOException e) {
      LOG.fatal("Error initializing node monitor state.", e);
    }
    capacity = new TResourceVector();
    int mem = Resources.getSystemMemoryMb(conf);
    capacity.setMemory(mem);
    LOG.info("Using memory allocation: " + mem);

    int cores = Resources.getSystemCPUCount(conf);
    capacity.setCores(cores);
    LOG.info("Using core allocation: " + cores);

    scheduler = new FifoTaskScheduler();
    scheduler.setMaxActiveTasks(cores);
    scheduler.initialize(capacity, conf, nodeMonitorInternalPort);
    taskLauncherService = new TaskLauncherService();
    taskLauncherService.initialize(conf, scheduler, nodeMonitorInternalPort);
  }

  /**
   * Registers the backend with assumed 0 load, and returns true if successful.
   * Returns false if the backend was already registered.
   */
  public boolean registerBackend(String appId, InetSocketAddress nmAddr,
      InetSocketAddress backendAddr) {
    LOG.debug(Logging.functionCall(appId, nmAddr, backendAddr));
    if (appSockets.containsKey(appId)) {
      LOG.warn("Attempt to re-register app " + appId);
      return false;
    }
    appSockets.put(appId, backendAddr);
    appTasks.put(appId, new ArrayList<TFullTaskId>());
    return state.registerBackend(appId, nmAddr);
  }

  /**
   * Return a map of applications to current resource usage (aggregated across all users).
   * If appId is set to "*", this map includes all applications. If it is set to an
   * application name, the map only includes that application. If it is set to anything
   * else, an empty map is returned.
   */
  public Map<String, TResourceUsage> getLoad(String appId, String requestId) {
    LOG.debug(Logging.functionCall(appId));
    Map<String, TResourceUsage> out = new HashMap<String, TResourceUsage>();
    if (appId.equals("*")) {
      for (String app : appSockets.keySet()) {
        out.put(app, scheduler.getResourceUsage(app));
      }
    }
    LOG.debug("Returning " + out);
    return out;
  }

  /**
   * Account for tasks which have finished.
   */
  public void tasksFinished(List<TFullTaskId> tasks) {
    LOG.debug(Logging.functionCall(tasks));
    scheduler.tasksFinished(tasks);
  }

  public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) {
    LOG.debug(Logging.functionCall(request));

    InetSocketAddress schedulerAddress = new InetSocketAddress(
        request.getSchedulerAddress().getHost(), request.getSchedulerAddress().getPort());
    requestSchedulers.put(request.getRequestId(), schedulerAddress);

    InetSocketAddress socket = appSockets.get(request.getAppId());
    if (socket == null) {
      LOG.error("No socket stored for " + request.getAppId() + " (never registered?). " +
      		"Can't launch task.");
      return false;
    }
    scheduler.submitTaskReservations(request, socket);
    return true;
  }

  private class sendFrontendMessageCallback implements
  AsyncMethodCallback<sendFrontendMessage_call> {
    private InetSocketAddress frontendSocket;
    private AsyncClient client;
    public sendFrontendMessageCallback(InetSocketAddress socket, AsyncClient client) {
      frontendSocket = socket;
      this.client = client;
    }

    public void onComplete(sendFrontendMessage_call response) {
      try { schedulerClientPool.returnClient(frontendSocket, client); }
      catch (Exception e) { LOG.error(e); }
    }

    public void onError(Exception exception) {
      try { schedulerClientPool.returnClient(frontendSocket, client); }
      catch (Exception e) { LOG.error(e); }
      LOG.error(exception);
    }
  }

  public void sendFrontendMessage(String app, TFullTaskId taskId,
      int status, ByteBuffer message) {
    LOG.debug(Logging.functionCall(app, taskId, message));
    InetSocketAddress scheduler = requestSchedulers.get(taskId.requestId);
    if (scheduler == null) {
      LOG.error("Did not find any scheduler info for request: " + taskId);
      return;
    }

    try {
      AsyncClient client = schedulerClientPool.borrowClient(scheduler);
      client.sendFrontendMessage(app, taskId, status, message,
          new sendFrontendMessageCallback(scheduler, client));
      LOG.debug("finished sending message");
    } catch (IOException e) {
      LOG.error(e);
    } catch (TException e) {
      LOG.error(e);
    } catch (Exception e) {
      LOG.error(e);
    }
  }
}
