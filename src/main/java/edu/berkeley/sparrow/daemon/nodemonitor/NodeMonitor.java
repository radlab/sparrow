package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Hostname;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Resources;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.SchedulerService.AsyncClient;
import edu.berkeley.sparrow.thrift.SchedulerService.AsyncClient.sendFrontendMessage_call;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * A Node Monitor which is responsible for communicating with application
 * backends. This class is wrapped by multiple thrift servers, so it may 
 * be concurrently accessed when handling multiple function calls 
 * simultaneously.
 */
public class NodeMonitor {
  private final static Logger LOG = Logger.getLogger(NodeMonitor.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(NodeMonitor.class);
  private final static int DEFAULT_MEMORY_MB = 1024; // Default memory capacity
  private final static int DEFAULT_CORES = 8;        // Default CPU capacity
  
  /** How many blocking thrift clients to make for each registered backend. */ 
  
  private static NodeMonitorState state;
  private HashMap<String, InetSocketAddress> appSockets = 
      new HashMap<String, InetSocketAddress>();
  private HashMap<String, List<TFullTaskId>> appTasks = 
      new HashMap<String, List<TFullTaskId>>();
  // Map to scheduler socket address for each request id.
  private HashMap<String, InetSocketAddress> appSchedulers =
      new HashMap<String, InetSocketAddress>();
  private ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool = 
      new ThriftClientPool<SchedulerService.AsyncClient>(
          new ThriftClientPool.SchedulerServiceMakerFactory());
  
  private TResourceVector capacity;
  private Configuration conf;
  private String ipAddress;
  private FifoTaskScheduler scheduler;
  private TaskLauncherService taskLauncherService;

  public void initialize(Configuration conf) throws UnknownHostException {
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
    this.conf = conf;
    ipAddress = Hostname.getIPAddress(conf);
    
    int mem = Resources.getSystemMemoryMb(conf);
    capacity.setMemory(mem);
    LOG.info("Using memory allocation: " + mem);
    
    int cores = Resources.getSystemCPUCount(conf);
    capacity.setCores(cores);
    LOG.info("Using core allocation: " + cores);
    
    scheduler = new FifoTaskScheduler();
    scheduler.setMaxActiveTasks(cores);
    scheduler.initialize(capacity, conf);
    taskLauncherService = new TaskLauncherService();
    taskLauncherService.initialize(conf, scheduler);
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
    if (!requestId.equals("*")) { // Don't log state store request
      AUDIT_LOG.info(Logging.auditEventString("probe_received", requestId,
                                              ipAddress));
    }
    Map<String, TResourceUsage> out = new HashMap<String, TResourceUsage>();
    if (appId.equals("*")) {
      for (String app : appSockets.keySet()) {
        out.put(app, scheduler.getResourceUsage(app)); 
      }
    }
    else {
      out.put(appId, scheduler.getResourceUsage(appId));
    }
    LOG.debug("Returning " + out);
    return out;
  }

  /**
   * Update the resource usage for a given application.
   */
  public void updateResourceUsage(
      String app, Map<TUserGroupInfo, TResourceVector> load, 
      List<TFullTaskId> activeTaskIds) {
    LOG.debug(Logging.functionCall(app, load, activeTaskIds));
    scheduler.noteCompletedTasks(activeTaskIds);
  }
  
  /**
   * Launch a task for the given app.
   * @param schedulerAddress 
   */
  public boolean launchTask(ByteBuffer message, TFullTaskId taskId,
      TUserGroupInfo user, TResourceVector estimatedResources)
          throws TException {
    LOG.debug(Logging.functionCall(message, taskId, user, estimatedResources));
    
    Optional<InetSocketAddress> schedAddr = Serialization.strToSocket(
        taskId.frontendSocket);
    if (!schedAddr.isPresent()) {
      LOG.error("No scheduler address specified in request for " + taskId.appId + " got " + 
          taskId.frontendSocket);
      return false;
    }
    appSchedulers.put(taskId.requestId, schedAddr.get());
    
    InetSocketAddress socket = appSockets.get(taskId.appId);
    if (socket == null) {
      LOG.error("No socket stored for " + taskId.appId + " (never registered?). " +
      		"Can't launch task.");
      return false;
    }
    scheduler.submitTask(scheduler.new TaskDescription(taskId, message, 
        estimatedResources, user, socket), taskId.appId);
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
  
  public void sendFrontendMessage(String app, String requestId,
      ByteBuffer message) {
    LOG.debug(Logging.functionCall(app, requestId, message));
    InetSocketAddress scheduler = appSchedulers.get(requestId);
    if (scheduler == null) {
      LOG.error("Did not find any scheduler info for request: " + requestId);
      return;
    }
    
    try {
      LOG.debug("requestID: " + requestId + " scheduler: " + 
                scheduler.getHostName() + " app:" + app);
      AsyncClient client = schedulerClientPool.borrowClient(scheduler);
      client.sendFrontendMessage(app, requestId, message, 
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
