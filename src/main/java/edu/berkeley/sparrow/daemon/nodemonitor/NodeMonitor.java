package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.BackendService;
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
  /** How many blocking thrift clients to make for each registered backend. */ 
  public final static int CLIENT_POOL_SIZE = 10;
  
  private static NodeMonitorState state;
  private HashMap<String, Map<TUserGroupInfo, TResourceVector>> appLoads = 
      new HashMap<String, Map<TUserGroupInfo, TResourceVector>>();
  
  /** Cache of thrift clients pools for each backends. It is expected that clients
   *  are removed from the pool when in use. */
  private HashMap<String, BlockingQueue<BackendService.Client>> backendClients =
      new HashMap<String, BlockingQueue<BackendService.Client>>();
  private TResourceVector capacity;
  private InetAddress address;
  private Configuration conf;

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
    address = InetAddress.getLocalHost();
    this.conf = conf;
    
    // Interrogate system resources. We may want to put this in another class, and note
    // that currently this will only work on Linux machines (otherwise will use default).
    try {
      Process p = Runtime.getRuntime().exec("cat /proc/meminfo");  
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = in.readLine();
      while (line != null) {
        if (line.contains("MemTotal")) { 
          String[] parts = line.split("\\s+");
          if (parts.length > 1) {
            int memory = Integer.parseInt(parts[1]) / 1000;
            capacity.setMemory(memory);
            LOG.info("Setting memory capacity to " + memory);
          }
        }
        line = in.readLine();
      }
    } catch (IOException e) {
      LOG.info("Error interrogating memory from system.");
    }
    if (!capacity.isSetMemory()) { 
      LOG.info("Using default memory allocation: " + DEFAULT_MEMORY_MB);
      capacity.setMemory(DEFAULT_MEMORY_MB);  
    }
  }
  
  /**
   * Registers the backend with assumed 0 load, and returns true if successful.
   * Returns false if the backend was already registered.
   */
  public boolean registerBackend(String appId, InetSocketAddress nmAddr, 
      InetSocketAddress backendAddr) {
    LOG.debug(Logging.functionCall(appId, nmAddr, backendAddr));
    if (appLoads.containsKey(appId)) {
      LOG.warn("Attempt to re-register app " + appId);
      return false;
    }
    appLoads.put(appId, new HashMap<TUserGroupInfo, TResourceVector>());
    
    // NOTE: for now we do not require backends to export scheduling interface under
    // standalone mode.
    if (!conf.getString(SparrowConf.DEPLYOMENT_MODE).equals("standalone")) {
    BlockingQueue<BackendService.Client> clients = new 
        LinkedBlockingDeque<BackendService.Client>();
    for (int i = 0; i < CLIENT_POOL_SIZE; i++) {
      try {
        clients.put(TClients.createBlockingBackendClient(
            backendAddr.getHostName(), backendAddr.getPort()));
      } catch (InterruptedException e) {
        LOG.error("Interrupted creating thrift clients", e);
      } catch (IOException e) {
        LOG.error("Error creating thrift client", e);
      }
    }
    backendClients.put(appId, clients);

    }
    return state.registerBackend(appId, nmAddr);
  }

  /**
   * Return a map of applications to current resource usage (aggregated across all users).
   * If appId is set to "*", this map includes all applications. If it is set to an
   * application name, the map only includes that application. If it is set to anything
   * else, an empty map is returned.
   */
  public Map<String, TResourceVector> getLoad(String appId, String requestId) {
    LOG.debug(Logging.functionCall(appId));
    AUDIT_LOG.info(Logging.auditEventString("probe_received", requestId,
                                            address.getHostAddress()));
    Map<String, TResourceVector> out = new HashMap<String, TResourceVector>();
    if (appId.equals("*")) {
      for (String app : appLoads.keySet()) {out.put(app, aggregateAppResources(app)); }
      LOG.debug("Returning " + out);
      return out;
    }
    else if (appLoads.containsKey(appId)) {
      out.put(appId, aggregateAppResources(appId));
      LOG.debug("Returning " + out);
      return out;
    } else {
      LOG.warn("Request for load of uknown app " + appId);
      return out;
    }
  }
  
  /** Return the aggregate resource usage for a given appId, across all users. This will
   *  fail if appId is not currently tracked.
   */
  public TResourceVector aggregateAppResources(String appId) {
    TResourceVector inUse = TResources.none();
    for (TResourceVector res : appLoads.get(appId).values()) {
      TResources.addTo(inUse, res);
    }
    return inUse;
  }
  /**
   * Update the resource usage for a given application.
   */
  public void updateResourceUsage(
      String app, Map<TUserGroupInfo, TResourceVector> load, 
      List<String> activeTaskIds) {
    LOG.debug(Logging.functionCall(app, load, activeTaskIds));
    // TODO: currently ignores active task list, eventually want to check the duration
    // of tasks in that list.
    appLoads.put(app, load);
  }
  
  /**
   * Launch a task for the given app.
   */
  public boolean launchTask(String app, ByteBuffer message, String requestId,
      String taskId, TUserGroupInfo user, TResourceVector estimatedResources)
          throws TException {
    LOG.debug(Logging.functionCall(app, message, requestId, taskId, user,
                                   estimatedResources));
    AUDIT_LOG.info(Logging.auditEventString("nodemonitor_launch_start", requestId,
                                            address.getHostAddress(), taskId));
    if (!backendClients.containsKey(app)) {
      LOG.warn("Requested task launch for unknown app: " + app);
      return false;
    }
    
    BackendService.Client client = null;
    try {
      client = backendClients.get(app).take();
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    AUDIT_LOG.info(Logging.auditEventString("nodemonitor_launch_call_start", requestId,
        address.getHostAddress(), taskId));
    client.launchTask(message, requestId, taskId, user, estimatedResources);
    AUDIT_LOG.info(Logging.auditEventString("nodemonitor_launch_call_finish", requestId,
        address.getHostAddress(), taskId));
    try {
      backendClients.get(app).put(client);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    AUDIT_LOG.info(Logging.auditEventString("nodemonitor_launch_finish", requestId,
        address.getHostAddress(), taskId));
    LOG.debug("Launched task " + taskId + " for app " + app);
    return true;
  }
}
