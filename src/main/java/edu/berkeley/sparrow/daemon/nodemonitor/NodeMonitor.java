package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
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
  private final static int DEFAULT_MEMORY_MB = 1024; // Default memory capacity
  
  private static NodeMonitorState state;
  private HashMap<String, Map<TUserGroupInfo, TResourceVector>> appLoads = 
      new HashMap<String, Map<TUserGroupInfo, TResourceVector>>();
  private HashMap<String, BackendService.Client> backendClients =
      new HashMap<String, BackendService.Client>();
  private TResourceVector capacity;

  public void initialize(Configuration conf) {
    LOG.setLevel(Level.DEBUG);
    String mode = conf.getString(SparrowConf.DEPLYOMENT_MODE, "unspecified");
    if (mode.equals("standalone")) {
      state = new StandaloneNodeMonitorState();
    } else if (mode.equals("configbased")) {
      state = new ConfigNodeMonitorState();
    }  else {
      throw new RuntimeException("Unsupported deployment mode: " + mode);
    }
    state.initialize(conf);
    capacity = new TResourceVector();
    
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
    TTransport tr = new TFramedTransport(
        new TSocket(backendAddr.getHostName(), backendAddr.getPort()));
    try {
      tr.open();
    } catch (TTransportException e) {
      e.printStackTrace(); // TODO handle
    }
    TProtocol proto = new TBinaryProtocol(tr);
    backendClients.put(appId, new BackendService.Client(proto));
    return state.registerBackend(appId, nmAddr);
  }

  /**
   * Return a map of applications to current resource usage (aggregated across all users).
   * If appId is set to "*", this map includes all applications. If it is set to an
   * application name, the map only includes that application. If it is set to anything
   * else, an empty map is returned.
   */
  public Map<String, TResourceVector> getLoad(String appId) {
    LOG.debug(Logging.functionCall(appId));
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
      List<ByteBuffer> activeTaskIds) {
    LOG.debug(Logging.functionCall(app, load, activeTaskIds));
    // TODO: currently ignores active task list, eventually want to check the duration
    // of tasks in that list.
    appLoads.put(app, load);
  }
  
  /**
   * Launch a task for the given app.
   */
  public boolean launchTask(String app, ByteBuffer message, ByteBuffer taskId,
      TUserGroupInfo user, TResourceVector estimatedResources) throws TException {
    LOG.debug(Logging.functionCall(app, message, taskId, user, estimatedResources));
    if (!backendClients.containsKey(app)) {
      LOG.warn("Requested task launch for unknown app: " + app);
      return false;
    }
    BackendService.Client client = backendClients.get(app);
    client.launchTask(message, taskId, user, estimatedResources);
    LOG.debug("Launched task " + taskId + " for app " + app);
    return true;
  }
}
