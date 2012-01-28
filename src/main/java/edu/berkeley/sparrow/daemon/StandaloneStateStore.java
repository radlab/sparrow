package edu.berkeley.sparrow.daemon;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.berkeley.sparrow.daemon.nodemonitor.StandaloneNodeMonitorState;
import edu.berkeley.sparrow.daemon.scheduler.StandaloneSchedulerState;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * When Sparrow is running in standalone mode (single machine) it is
 * necessary to have a singleton global state store to coordinate data
 * between {@link StandaloneNodeMonitorState} and 
 * {@link StandaloneSchedulerState}. This class acts as that state store.
 */
public class StandaloneStateStore {
  private static final StandaloneStateStore instance = 
      new StandaloneStateStore();

  public static StandaloneStateStore getInstance() {
          return instance;
  }
  
  // appId -> map of app nodes
  private Map<String, Map<InetSocketAddress, TResourceVector>> applications;
  
  // Private constructor prevents instantiation from other classes
  private StandaloneStateStore() {
    applications = new HashMap<String, Map<InetSocketAddress, TResourceVector>>();
  }
  
  // SOURCE: StandaloneNodeMonitorState
  public synchronized void registerBackend(
      String appId, InetSocketAddress nmAddr) {
    if (!this.applications.containsKey(appId)) {
      this.applications.put(appId, new HashMap<InetSocketAddress, TResourceVector>());
    }
    this.applications.get(appId).put(nmAddr, new TResourceVector());
  }
  
  // SOURCE: StandaloneSchedulerState
  public synchronized ConcurrentHashMap<InetSocketAddress, TResourceVector> getBackends(
      String appId) {
    if (applications.containsKey(appId)) {
      return new ConcurrentHashMap<InetSocketAddress, TResourceVector>(
          applications.get(appId));
    } else {
      return new ConcurrentHashMap<InetSocketAddress, TResourceVector>();
    }
  }
}
