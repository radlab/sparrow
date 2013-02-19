package edu.berkeley.sparrow.daemon;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.berkeley.sparrow.daemon.nodemonitor.StandaloneNodeMonitorState;
import edu.berkeley.sparrow.daemon.scheduler.StandaloneSchedulerState;

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
  private Map<String, Set<InetSocketAddress>> applications;

  // Private constructor prevents instantiation from other classes
  private StandaloneStateStore() {
    applications = new HashMap<String, Set<InetSocketAddress>>();
  }

  // SOURCE: StandaloneNodeMonitorState
  public synchronized void registerBackend(
      String appId, InetSocketAddress nmAddr) {
    if (!this.applications.containsKey(appId)) {
      this.applications.put(appId, new HashSet<InetSocketAddress>());
    }
    this.applications.get(appId).add(nmAddr);
  }

  // SOURCE: StandaloneSchedulerState
  public synchronized Set<InetSocketAddress> getBackends(
      String appId) {
    if (applications.containsKey(appId)) {
      return new HashSet<InetSocketAddress>(
          applications.get(appId));
    } else {
      return new HashSet<InetSocketAddress>();
    }
  }
}
