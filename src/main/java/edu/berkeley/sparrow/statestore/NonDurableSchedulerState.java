package edu.berkeley.sparrow.statestore;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;

/**
 * A {@link StateStoreState} implementation which initializes with no node
 * monitors or schedulers and stores information in memory. 
 */
public class NonDurableSchedulerState implements StateStoreState {
  private List<InetSocketAddress> nodeMonitors = new ArrayList<InetSocketAddress>();
  private List<InetSocketAddress> schedulers = new ArrayList<InetSocketAddress>();
  
  @Override
  public void initialize(Configuration conf) {
    // Just chillin
  }

  @Override
  public List<InetSocketAddress> getInitialSchedulers() {
    return new ArrayList<InetSocketAddress>();
  }

  @Override
  public List<InetSocketAddress> getInitialNodeMonitors() {
    return new ArrayList<InetSocketAddress>();
  }

  @Override
  public void signalInactiveScheduler(InetSocketAddress scheduler) {
    if (schedulers.contains(scheduler)) { schedulers.remove(scheduler); }
  }

  @Override
  public void signalInactiveNodeMonitor(InetSocketAddress nodeMonitor) {
    if (nodeMonitors.contains(nodeMonitor)) { nodeMonitors.remove(nodeMonitor); }
  }

  @Override
  public void signalActiveScheduer(InetSocketAddress scheduler) {
    if (!schedulers.contains(scheduler)) { schedulers.add(scheduler); }
  }

  @Override
  public void signalActiveNodeMonitor(InetSocketAddress nodeMonitor) {
    if (nodeMonitors.contains(nodeMonitor)) { nodeMonitors.remove(nodeMonitor); }
  }

}
