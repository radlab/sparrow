package edu.berkeley.sparrow.statestore;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.util.ConfigUtil;

/**
 * A {@link StateStoreState} which reads schedulers and node monitors from a static
 * configuration file. We expect this file to contain a complete list of node monitors
 * and schedulers which are waiting for the StateStore to contact them. If a different
 * node monitor or scheduler contacts the state store, this class throws a
 * RuntimeException.
 */
public class ConfigStateStoreState implements StateStoreState {
  private List<InetSocketAddress> nodeMonitors = new ArrayList<InetSocketAddress>();
  private List<InetSocketAddress> schedulers = new ArrayList<InetSocketAddress>();

  @Override
  public void initialize(Configuration conf) {
    nodeMonitors.addAll(ConfigUtil.parseBackends(conf));
    schedulers.addAll(ConfigUtil.parseSchedulers(conf));
  }

  @Override
  public List<InetSocketAddress> getInitialSchedulers() {
    return schedulers;
  }

  @Override
  public List<InetSocketAddress> getInitialNodeMonitors() {
    return nodeMonitors;
  }

  @Override
  public void signalInactiveScheduler(InetSocketAddress scheduler) {
    throw new RuntimeException("Not expecting loss of scheduler in " +
    		"configbased deployment mode.");
  }

  @Override
  public void signalInactiveNodeMonitor(InetSocketAddress nodeMonitor) {
    throw new RuntimeException("Not expecting loss of node monitor in " +
        "configbased deployment mode.");
  }

  @Override
  public void signalActiveScheduer(InetSocketAddress scheduler) {
    throw new RuntimeException("Not expecting new scheduler in " +
        "configbased deployment mode.");
  }

  @Override
  public void signalActiveNodeMonitor(InetSocketAddress nodeMonitor) {
    throw new RuntimeException("Not expecting new scheduler in " +
        "configbased deployment mode.");
  }
}
