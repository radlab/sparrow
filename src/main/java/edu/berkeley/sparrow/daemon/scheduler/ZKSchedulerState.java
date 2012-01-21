package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Scheduler state that uses a shared ZooKeeper instance.
 */
public class ZKSchedulerState implements SchedulerState {

  @Override
  public void initialize(Configuration conf) {
    // TODO Connect to ZooKeeper.

  }

  @Override
  public boolean watchApplication(String appId) {
    // TODO Install relevant watches.
    return false;
  }

  @Override
  public ConcurrentMap<InetSocketAddress, TResourceVector> getBackends(String appId) {
    // TODO Query ZK node (or serve from local storage if we cache this).
    return null;
  }

}
