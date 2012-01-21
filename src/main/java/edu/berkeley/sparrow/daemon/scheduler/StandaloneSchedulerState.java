package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.StandaloneStateStore;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Implementation of state storage for use in a standalone deployment of 
 * Sparrow.
 */
public class StandaloneSchedulerState implements SchedulerState {
  private StandaloneStateStore state = StandaloneStateStore.getInstance();
  @Override
  public void initialize(Configuration conf) {
  }
 
  @Override
  public boolean watchApplication(String appId) {
    return true;
  }
  
  @Override
  public ConcurrentMap<InetSocketAddress, TResourceVector> getBackends(String appId) {
    return state.getBackends(appId);
  }
}
