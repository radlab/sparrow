package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.StandaloneStateStore;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * A {@link NodeMonitorState} implementation for use in standalone mode.
 */
public class StandaloneNodeMonitorState implements NodeMonitorState {
  private StandaloneStateStore stateStore = StandaloneStateStore.getInstance();
 
  @Override
  public void initialize(Configuration conf) {
    // Nothing required
  }
  
  @Override
  public boolean registerBackend(String appId, InetSocketAddress nmAddr,
      TResourceVector capacity) {
    stateStore.registerBackend(appId, nmAddr, capacity);
    return true;
  }
}
