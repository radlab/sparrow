package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;


public interface NodeMonitorState {
  /**
   * Initialize state storage. This should open connections to any external
   * services if required.
   * @throws IOException 
   */
  public void initialize(Configuration conf) throws IOException;
  
  /**
   * Register a backend identified by {@code appId} which can be reached via
   * a NodeMonitor running at the given address. The node is assumed to have
   * resources given by {@code capacity}.
   */
  public boolean registerBackend(String appId, InetSocketAddress nodeMonitor);
}
