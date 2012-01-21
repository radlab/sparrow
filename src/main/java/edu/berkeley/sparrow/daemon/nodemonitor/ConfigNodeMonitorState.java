package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.thrift.TResourceVector;

/***
 * A {@link NodeMonitorState} implementation which is based on a static config
 * file.
 */
public class ConfigNodeMonitorState implements NodeMonitorState {

  @Override
  public boolean registerBackend(String appId, InetSocketAddress nodeMonitor, 
      TResourceVector resources) {
    // Do nothing, we don't need to register anything since it's read from
    // a config file.
    return true;
  }

  @Override
  public void initialize(Configuration conf) {
    // TODO Auto-generated method stub
    
  }

}
