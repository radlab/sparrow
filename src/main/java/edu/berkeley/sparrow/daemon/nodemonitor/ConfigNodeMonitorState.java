package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.ConfigUtil;
import edu.berkeley.sparrow.thrift.TResourceVector;

/***
 * A {@link NodeMonitorState} implementation which is based on a static config
 * file.
 */
public class ConfigNodeMonitorState implements NodeMonitorState {
  ConcurrentMap<InetSocketAddress, TResourceVector> backends;
  String staticAppId;
  
  @Override
  public void initialize(Configuration conf) {
    backends = ConfigUtil.parseBackends(conf);
    staticAppId = conf.getString(SparrowConf.STATIC_APP_NAME);
  }

  @Override
  public boolean registerBackend(String appId, InetSocketAddress nodeMonitor, 
      TResourceVector resources) {
    // Verify that the given backend information matches the static configuration.
    if (!appId.equals(staticAppId)) {
      throw new RuntimeException("Requested to register backend for app " + appId +
          " but was expecting app " + staticAppId);
    } else if (!backends.containsKey(nodeMonitor)) {
      throw new RuntimeException("Address " + nodeMonitor.toString() + 
          " not found among statically configured addreses for app " + appId);
    } else if (!backends.get(nodeMonitor).equals(resources)) {
      throw new RuntimeException("Given resources (" + resources.toString() +
          ") for node monitor at " + nodeMonitor.toString() +" for app " + appId +
          " don't match statically configured resources (" +
          backends.get(nodeMonitor).toString() + ")");
    }
    
    return true;
  }
}
