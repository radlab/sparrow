package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.ConfigUtil;

/***
 * A {@link NodeMonitorState} implementation which is based on a static config
 * file.
 */
public class ConfigNodeMonitorState implements NodeMonitorState {
  private static final Logger LOG = Logger.getLogger(ConfigNodeMonitorState.class);

  private Set<InetSocketAddress> nodeMonitors;
  private String staticAppId;

  @Override
  public void initialize(Configuration conf) {
    nodeMonitors = ConfigUtil.parseBackends(conf);
    staticAppId = conf.getString(SparrowConf.STATIC_APP_NAME);
  }

  @Override
  public boolean registerBackend(String appId, InetSocketAddress nodeMonitor) {
    // Verify that the given backend information matches the static configuration.
    if (!appId.equals(staticAppId)) {
      LOG.warn("Requested to register backend for app " + appId +
          " but was expecting app " + staticAppId);
    } else if (!nodeMonitors.contains(nodeMonitor)) {
      StringBuilder errorMessage = new StringBuilder();
      for (InetSocketAddress nodeMonitorAddress : nodeMonitors) {
        errorMessage.append(nodeMonitorAddress.toString());
      }
      throw new RuntimeException("Address " + nodeMonitor.toString() +
          " not found among statically configured addreses for app " + appId + " (statically " +
          "configured addresses include: " + errorMessage.toString());
    }

    return true;
  }
}
