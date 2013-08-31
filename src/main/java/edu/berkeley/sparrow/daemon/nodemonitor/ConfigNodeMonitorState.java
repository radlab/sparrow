/*
 * Copyright 2013 The Regents of The University California
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.daemon.nodemonitor;

import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.ConfigUtil;

/***
 * A {@link NodeMonitorState} implementation based on a static config file.
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
      LOG.error("Requested to register backend for app " + appId +
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
