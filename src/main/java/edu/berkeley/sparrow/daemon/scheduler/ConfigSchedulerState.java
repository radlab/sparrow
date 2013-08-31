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

package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.ConfigUtil;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState {
  private static final Logger LOG = Logger.getLogger(ConfigSchedulerState.class);

  Set<InetSocketAddress> backends;
  private Configuration conf;

  @Override
  public void initialize(Configuration conf) {
    backends = ConfigUtil.parseBackends(conf);
    this.conf = conf;
  }

  @Override
  public boolean watchApplication(String appId) {
    if (!appId.equals(conf.getString(SparrowConf.STATIC_APP_NAME))) {
      LOG.warn("Requested watch for app " + appId +
          " but was expecting app " + conf.getString(SparrowConf.STATIC_APP_NAME));
    }
    return true;
  }

  @Override
  public Set<InetSocketAddress> getBackends(String appId) {
    if (!appId.equals(conf.getString(SparrowConf.STATIC_APP_NAME))) {
     LOG.warn("Requested backends for app " + appId +
          " but was expecting app " + conf.getString(SparrowConf.STATIC_APP_NAME));
    }
    return backends;
  }

}
