package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.ConfigUtil;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState {
  private static final Logger LOG = Logger.getLogger(ConfigSchedulerState.class);

  ConcurrentMap<InetSocketAddress, TResourceVector> backends;
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
  public ConcurrentMap<InetSocketAddress, TResourceVector> getBackends(String appId) {
    if (!appId.equals(conf.getString(SparrowConf.STATIC_APP_NAME))) {
     LOG.warn("Requested backends for app " + appId + 
          " but was expecting app " + conf.getString(SparrowConf.STATIC_APP_NAME));
    }
    return backends;
  }

}
