package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.ConfigUtil;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState {
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
      throw new RuntimeException("Requested watch for app " + appId + 
          " but was expecting app " + conf.getString(SparrowConf.STATIC_APP_NAME));
    }
    return true;
  }

  @Override
  public ConcurrentMap<InetSocketAddress, TResourceVector> getBackends(String appId) {
    if (!appId.equals(conf.getString(SparrowConf.STATIC_APP_NAME))) {
      throw new RuntimeException("Requested backends for app " + appId + 
          " but was expecting app " + conf.getString(SparrowConf.STATIC_APP_NAME));
    }
    return backends;
  }

}
