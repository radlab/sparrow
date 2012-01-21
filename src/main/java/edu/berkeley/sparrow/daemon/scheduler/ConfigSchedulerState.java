package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState {
  private TResourceVector resPerNode;
  ConcurrentMap<InetSocketAddress, TResourceVector> backends;
  private Configuration conf;
  
  @Override
  public void initialize(Configuration conf) {
    if (!conf.containsKey(SparrowConf.STATIC_BACKENDS) || 
        !conf.containsKey(SparrowConf.STATIC_MEM_PER_BACKEND)) {
      throw new RuntimeException("Missing configuration backend list.");
    }
    backends = new ConcurrentHashMap<InetSocketAddress, TResourceVector>();
    
    resPerNode = TResources.createResourceVector(
        conf.getInt(SparrowConf.STATIC_MEM_PER_BACKEND));
    
    for (String node: conf.getStringArray(SparrowConf.STATIC_BACKENDS)) {
      String[] parts = node.split(":");
      if (parts.length != 2) {
        throw new RuntimeException("Invalid backend address: " + node);
      }
      int port = Integer.parseInt(parts[1]);
      InetSocketAddress addr = new InetSocketAddress(parts[0], port);
      backends.put(addr, resPerNode);
    }
    
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
    return backends;
  }

}
