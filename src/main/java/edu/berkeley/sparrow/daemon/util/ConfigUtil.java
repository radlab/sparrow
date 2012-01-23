package edu.berkeley.sparrow.daemon.util;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Utilities to aid the configuration file-based scheduler and node monitor.
 */
public class ConfigUtil {
  /**
   * Parses the list of backends from a {@code Configuration}.
   */
  public static ConcurrentMap<InetSocketAddress, TResourceVector> parseBackends(
      Configuration conf) {
    if (!conf.containsKey(SparrowConf.STATIC_BACKENDS) || 
        !conf.containsKey(SparrowConf.STATIC_MEM_PER_BACKEND)) {
      throw new RuntimeException("Missing configuration backend list.");
    }
    
    ConcurrentMap<InetSocketAddress, TResourceVector> backends =
        new ConcurrentHashMap<InetSocketAddress, TResourceVector>();
    TResourceVector nodeResources = TResources.createResourceVector(
        conf.getInt(SparrowConf.STATIC_MEM_PER_BACKEND));
    
    for (String node: conf.getStringArray(SparrowConf.STATIC_BACKENDS)) {
      String[] parts = node.split(":");
      if (parts.length != 2) {
        throw new RuntimeException("Invalid backend address: " + node);
      }
      int port = Integer.parseInt(parts[1]);
      InetSocketAddress addr = new InetSocketAddress(parts[0], port);
      backends.put(addr, nodeResources);
    }
    
    return backends;
  }
}
