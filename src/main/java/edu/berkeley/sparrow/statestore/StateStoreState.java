package edu.berkeley.sparrow.statestore;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.configuration.Configuration;

/**
 * Represents a class capable of supplying the {@link StateStore} with information
 * about Sparrow frontends and backends running in the cluster.
 */
public interface StateStoreState {
  /** Initialize this state. */
  void initialize(Configuration conf);
  
  /**
   * Return the Sparrow internal interface address of known scheduling nodes.
   */
  List<InetSocketAddress> getSchedulers();
  
  /**
   * Return the Sparrow internal interface address of known node monitors.
   */
  List<InetSocketAddress> getNodeMonitors();
}
