package edu.berkeley.sparrow.statestore;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.util.ConfigUtil;

public class ConfigStateStoreState implements StateStoreState {
  private List<InetSocketAddress> nodeMonitors = new ArrayList<InetSocketAddress>();
  private List<InetSocketAddress> schedulers = new ArrayList<InetSocketAddress>();
  
  @Override
  public void initialize(Configuration conf) {
    nodeMonitors.addAll(ConfigUtil.parseBackends(conf).keySet());
    schedulers.addAll(ConfigUtil.parseSchedulers(conf));
  }
  
  @Override
  public List<InetSocketAddress> getSchedulers() {
    return schedulers;
  }

  @Override
  public List<InetSocketAddress> getNodeMonitors() {
    return nodeMonitors;
  }
}
