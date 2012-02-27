package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.scheduler.StateStoreSchedulerState;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.thrift.StateStoreService;

public class StateStoreNodeMonitorState implements NodeMonitorState {

  private final static Logger LOG = Logger.getLogger(StateStoreSchedulerState.class);
  
  @Override
  public void initialize(Configuration conf) throws IOException {
    // We need to register with the state store so it starts polling us for updates
    String stateStoreHost = conf.getString(SparrowConf.STATE_STORE_HOST, 
        SparrowConf.DEFAULT_STATE_STORE_HOST);
    int stateStorePort = conf.getInt(SparrowConf.STATE_STORE_PORT,
        SparrowConf.DEFAULT_STATE_STORE_PORT);
    StateStoreService.Client client = TClients.createBlockingStateStoreClient(
        stateStoreHost, stateStorePort);
    
    /* TODO: It's not clear whether this will always give us the right hostname.
     *       We might want to add a configuration option to set the hostname to use.*/ 
    String hostname = InetAddress.getLocalHost().getHostName();
    int port = conf.getInt(SparrowConf.INTERNAL_THRIFT_PORTS, 
        NodeMonitorThrift.DEFAULT_INTERNAL_THRIFT_PORT);
    try {
      client.registerNodeMonitor(hostname + ":" + port);
    } catch (TException e) {
      LOG.fatal("Error registering node monitor with state store");
      throw new IOException(e);
    }
    LOG.info("Registered with state store at " + stateStoreHost + ":" + stateStorePort);
  }

  @Override
  public boolean registerBackend(String appId, InetSocketAddress nodeMonitor) {
    /* NOTE: right now we don't discriminate per-application yet, so we just register
             with the state store once on instantiation. */
    return true;
  }

}
