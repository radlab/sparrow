package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.SchedulerStateStoreService;
import edu.berkeley.sparrow.thrift.StateStoreService;
import edu.berkeley.sparrow.thrift.TNodeState;

/**
 * {@link SchedulerState} implementation which relies on asynchronous updates from a
 * central state store.
 */
public class StateStoreSchedulerState implements SchedulerState,
                                                 SchedulerStateStoreService.Iface {
  public static int DEFAULT_SCHEDULER_STATE_THRIFT_PORT = 20504;
  public static int DEFAULT_SCHEDULER_STATE_THRIFT_THREADS = 2;

  private final static Logger LOG = Logger.getLogger(StateStoreSchedulerState.class);
  private Set<InetSocketAddress> nodeMonitors = new HashSet<InetSocketAddress>();

  @Override
  public void initialize(Configuration conf) throws IOException {
    String stateStoreHost = conf.getString(SparrowConf.STATE_STORE_HOST,
        SparrowConf.DEFAULT_STATE_STORE_HOST);
    int stateStorePort = conf.getInt(SparrowConf.STATE_STORE_PORT,
        SparrowConf.DEFAULT_STATE_STORE_PORT);
    StateStoreService.Client client = TClients.createBlockingStateStoreClient(
        stateStoreHost, stateStorePort);
    int port = conf.getInt(SparrowConf.SCHEDULER_STATE_THRIFT_PORT,
        DEFAULT_SCHEDULER_STATE_THRIFT_PORT);
    /* TODO: It's not clear whether this will always give us the right hostname.
     *       We might want to add a configuration option to set the hostname to use.*/
    String hostname = Network.getHostName(conf);
    try {
      client.registerScheduler(hostname + ":" + port);
    } catch (TException e) {
      LOG.fatal("Error registering scheduler with state store");
      throw new IOException(e);
    }
    LOG.info("Registered with state store at " + stateStoreHost + ":" + stateStorePort);
    SchedulerStateStoreService.Processor<SchedulerStateStoreService.Iface> processor =
        new SchedulerStateStoreService.Processor<SchedulerStateStoreService.Iface>(this);
    int threads = conf.getInt(SparrowConf.SCHEDULER_STATE_THRIFT_THREADS,
        DEFAULT_SCHEDULER_STATE_THRIFT_THREADS);

    TServers.launchThreadedThriftServer(port, threads, processor);
  }

  @Override
  public boolean watchApplication(String appId) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Set<InetSocketAddress> getBackends(
      String appId) {
    return nodeMonitors;
  }

  @Override
  public void updateNodeState(Map<String, TNodeState> snapshot)
      throws TException {
    LOG.debug(Logging.functionCall(snapshot));
    for (Entry<String, TNodeState> entry : snapshot.entrySet()) {
      Optional<InetSocketAddress> address = Serialization.strToSocket(entry.getKey());
      if (!address.isPresent()) {
        LOG.warn("State store gave bad node monitor descriptor: " + entry.getKey());
        continue;
      }

      // Currently, resource usage given in the snapshot is ignored.
      nodeMonitors.add(address.get());
    }
  }

}
