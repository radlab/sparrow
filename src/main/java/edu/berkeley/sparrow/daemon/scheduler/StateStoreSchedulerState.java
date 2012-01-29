package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.TNodeState;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.SchedulerStateStoreService;

/**
 * {@link SchedulerState} implementation which relies on asynchronous updates from a 
 * central state store.
 */
public class StateStoreSchedulerState implements SchedulerState, 
                                                 SchedulerStateStoreService.Iface {
  public static int DEFAULT_SCHEDULER_STATE_THRIFT_PORT = 20503;
  public static int DEFAULT_SCHEDULER_STATE_THRIFT_THREADS = 2;
  
  private final static Logger LOG = Logger.getLogger(StateStoreSchedulerState.class);
  private ConcurrentMap<InetSocketAddress, TResourceVector> nodeMonitors = 
      new ConcurrentHashMap<InetSocketAddress, TResourceVector>();
  
  @Override
  public void initialize(Configuration conf) throws IOException {
    SchedulerStateStoreService.Processor<SchedulerStateStoreService.Iface> processor = 
        new SchedulerStateStoreService.Processor<SchedulerStateStoreService.Iface>(this);
    int port = conf.getInt(SparrowConf.SCHEDULER_STATE_THRIFT_PORT, 
        DEFAULT_SCHEDULER_STATE_THRIFT_PORT);
    int threads = conf.getInt(SparrowConf.SCHEDULER_STATE_THRIFT_THREADS, 
        DEFAULT_SCHEDULER_STATE_THRIFT_THREADS);

    TServers.launchThreadedThriftServer(port, threads, processor);
    LOG.setLevel(Level.DEBUG);
  }

  @Override
  public boolean watchApplication(String appId) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ConcurrentMap<InetSocketAddress, TResourceVector> getBackends(
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
      
      // For now, simply combine Sparrow and external resource usage
      TResourceVector total = TResources.add(entry.getValue().getExternalUsage(), 
          entry.getValue().getSparrowUsage());
      nodeMonitors.put(address.get(), total);
    }
  }

}
