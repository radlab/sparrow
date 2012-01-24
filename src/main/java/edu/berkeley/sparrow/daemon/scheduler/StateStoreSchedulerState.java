package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
import org.mortbay.log.Log;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.TNodeState;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.SchedulerStateStoreService;

/**
 * {@link SchedulerState} implementation which relies on asynchronus updates from a 
 * central state store.
 */
public class StateStoreSchedulerState implements SchedulerState, 
                                                 SchedulerStateStoreService.Iface {
  public static int DEFAULT_SCHEDULER_STATE_THRIFT_PORT = 20503;
  public static int DEFAULT_SCHEDULER_STATE_THRIFT_THREADS = 2;
  
  private ConcurrentMap<InetSocketAddress, TResourceVector> backends = 
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
  }

  @Override
  public boolean watchApplication(String appId) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ConcurrentMap<InetSocketAddress, TResourceVector> getBackends(
      String appId) {
    return backends;
  }

  @Override
  public void updateNodeState(Map<String, TNodeState> snapshot)
      throws TException {
    for (Entry<String, TNodeState> entry : snapshot.entrySet()) {
      Optional<InetSocketAddress> address = Serialization.strToSocket(entry.getKey());
      if (!address.isPresent()) {
        Log.warn("State store gave bad backend descriptor: " + entry.getKey());
        continue;
      }
      
      // For now, simply combine Sparrow and external resource usage
      TResourceVector total = TResources.add(entry.getValue().getExternalUsage(), 
          entry.getValue().getSparrowUsage());
      backends.put(address.get(), total);
    }
  }

}
