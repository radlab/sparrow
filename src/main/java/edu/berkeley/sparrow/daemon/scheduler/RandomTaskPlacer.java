package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TTransport;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/***
 * A {@link TaskPlacer} implementation which randomly distributes tasks accross
 * backends. Note that if there are fewer backends than tasks, this will distributed multiple
 * tasks on some backends.
 */
public class RandomTaskPlacer implements TaskPlacer {
  
  @Override
  public Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks, 
      TAsyncClientManager clientManager) throws IOException {
    Collection<TaskPlacementResponse> out = new HashSet<TaskPlacementResponse>();
    
    ArrayList<InetSocketAddress> orderedNodes = new ArrayList<InetSocketAddress>(nodes);
    Collections.shuffle(orderedNodes);
    
    // Empty client/transport used for all responses
    Optional<InternalService.AsyncClient> client = Optional.absent();
    Optional<TTransport> transport = Optional.absent();
   
    int i = 0;
    for (TTaskSpec task : tasks) {
      InetSocketAddress addr = orderedNodes.get(i++ % nodes.size());
      TaskPlacementResponse response = new TaskPlacementResponse(task,
          addr, client, transport);
      out.add(response);
    }
    return out;
  }

  @Override
  public void initialize(Configuration conf) {
  }
}
