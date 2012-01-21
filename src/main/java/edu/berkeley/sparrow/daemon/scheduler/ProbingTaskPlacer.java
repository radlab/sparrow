package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.getLoad_call;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * A task placer which probes node monitors in order to determine placement.
 */
public class ProbingTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(TaskPlacer.class);
  
  /**
   * A comparator for (node, resource vector) pairs, based on the resource vector.
   */
  private class NodeComparator implements Comparator<
      Entry<InetSocketAddress, TResourceVector>> {
    @Override
    public int compare(Entry<InetSocketAddress, TResourceVector> e1, 
        Entry<InetSocketAddress, TResourceVector> e2) {
      return TResources.compareTo(e1.getValue(), e2.getValue());
    }
  }
  
  /**
   * This acts as a callback for the asynchronous Thrift interface.
   */
  private class ProbeCallback implements AsyncMethodCallback<getLoad_call> {
    InetSocketAddress socket;
    Map<InetSocketAddress, TResourceVector> loads;
    CountDownLatch latch; // Synchronization latch so caller can return when enough
                          // backends have responded.
    
    private ProbeCallback(
        InetSocketAddress socket, Map<InetSocketAddress, TResourceVector> loads, 
        CountDownLatch latch) {
      this.socket = socket;
      this.loads = loads;
      this.latch = latch;
    }
    
    @Override
    public void onComplete(getLoad_call response) {
      LOG.debug("Received load response from node " + socket);
      try {
        TResourceVector result = response.getResult();
        loads.put(socket,result);
        latch.countDown();
      } catch (TException e) {
        LOG.error("Error getting resources from response data", e);
      }
    }

    @Override
    public void onError(Exception exception) {
      // TODO Auto-generated method stub
      
    }
  }
    
  public ProbingTaskPlacer() {
    LOG.setLevel(Level.DEBUG);
  }
  
  @Override
  public Collection<TaskPlacer.TaskPlacementResponse> placeTasks(String appId,
      Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks)
      throws IOException {
    Map<InetSocketAddress, TResourceVector> loads = 
        new HashMap<InetSocketAddress, TResourceVector>(); 

    TAsyncClientManager clientManager = new TAsyncClientManager();
    
    // Keep track of thrift clients since we return handles on them to caller
    Map<InetSocketAddress, InternalService.AsyncClient> clients = 
        new HashMap<InetSocketAddress, InternalService.AsyncClient>();
    
    // This latch decides how many nodes need to respond for us to make a decision.
    // Using a simple counter is okay for now, but eventually we will want to use
    // per-task information to decide when to return.
    CountDownLatch latch = new CountDownLatch(tasks.size());
    
    for (InetSocketAddress node : nodes) {
      TNonblockingTransport nbTr = new TNonblockingSocket(
          node.getHostName(), node.getPort());
      TProtocolFactory factory = new TBinaryProtocol.Factory();
      InternalService.AsyncClient client = new InternalService.AsyncClient(
          factory, clientManager, nbTr);
      clients.put(node, client);
      try {
        ProbeCallback callback = new ProbeCallback(node, loads, latch);
        LOG.debug("Launching probe on node: " + node); 
        client.getLoad(appId, callback);
      } catch (TException e) {
        e.printStackTrace();
      }
    }
    
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Sort nodes by resource usage
    List<Entry<InetSocketAddress, TResourceVector>> results = 
        new ArrayList<Entry<InetSocketAddress, TResourceVector>>(loads.entrySet());
    Collections.sort(results, new NodeComparator());
    Collections.reverse(results);
    
    ArrayList<TaskPlacementResponse> out = new ArrayList<TaskPlacementResponse>();
    
    int i = 0;
    for (TTaskSpec task : tasks) {
      Entry<InetSocketAddress, TResourceVector> entry = results.get(i++ % results.size());
      
      TaskPlacementResponse place = new TaskPlacementResponse(
          task, entry.getKey(), Optional.of(clients.get(entry.getKey())));
      out.add(place);
    }
    
    return out;
  }
}
