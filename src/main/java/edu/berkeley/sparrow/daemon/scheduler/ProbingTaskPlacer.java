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

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.getLoad_call;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * A task placer which probes node monitors in order to determine placement.
 */
public class ProbingTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(TaskPlacer.class);
  private static final Logger AUDIT_LOG = Logging.getAuditLogger(TaskPlacer.class);
  
  /**
   * A comparator for (node, resource vector) pairs, based on the resource vector.
   */
  private class NodeComparator implements Comparator<
      Entry<InetSocketAddress, TResourceVector>> {
    @Override
    public int compare(Entry<InetSocketAddress, TResourceVector> e1, 
        Entry<InetSocketAddress, TResourceVector> e2) {
      // NOTE: right now we just compare based on memory. This is a simplification, since
      // resource vectors are actually multi-dimensional.
      long m1 = e1.getValue().memory;
      long m2 = e2.getValue().memory;
      if (m1 > m2) { return 1; }
      if (m1 < m2) { return -1; }
      return 0;
    }
  }
  
  /**
   * This acts as a callback for the asynchronous Thrift interface.
   */
  private class ProbeCallback implements AsyncMethodCallback<getLoad_call> {
    InetSocketAddress socket;
    /** This should not be modified after the {@code latch} count is zero! */
    Map<InetSocketAddress, TResourceVector> loads;
    /** Synchronization latch so caller can return when enough backends have
     * responded. */
    CountDownLatch latch;
    private String appId;
    private String requestId;
    private TTransport transport;
    
    private ProbeCallback(
        InetSocketAddress socket, Map<InetSocketAddress, TResourceVector> loads, 
        CountDownLatch latch, String appId, String requestId, TTransport transport) {
      this.socket = socket;
      this.loads = loads;
      this.latch = latch;
      this.appId = appId;
      this.requestId = requestId;
      this.transport = transport;
    }
    
    @Override
    public void onComplete(getLoad_call response) {
      LOG.debug("Received load response from node " + socket);
      
      // TODO: Include the port, as well as the address, in the log message, so this
      // works properly when multiple daemons are running on the same machine.
      AUDIT_LOG.info(Logging.auditEventString("probe_completion", requestId,
                                              socket.getAddress().getHostAddress()));
      try {
        if (latch.getCount() == 0) {
          transport.close();
        }
        else if (!response.getResult().containsKey(appId)) {
          LOG.warn("Probe returned no load information for " + appId);
        }
        else {
          TResourceVector result = response.getResult().get(appId);
          loads.put(socket,result);
          latch.countDown();
        }
      } catch (TException e) {
        LOG.error("Error getting resources from response data", e);
      }
    }

    @Override
    public void onError(Exception exception) {
      LOG.error("Error in probe callback", exception);
      // TODO: Figure out what failure model we want here
      latch.countDown(); 
    }
  }
  
  @Override
  public Collection<TaskPlacer.TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks,
      TAsyncClientManager clientManager) throws IOException {
    LOG.debug(Logging.functionCall(appId, nodes, tasks));
    Map<InetSocketAddress, TResourceVector> loads = 
        new HashMap<InetSocketAddress, TResourceVector>(); 
    
    // Keep track of thrift clients/transports since we return handles on them to caller
    Map<InetSocketAddress, InternalService.AsyncClient> clients = 
        new HashMap<InetSocketAddress, InternalService.AsyncClient>();
    Map<InetSocketAddress, TTransport> transports = 
        new HashMap<InetSocketAddress, TTransport>();
    
    // This latch decides how many nodes need to respond for us to make a decision.
    // Using a simple counter is okay for now, but eventually we will want to use
    // per-task information to decide when to return.
    CountDownLatch latch = new CountDownLatch(nodes.size());
    
    for (InetSocketAddress node : nodes) {
      TNonblockingTransport nbTr = new TNonblockingSocket(
          node.getHostName(), node.getPort());
      TProtocolFactory factory = new TBinaryProtocol.Factory();
      InternalService.AsyncClient client = new InternalService.AsyncClient(
          factory, clientManager, nbTr);
      clients.put(node, client);
      transports.put(node, nbTr);
      try {
        ProbeCallback callback = new ProbeCallback(node, loads, latch, appId, requestId,
                                                   nbTr);
        LOG.debug("Launching probe on node: " + node); 
        AUDIT_LOG.info(Logging.auditEventString("probe_launch", requestId,
                                                node.getAddress().getHostAddress()));
        client.getLoad(appId, requestId, callback);
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
          task, entry.getKey(), Optional.of(clients.get(entry.getKey())),
          Optional.of(transports.get(entry.getKey())));
      out.add(place);
    }
    
    // Close out any sockets related to nodes we aren't going to use
    // TODO: really we need to change the way that thrift handles are re-used,
    // and have a pool of thrift handles that is shared between the Scheduler and
    // this class. The pool should be periodically cleaned up based on LRU.
    for (;i < results.size(); i++) {
      InetSocketAddress addr = results.get(i).getKey();
      transports.get(addr).close();
    }
    return out;
  }
}
