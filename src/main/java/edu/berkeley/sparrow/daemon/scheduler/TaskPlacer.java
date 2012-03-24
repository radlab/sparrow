package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/***
 * Represents a class which is capable of determining task placement on 
 * backends.
 */
public interface TaskPlacer {
  public class TaskPlacementResponse {
    private TTaskSpec taskSpec; // Original request specification 
    private InetSocketAddress nodeAddr;
    private Optional<InternalService.AsyncClient> client; // Pointer to thrift client, if 
                                                        // this TaskPlacer spoke to Thrift
    private Optional<TNonblockingTransport> transport;
    
    public TaskPlacementResponse(TTaskSpec taskSpec, InetSocketAddress nodeAddr, 
        Optional<InternalService.AsyncClient> client, Optional<TNonblockingTransport> transport) {
      this.taskSpec = taskSpec;
      this.client = client;
      this.nodeAddr = nodeAddr;
      this.transport = transport;
    }
    
    public TTaskSpec getTaskSpec() { return this.taskSpec; }
    public InetSocketAddress getNodeAddr() { return this.nodeAddr; }
    public Optional<InternalService.AsyncClient> getClient() { return this.client; }
    public Optional<TNonblockingTransport> getTransport() { return this.transport; }
    
    public void setTransport(TNonblockingTransport transport) {
      this.transport = Optional.of(transport);
    }
    
    public void setClient(InternalService.AsyncClient client) {
      this.client = Optional.of(client);
    }
  }
  
  /** Initialize this TaskPlacer. */
  void initialize(Configuration conf);
  
  /**
   * Given a list of {@link NodeMonitor} network addresses and a list of
   * task placement preferences, return a list of task placement choices.
   * @throws IOException 
   */
  Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks,
      TAsyncClientManager clientManager) throws IOException;
  // TODO: For performance reasons it might make sense to just have these arguments as 
  //       List rather than Collection since they need to be returned as a list eventually.


}
