package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.transport.TNonblockingTransport;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitor;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/***
 * Represents a class which is capable of determining task placement on 
 * backends.
 */
public interface TaskPlacer {
  public class TaskPlacementResponse {
    private TTaskSpec taskSpec; // Original request specification 
    private InetSocketAddress nodeAddr;
    
    public TaskPlacementResponse(TTaskSpec taskSpec, InetSocketAddress nodeAddr, 
        Optional<InternalService.AsyncClient> client, Optional<TNonblockingTransport> transport) {
      this.taskSpec = taskSpec;
      this.nodeAddr = nodeAddr;
    }
    
    public TTaskSpec getTaskSpec() { return this.taskSpec; }
    public InetSocketAddress getNodeAddr() { return this.nodeAddr; }
  }
  
  /** Initialize this TaskPlacer. */
  void initialize(Configuration conf, ThriftClientPool<AsyncClient> clientPool);
  
  /**
   * Given a list of {@link NodeMonitor} network addresses and a list of
   * task placement preferences, return a list of task placement choices.
   * @throws IOException 
   */
  Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks)
          throws IOException;
  // TODO: For performance reasons it might make sense to just have these arguments as 
  //       List rather than Collection since they need to be returned as a list eventually.

}
