package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * This class extends the thrift Sparrow node monitor interface. It wraps the
 * {@link NodeMonitor} class and delegates most calls to that class.
 */
public class NodeMonitorThrift implements NodeMonitorService.Iface,
                                          InternalService.Iface {
  // Defaults if not specified by configuration
  public final static int DEFAULT_NM_THRIFT_PORT = 20501;
  public final static int DEFAULT_NM_THRIFT_THREADS = 2;
  public final static int DEFAULT_INTERNAL_THRIFT_PORT = 20502;
  public final static int DEFAULT_INTERNAL_THRIFT_THREADS = 2;
 
  private NodeMonitor nodeMonitor = new NodeMonitor();
  // The socket addr (ip:port) where we listen for requests from other Sparrow daemons.
  // Used when registering backends with the state store.
  private InetSocketAddress internalAddr;
  
  /**
   * Initialize this thrift service.
   * 
   * This spawns 2 multi-threaded thrift servers, one exposing the app-facing
   * agent service and the other exposing the internal-facing agent service,
   * and listens for requests to both servers.
   */
  public void initialize(Configuration conf) throws IOException {
    nodeMonitor.initialize(conf);

    // Setup application-facing agent service.
    NodeMonitorService.Processor<NodeMonitorService.Iface> processor = 
        new NodeMonitorService.Processor<NodeMonitorService.Iface>(this);

    int port = conf.getInt(SparrowConf.NM_THRIFT_PORT, 
        DEFAULT_NM_THRIFT_PORT);
    int threads = conf.getInt(SparrowConf.NM_THRIFT_THREADS, 
        DEFAULT_NM_THRIFT_THREADS);
    TServers.launchThreadedThriftServer(port, threads, processor);

    // Setup internal-facing agent service.
    InternalService.Processor<InternalService.Iface> internalProcessor =
        new InternalService.Processor<InternalService.Iface>(this);
    int internalPort = conf.getInt(SparrowConf.INTERNAL_THRIFT_PORT,
        DEFAULT_INTERNAL_THRIFT_PORT);
    int internalThreads = conf.getInt(
        SparrowConf.INTERNAL_THRIFT_THREADS,
        DEFAULT_INTERNAL_THRIFT_THREADS);
    TServers.launchThreadedThriftServer(internalPort, internalThreads, internalProcessor);
    
    internalAddr = new InetSocketAddress(InetAddress.getLocalHost(),internalPort);
  }
  
  @Override
  public boolean registerBackend(String app, String backendSocket) throws TException {
    Optional<InetSocketAddress> backendAddr = Serialization.strToSocket(backendSocket);
    if (!backendAddr.isPresent()) {
      return false; // TODO: maybe we should throw some exception here?
    }
    return nodeMonitor.registerBackend(app, internalAddr, backendAddr.get());
  }
  
  @Override
  public TResourceVector getLoad(String app) throws TException {
    return nodeMonitor.getLoad(app);
  }


  @Override
  public boolean launchTask(String app, ByteBuffer message, ByteBuffer taskId,
      TUserGroupInfo user, TResourceVector estimatedResources)
      throws TException {
    return nodeMonitor.launchTask(app, message, taskId, user, estimatedResources);
  }


  @Override
  public void updateResourceUsage(String app,
      Map<TUserGroupInfo, TResourceVector> usage, List<ByteBuffer> activeTaskIds)
      throws TException {
    nodeMonitor.updateResourceUsage(app, usage, activeTaskIds);
  }

}