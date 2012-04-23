package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.getLoad_call;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TSchedulingPref;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * A task placer which probes node monitors in order to determine placement.
 */
public class ProbingTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(ProbingTaskPlacer.class);
  protected static final Logger AUDIT_LOG = Logging.getAuditLogger(ProbingTaskPlacer.class);
   
  /** See {@link SparrowConf.PROBE_MULTIPLIER} */
  private double probeRatio;
  
  private ThriftClientPool<AsyncClient> clientPool;
  
  /**
   * This acts as a callback for the asynchronous Thrift interface.
   */
  protected class ProbeCallback implements AsyncMethodCallback<getLoad_call> {
    InetSocketAddress socket;
    /** This should not be modified after the {@code latch} count is zero! */
    Map<InetSocketAddress, TResourceUsage> loads;
    /** Synchronization latch so caller can return when enough backends have
     * responded. */
    CountDownLatch latch;
    private String appId;
    private String requestId;
    private AsyncClient client;
    
    ProbeCallback(
        InetSocketAddress socket, Map<InetSocketAddress, TResourceUsage> loads, 
        CountDownLatch latch, String appId, String requestId, AsyncClient client) {
      this.socket = socket;
      this.loads = loads;
      this.latch = latch;
      this.appId = appId;
      this.requestId = requestId;
      this.client = client;
    }
    
    @Override
    public void onComplete(getLoad_call response) {
      LOG.debug("Received load response from node " + socket);
      
      // TODO: Include the port, as well as the address, in the log message, so this
      // works properly when multiple daemons are running on the same machine.
      int queueLength = -1;
      int cores = -1;
      try {
        queueLength = response.getResult().get(appId).queueLength;
        cores = response.getResult().get(appId).resources.cores;
      } catch (TException e1) {
        LOG.error("Probe returned no information for " + appId);
      }
      AUDIT_LOG.info(Logging.auditEventString("probe_completion", requestId,
                                              socket.getAddress().getHostAddress(),
                                              queueLength, cores));
      try {
        clientPool.returnClient(socket, client);
        Map<String, TResourceUsage> resp = response.getResult();
        if (!resp.containsKey(appId)) {
          LOG.warn("Probe returned no load information for " + appId);
        }
        else {
          TResourceUsage result = response.getResult().get(appId);
          loads.put(socket,result);
          latch.countDown();
        }
      } catch (Exception e) {
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
  public void initialize(Configuration conf, ThriftClientPool<AsyncClient> clientPool) {
    probeRatio = conf.getDouble(SparrowConf.SAMPLE_RATIO, 
        SparrowConf.DEFAULT_SAMPLE_RATIO);
    this.clientPool = clientPool;
  }
  
  @Override
  public Collection<TaskPlacer.TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes, 
      Collection<TTaskSpec> tasks, TSchedulingPref schedulingPref) 
          throws IOException {
    LOG.debug(Logging.functionCall(appId, nodes, tasks));
    Map<InetSocketAddress, TResourceUsage> loads = Maps.newHashMap();
    
    // This latch decides how many nodes need to respond for us to make a decision.
    // Using a simple counter is okay for now, but eventually we will want to use
    // per-task information to decide when to return.
    int probesToLaunch = (int) Math.ceil(probeRatio * tasks.size());
    probesToLaunch = Math.min(probesToLaunch, nodes.size());
    LOG.debug("Launching " + probesToLaunch + " probes");

    // Right now we wait for all probes to return, in the future we might add a timeout
    CountDownLatch latch = new CountDownLatch(probesToLaunch);
    List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
    
    // Get a random subset of nodes by shuffling list
    Collections.shuffle(nodeList);
    nodeList = nodeList.subList(0, probesToLaunch);
    
    for (InetSocketAddress node : nodeList) {
      try {
        AsyncClient client = clientPool.borrowClient(node);
        ProbeCallback callback = new ProbeCallback(node, loads, latch, appId, requestId,
                                                   client);
        LOG.debug("Launching probe on node: " + node); 
        AUDIT_LOG.info(Logging.auditEventString("probe_launch", requestId,
                                                node.getAddress().getHostAddress()));
        client.getLoad(appId, requestId, callback);
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    AssignmentPolicy assigner = new ComparatorAssignmentPolicy(
        new TResources.CPUThenQueueComparator());
    Collection<TaskPlacementResponse> out = assigner.assignTasks(tasks, loads);

    return out;
  }
}
