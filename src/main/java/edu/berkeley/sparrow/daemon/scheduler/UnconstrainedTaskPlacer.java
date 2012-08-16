package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * A task placer for jobs whose tasks have no placement constraints.
 */
public class UnconstrainedTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(UnconstrainedTaskPlacer.class);
  protected static final Logger AUDIT_LOG = Logging.getAuditLogger(UnconstrainedTaskPlacer.class);

  /** Specifications for tasks that have not yet been launched. */
 List<TTaskLaunchSpec> unlaunchedTasks;
  
  private double probeRatio;

  UnconstrainedTaskPlacer(double probeRatio) {
    this.probeRatio = probeRatio;
    unlaunchedTasks = Collections.synchronizedList(new LinkedList<TTaskLaunchSpec>());
  }
  
  @Override
  public Map<InetSocketAddress, TEnqueueTaskReservationsRequest>
      getEnqueueTaskReservationsRequests(
          TSchedulingRequest schedulingRequest, String requestId,
          Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {
    LOG.debug(Logging.functionCall(schedulingRequest, requestId, nodes, schedulerAddress));
    
    int numTasks = schedulingRequest.getTasks().size();
    int probesToLaunch = (int) Math.ceil(probeRatio * numTasks);
    probesToLaunch = Math.min(probesToLaunch, nodes.size());
    LOG.debug("Creating " + probesToLaunch + " task reservations");
    
    // Get a random subset of nodes by shuffling list.
    List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
    Collections.shuffle(nodeList);
    if (nodeList.size() < probesToLaunch) {
      LOG.fatal("Cannot launch " + probesToLaunch + " reservations, because there are not enough "
                + "nodes. This use case is not currently supported.");
    }
    nodeList = nodeList.subList(0, probesToLaunch);
    
    TResourceVector estimatedResources = null;
    
    // Create a TTaskLaunchSpec for each task. Do this before launching the requests to enqueue
    // tasks, to ensure that the list is populated before any of the node monitors signal that
    // they are ready to launch a task.
    for (TTaskSpec task : schedulingRequest.getTasks()) {
      if (estimatedResources == null) {
        // Assume estimated resources for all tasks in the job is the same.
        estimatedResources = task.getEstimatedResources();
      }
      TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(),
                                                           task.bufferForMessage());
      unlaunchedTasks.add(taskLaunchSpec);
    }
    
    HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();
    
    for (InetSocketAddress node : nodeList) {
      TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(
          schedulingRequest.getApp(), schedulingRequest.getUser(), requestId, estimatedResources,
          schedulerAddress, 1);
      requests.put(node, request);
    }
    
    return requests;
  }

  @Override
  public TTaskLaunchSpec assignTask(THostPort nodeMonitorAddress) {
    synchronized(unlaunchedTasks) {
      if (unlaunchedTasks.isEmpty()) {
        LOG.debug("Not launching a task at " + nodeMonitorAddress.getHost() + ":" +
                  nodeMonitorAddress.getPort() + "; no remaining unlaunched tasks");
        return null;
      } else {
        TTaskLaunchSpec launchSpec = unlaunchedTasks.get(0);
        unlaunchedTasks.remove(0);
        LOG.debug("Assigning task " + launchSpec.getTaskId() + " to node monitor at " +
                  nodeMonitorAddress.getHost() + ":" + nodeMonitorAddress.getPort());
        return launchSpec;
      }
    }
  }
  
  @Override
  public boolean allResponsesReceived() {
    return unlaunchedTasks.isEmpty();
  }
}
