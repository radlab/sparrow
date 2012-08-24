package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
 
  /**
   * Number of outstanding reservations. Used to determine when all reservations have been
   * responded to.
   */
  AtomicInteger numOutstandingReservations;
  
  /**
   * Id of the request associated with this task placer.
   */
  String requestId;
  
  private double probeRatio;

  UnconstrainedTaskPlacer(String requestId, double probeRatio) {
    this.requestId = requestId;
    this.probeRatio = probeRatio;
    unlaunchedTasks = Collections.synchronizedList(new LinkedList<TTaskLaunchSpec>());
    this.numOutstandingReservations = new AtomicInteger(0);
  }
  
  @Override
  public Map<InetSocketAddress, TEnqueueTaskReservationsRequest>
      getEnqueueTaskReservationsRequests(
          TSchedulingRequest schedulingRequest, String requestId,
          Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {
    LOG.debug(Logging.functionCall(schedulingRequest, requestId, nodes, schedulerAddress));
    
    int numTasks = schedulingRequest.getTasks().size();
    int reservationsToLaunch = (int) Math.ceil(probeRatio * numTasks);
    reservationsToLaunch = Math.min(reservationsToLaunch, nodes.size());
    LOG.debug("Request " + requestId + ": Creating " + reservationsToLaunch +
              " task reservations");
    
    // Get a random subset of nodes by shuffling list.
    List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
    Collections.shuffle(nodeList);
    if (nodeList.size() < reservationsToLaunch) {
      LOG.fatal("Request " + requestId + ": Cannot launch " + reservationsToLaunch +
                " reservations, because there are not enough nodes. This use case is not " +
                "currently supported.");
    }
    nodeList = nodeList.subList(0, reservationsToLaunch);
    
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
    
    numOutstandingReservations.set(requests.size());
    
    return requests;
  }

  @Override
  public List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress) {
    numOutstandingReservations.decrementAndGet();
    synchronized(unlaunchedTasks) {
      if (unlaunchedTasks.isEmpty()) {
        LOG.debug("Request " + requestId + ": Not launching a task at " +
                  nodeMonitorAddress.getHost() + ":" + nodeMonitorAddress.getPort() +
                  "; no remaining unlaunched tasks");
        return Lists.newArrayList();
      } else {
        TTaskLaunchSpec launchSpec = unlaunchedTasks.get(0);
        unlaunchedTasks.remove(0);
        LOG.debug("Request " + requestId + ": Assigning task " + launchSpec.getTaskId() +
                  " to node monitor at " + nodeMonitorAddress.getHost() + ":" +
                  nodeMonitorAddress.getPort());
        return Lists.newArrayList(launchSpec);
      }
    }
  }
  
  @Override
  public boolean allResponsesReceived() {
    return numOutstandingReservations.get() == 0;
  }
}
