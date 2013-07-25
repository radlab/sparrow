package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * A task placer for jobs whose tasks have no placement constraints.
 */
public class UnconstrainedTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(UnconstrainedTaskPlacer.class);

  /** Specifications for tasks that have not yet been launched. */
  List<TTaskLaunchSpec> unlaunchedTasks;

  /** Nodes with outstanding reservations. */
  Set<THostPort> nodeMonitorsOutstanding;

  /** Whether the remaining reservations have been cancelled. */
  boolean cancelled;

  /**
   * Id of the request associated with this task placer.
   */
  String requestId;

  private double probeRatio;

  UnconstrainedTaskPlacer(String requestId, double probeRatio) {
    this.requestId = requestId;
    this.probeRatio = probeRatio;
    unlaunchedTasks = new LinkedList<TTaskLaunchSpec>();
    nodeMonitorsOutstanding = new HashSet<THostPort>();
    cancelled = false;
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

    for (TTaskSpec task : schedulingRequest.getTasks()) {
      TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(),
                                                           task.bufferForMessage());
      unlaunchedTasks.add(taskLaunchSpec);
    }

    HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();

    StringBuilder debugString = new StringBuilder();
    for (InetSocketAddress node : nodeList) {
      debugString.append(node);
      debugString.append(";");
      nodeMonitorsOutstanding.add(
          new THostPort(node.getAddress().getHostAddress(), node.getPort()));
      TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(
          schedulingRequest.getApp(), schedulingRequest.getUser(), requestId, schedulerAddress, 1);
      requests.put(node, request);
    }

    LOG.debug("Request " + requestId + ": Launching enqueueReservation on " +
        nodeList.size() + " node monitors: " + debugString.toString());
    return requests;
  }

  @Override
  public List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress) {
    nodeMonitorsOutstanding.remove(nodeMonitorAddress);
    if (unlaunchedTasks.isEmpty()) {
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
               ": Not assigning a task (no remaining unlaunched tasks).");
      return Lists.newArrayList();
    } else {
      TTaskLaunchSpec launchSpec = unlaunchedTasks.get(0);
      unlaunchedTasks.remove(0);
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
                ": Assigning task");
      return Lists.newArrayList(launchSpec);
    }
  }

  @Override
  public boolean allTasksPlaced() {
    return unlaunchedTasks.isEmpty();
  }

  @Override
  public Set<THostPort> getOutstandingNodeMonitorsForCancellation() {
    if (!cancelled) {
      cancelled = true;
      return nodeMonitorsOutstanding;
    }
    return new HashSet<THostPort>();
  }
}
