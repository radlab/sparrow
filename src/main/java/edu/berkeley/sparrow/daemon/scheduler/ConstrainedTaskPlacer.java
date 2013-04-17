package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * The constrained task placer may be used to place jobs with tasks that are some combination of
 * constrained and unconstrained, or jobs that just have constrained tasks.
 */
public class ConstrainedTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(ConstrainedTaskPlacer.class);

  /** Constrained tasks that have been launched. */
  private Set<TTaskLaunchSpec> launchedConstrainedTasks;

  /**
   * For each backend machine, the constrained tasks that can be launched there. Unconstrained
   * tasks will never appear in this mapping.  Constrained tasks for which
   * reservations were made on the backend are placed at the beginning of the list; any other
   * constrained tasks that can run on the backend are placed at the end of the list.
   */
  private Map<THostPort, List<TTaskLaunchSpec>> unlaunchedConstrainedTasks;

  /** Total number of outstanding reservations. */
  private int numOutstandingReservations;

  private double probeRatio;

  /** Id of the request associated with this task placer. */
  String requestId;

  /**
   * Tasks that are unconstrained, so can be placed on any node monitor, that have not yet been
   * launched.
   */
  List<TTaskLaunchSpec> unlaunchedUnconstrainedTasks;

  ConstrainedTaskPlacer(String requestId, double probeRatio){
    this.requestId = requestId;
    this.probeRatio = probeRatio;
    launchedConstrainedTasks = Collections.synchronizedSet(new HashSet<TTaskLaunchSpec>());
    unlaunchedConstrainedTasks = Maps.newConcurrentMap();
    numOutstandingReservations = 0;
    unlaunchedUnconstrainedTasks = Lists.newArrayList();
  }

  @Override
  public Map<InetSocketAddress, TEnqueueTaskReservationsRequest>
  getEnqueueTaskReservationsRequests(
      TSchedulingRequest schedulingRequest, String requestId,
      Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {
    LOG.debug(Logging.functionCall(schedulingRequest, requestId, nodes, schedulerAddress));

    // Tracks number of tasks to be enqueued at each node monitor.
    HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();

    // Create a mapping of node monitor hostnames to socket addresses, to use to map node
    // preferences to the node monitor InetSocketAddresss.
    // TODO: Perform this in the scheduler, and reuse the mapping across multiple
    //       ConstrainedTaskPlacers?
    HashMap<InetAddress, InetSocketAddress> addrToSocket = Maps.newHashMap();
    for (InetSocketAddress node : nodes) {
      if (addrToSocket.containsKey(node.getAddress())) {
        // TODO: Should we support this case?  Seems like it's only useful for testing.
        LOG.warn("Two node monitors at " + node.getAddress() + "; only one will be used for " +
                 "scheduling.");
      }
      addrToSocket.put(node.getAddress(), node);
    }

    /* Shuffle tasks, to ensure that we don't use the same set of machines each time a job is
     * submitted. */
    List<TTaskSpec> taskList = Lists.newArrayList(schedulingRequest.getTasks());
    Collections.shuffle(taskList);

    List<TTaskSpec> unconstrainedTasks = Lists.newArrayList();

    // We assume all tasks in a job have the same resource usage requirements.
    TResourceVector estimatedResources = taskList.get(0).getEstimatedResources();

    for (TTaskSpec task : taskList) {
      if (task.preference == null || task.preference.nodes == null ||
          task.preference.nodes.size() == 0) {
        unconstrainedTasks.add(task);
        continue;
      }

      List<InetSocketAddress> preferredNodes = taskPreferencesToSocketList(task, addrToSocket);

      TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(),
                                                           task.bufferForMessage());

      int numEnqueuedNodes = 0;
      for (InetSocketAddress addr : preferredNodes) {
        THostPort hostPort = new THostPort(addr.getAddress().getHostAddress(), addr.getPort());
        if (!unlaunchedConstrainedTasks.containsKey(hostPort)) {
          unlaunchedConstrainedTasks.put(
              hostPort, Collections.synchronizedList(new LinkedList<TTaskLaunchSpec>()));
        }

        if (numEnqueuedNodes < probeRatio) {
          // TODO: Try to select nodes that haven't already been used by another task.
          if (!requests.containsKey(addr)) {
            TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(
                schedulingRequest.getApp(), schedulingRequest.getUser(), requestId,
                estimatedResources, schedulerAddress, 1);
            requests.put(addr, request);
          } else {
            // IsSetNumTasks should already be true, because it was set when the request was
            // created.
            requests.get(addr).numTasks += 1;
          }
          numOutstandingReservations++;

          unlaunchedConstrainedTasks.get(hostPort).add(0, taskLaunchSpec);
          numEnqueuedNodes += 1;
        } else {
          // As an optimization, add the task at the end of the list of tasks on the node monitor,
          // so that if all the tasks responsible for probing the node monitor have been launched
          // when the node monitor is ready to launch a task, and this task hasn't been launched
          // yet, this task can use the node monitor. This means that there may be more entries in
          // nodeMonitorsToTasks than in nodeMonitorTaskCount for some addresses.
          unlaunchedConstrainedTasks.get(hostPort).add(taskLaunchSpec);
        }
      }

      if (numEnqueuedNodes < probeRatio) {
        // This case can occur when the probeRatio is greater than the size of preferred nodes.
        LOG.info("For request " + requestId + ",task " + task.taskId +
                 ", only created enqueueTaskRequests on " +
                 numEnqueuedNodes + " node monitors, which is fewer than specified by the " +
                 "probe ratio (" + probeRatio + ")");
      }
    }

    LOG.debug("Request " + requestId + ": created enqueue task reservation requests at " +
              requests.keySet().size() + " node monitors for constrained tasks. " +
              unconstrainedTasks.size() + " unconstrained tasks");

    if (unconstrainedTasks.size() > 0) {
      addRequestsForUnconstrainedTasks(
          unconstrainedTasks, requestId, schedulingRequest.getApp(), schedulingRequest.getUser(),
          estimatedResources, schedulerAddress, nodes, requests);
    }

    return requests;
  }

  /**
   * Adds enqueue task reservation requests for {@link unconstrainedTasks} to {@link requests}.
   * {@link nodeMonitors} specifies the running node monitors that enqueue task reservations may
   * be placed on.
   */
  private void addRequestsForUnconstrainedTasks(
      List<TTaskSpec> unconstrainedTasks, String requestId, String appId, TUserGroupInfo user,
      TResourceVector estimatedResources, THostPort schedulerAddress,
      Collection<InetSocketAddress> nodeMonitors,
      HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests) {
    /* Identify the node monitors that aren't already being used for the constrained tasks, and
     * place all of reservations on those nodes (to try to spread the reservations evenly
     * throughout the cluster). */
    List<InetSocketAddress> unusedNodeMonitors = Lists.newArrayList();
    for (InetSocketAddress nodeMonitor : nodeMonitors) {
      if (!requests.containsKey(nodeMonitor)) {
         unusedNodeMonitors.add(nodeMonitor);
      }
    }
    Collections.shuffle(unusedNodeMonitors);
    LOG.info("Request " + requestId + ": " + unusedNodeMonitors.size() +
             " node monitors that were unused by constrained tasks so may be used for " +
             "unconstrained tasks.");

    int reservationsToLaunch = (int) Math.ceil(probeRatio * unconstrainedTasks.size());
    int reservationsCreated = 0;

    for (InetSocketAddress nodeMonitor : unusedNodeMonitors) {
      if (requests.containsKey(nodeMonitor)) {
        LOG.error("Adding enqueueTaskReservations requests for unused node monitors, so they " +
                  "should not already have any enqueueTaskReservation requests.");
      }

      TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(
          appId, user, requestId, estimatedResources, schedulerAddress, 1);
      requests.put(nodeMonitor, request);
      reservationsCreated++;
      numOutstandingReservations += 1;

      if (reservationsCreated >= reservationsToLaunch) {
        break;
      }
    }

    if (reservationsCreated < reservationsToLaunch) {
      LOG.error("Trying to launch more reservations than there are nodes; this use case is not " +
                "currently supported");
    }

    for (TTaskSpec task : unconstrainedTasks) {
      TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(
          task.getTaskId(), task.bufferForMessage());
      unlaunchedUnconstrainedTasks.add(taskLaunchSpec);
    }

  }

  /**
   * Converts the preferences for the task (which contain host names) to a list of socket
   * addresses. We return the preferences as socket addresses because the addresses are used to
   * open a client for the node monitor (so need to be InetSocketAddreses).
   */
  private List<InetSocketAddress> taskPreferencesToSocketList(
      TTaskSpec task, HashMap<InetAddress, InetSocketAddress> addrToSocket) {
    // Preferred nodes for this task.
    List<InetSocketAddress> preferredNodes = Lists.newLinkedList();

    // Convert the preferences (which contain host names) to a list of socket addresses.
    Collections.shuffle(task.preference.nodes);
    for (String node : task.preference.nodes) {
      try {
       InetAddress addr = InetAddress.getByName(node);
       if (addrToSocket.containsKey(addr)) {
         preferredNodes.add(addrToSocket.get(addr));
       } else {
         LOG.warn("Placement constraint for unknown node " + node);
         LOG.warn("Node address: " + addr);
         String knownAddrs = "";
         for (InetAddress knownAddress: addrToSocket.keySet()) {
           knownAddrs += " " + knownAddress.getHostAddress();
         }
         LOG.warn("Know about: " + knownAddrs);
       }
      } catch (UnknownHostException e) {
        LOG.warn("Got placement constraint for unresolvable node " + node);
      }
    }

    return preferredNodes;
  }

  @Override
  public synchronized List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress) {
    numOutstandingReservations--;
    if (!unlaunchedConstrainedTasks.containsKey(nodeMonitorAddress)) {
      StringBuilder nodeMonitors = new StringBuilder();
      for (THostPort nodeMonitor: unlaunchedConstrainedTasks.keySet()) {
        if (nodeMonitors.length() > 0) {
          nodeMonitors.append(",");
        }
        nodeMonitors.append(nodeMonitor.toString());
      }
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
          ": Node monitor not in the set of node monitors where constrained tasks were " +
          "enqueued: " + nodeMonitors.toString() +
          "; attempting to launch an unconstrainted task).");

      List<TTaskLaunchSpec> unconstrainedTasks = getUnconstrainedTasks(nodeMonitorAddress);

      if (unconstrainedTasks.size() == 0) {
        LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
            ": no remaining unconstrained tasks).");
        return Lists.newArrayList();
      } else {
        return unconstrainedTasks;
      }
    }
    List<TTaskLaunchSpec> taskSpecs = unlaunchedConstrainedTasks.get(nodeMonitorAddress);
    TTaskLaunchSpec taskSpec = null;
    synchronized(taskSpecs) {
      // Try to find a task that hasn't been launched yet.
      do {
        if (!taskSpecs.isEmpty()) {
          taskSpec = taskSpecs.get(0);
          taskSpecs.remove(0);
        } else {
          taskSpec = null;
        }
      } while (taskSpec != null && this.launchedConstrainedTasks.contains(taskSpec));
    }

    if (taskSpec != null) {
      this.launchedConstrainedTasks.add(taskSpec);
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
          ": Assigning task.");
      return Lists.newArrayList(taskSpec);
    } else {
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
          ": Not assigning a constrained task (no remaining unlaunched tasks that prefer " +
          "this node).");
      return getUnconstrainedTasks(nodeMonitorAddress);
    }
  }

  private List<TTaskLaunchSpec> getUnconstrainedTasks(THostPort nodeMonitorAddress) {
    if (this.unlaunchedUnconstrainedTasks.size() == 0) {
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
                ": Not assigning a task (no remaining unconstrained unlaunched tasks)");
      return Lists.newArrayList();
    }
    TTaskLaunchSpec spec = unlaunchedUnconstrainedTasks.get(0);
    unlaunchedUnconstrainedTasks.remove(0);
    LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
              ": Assigning task " + spec.getTaskId());
    return Lists.newArrayList(spec);
  }

  @Override
  public synchronized boolean allResponsesReceived() {
    return numOutstandingReservations == 0;
  }
}
