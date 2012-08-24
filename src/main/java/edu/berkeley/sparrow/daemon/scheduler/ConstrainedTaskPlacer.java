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

public class ConstrainedTaskPlacer implements TaskPlacer {
  private static final Logger LOG = Logger.getLogger(ConstrainedTaskPlacer.class);
  protected static final Logger AUDIT_LOG = Logging.getAuditLogger(ConstrainedTaskPlacer.class);
  
  /** Tasks that have been launched. */
  private Set<TTaskLaunchSpec> launchedTasks;
  
  /** Total number of tasks in the job. */
  private int numTasks;
  
  private double probeRatio; 
  
  /** Id of the request associated with this task placer. */
  String requestId;
  
  /**
   * For each backend machine, the tasks that can be launched there. Tasks for which reservations
   * were made on the backend are placed at the beginning of the list; any other tasks that can
   * run on the backend are placed at the end of the list.
   */
  private Map<InetSocketAddress, List<TTaskLaunchSpec>> nodeMonitorsToTasks;

  ConstrainedTaskPlacer(String requestId, double probeRatio){
    this.requestId = requestId;
    this.probeRatio = probeRatio;
    launchedTasks = Collections.synchronizedSet(new HashSet<TTaskLaunchSpec>());
    nodeMonitorsToTasks = Maps.newConcurrentMap();
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
    
    // Shuffle tasks, to ensure that we don't use the same set of machines each time a job is
    // submitted.
    List<TTaskSpec> taskList = Lists.newArrayList(schedulingRequest.getTasks());
    Collections.shuffle(taskList);
    
    numTasks = taskList.size();
    
    // We assume all tasks in a job have the same resource usage requirements.
    TResourceVector estimatedResources = taskList.get(0).getEstimatedResources();
        
    for (TTaskSpec task : taskList) {
      if (task.preference == null || task.preference.nodes == null) {
        // TODO: Do we ever need to support this case?
        LOG.fatal("ConstrainedTaskPlacer excepts to receive only constrained tasks; received " +
                  "a mix of constrained and unconstrained tasks.");
      }
      
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
      
      TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(),
                                                           task.bufferForMessage());
      
      int numEnqueuedNodes = 0;
      for (InetSocketAddress addr : preferredNodes) {
        if (!nodeMonitorsToTasks.containsKey(addr)) {
          nodeMonitorsToTasks.put(
              addr, Collections.synchronizedList(new LinkedList<TTaskLaunchSpec>()));
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
          
          nodeMonitorsToTasks.get(addr).add(0, taskLaunchSpec);
          numEnqueuedNodes += 1;
        } else {
          // As an optimization, add the task at the end of the list of tasks on the node monitor,
          // so that if all the tasks responsible for probing the node monitor have been launched
          // when the node monitor is ready to launch a task, and this task hasn't been launched
          // yet, this task can use the node monitor. This means that there may be more entries in
          // nodeMonitorsToTasks than in nodeMonitorTaskCount for some addresses.
          nodeMonitorsToTasks.get(addr).add(taskLaunchSpec);
        }
      }
      
      if (numEnqueuedNodes < probeRatio) {
        LOG.fatal("Only created requests for " + numEnqueuedNodes + " (expected to create " +
                  probeRatio + " requests).");
      }
    }
    
    return requests;
  }

  @Override
  public List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress) {
    InetSocketAddress nodeMonitorSocketAddress = new InetSocketAddress(
        nodeMonitorAddress.getHost(), nodeMonitorAddress.getPort());
    if (!nodeMonitorsToTasks.containsKey(nodeMonitorSocketAddress)) {
      LOG.error("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
          ": Not assigning a task (node monitor not in the set of node monitors where tasks " +
          "were enqueued).");
      return Lists.newArrayList();
    }
    List<TTaskLaunchSpec> taskSpecs = nodeMonitorsToTasks.get(nodeMonitorSocketAddress);
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
      } while (taskSpec != null && this.launchedTasks.contains(taskSpec));
    }
      
    if (taskSpec != null) {
      this.launchedTasks.add(taskSpec);
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
          ": Assigning task.");
      return Lists.newArrayList(taskSpec);
    } else {
      LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() +
          ": Not assigning a task (no remaining unlaunched tasks).");
      return Lists.newArrayList();
    }
  }

  @Override
  public boolean allResponsesReceived() {
    return numTasks == launchedTasks.size();
  }
}
