package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class ConstrainedTaskAssignmentPolicy implements AssignmentPolicy {

  private final static Logger LOG = 
      Logger.getLogger(ConstrainedTaskAssignmentPolicy.class);
  
  /**
   * Task assigner that takes into account tasks which may have constraints. This 
   * implements a heuristic for finding the optimal assignment as follows:
   * 
   * 1) For each task which has constraints:
   *    - Choose the least loaded node which is in the constraint set for the task
   *    - Assign the task to that node an increment the node's load accordingly
   * 2) For all remaining tasks:
   *    - Assign to nodes based on {@link MinCpuAssignmentPolicy}.
   */
  public Collection<TaskPlacementResponse> assignTasks(
      Collection<TTaskSpec> tasks, Map<InetSocketAddress, TResourceUsage> nodes) {
    HashMap<InetAddress, InetSocketAddress> addrToSocket = Maps.newHashMap();
    
    for (InetSocketAddress node: nodes.keySet()) {
      addrToSocket.put(node.getAddress(), node);
    }
    Set<TaskPlacementResponse> out = Sets.newHashSet();
    List<TTaskSpec> unconstrainedTasks = Lists.newLinkedList();
    for (TTaskSpec task : tasks) {
      List<InetSocketAddress> interests = Lists.newLinkedList();
      if (task.preference != null && task.preference.nodes != null) {
        for (String node : task.preference.nodes) {
          try {
            InetAddress addr = InetAddress.getByName(node);
            if (addrToSocket.containsKey(addr)) {
              interests.add(addrToSocket.get(addr));
            } else {
              LOG.warn("Got placement constraint for unknown node " + node);
            }
          } catch (UnknownHostException e) {
            LOG.warn("Got placement constraint for unresolvable node " + node);
          }
        }
      }
      // We have constraints
      if (interests.size() > 0) {
        HashMap<InetSocketAddress, TResourceUsage> choices = Maps.newHashMap();
        for (InetSocketAddress node : interests) {
          if (nodes.containsKey(node)) {
            choices.put(node, nodes.get(node));
          }
        }
        List<Entry<InetSocketAddress, TResourceUsage>> results = 
            new ArrayList<Entry<InetSocketAddress, TResourceUsage>>(choices.entrySet());
        Collections.sort(results, TResources.makeEntryComparator(
            new TResources.CPUThenQueueComparator()));
        
        // TODO: this should really return something saying the constraints are unsatisfiable
        if (results.size() == 0) LOG.fatal("No information pertaining to task: " + task); 
        
        Entry<InetSocketAddress, TResourceUsage> entry = results.get(0);
        out.add(new TaskPlacementResponse(task, entry.getKey()));
        TResources.addTo(entry.getValue().getResources(), task.estimatedResources);
        
        } else { // We are not constrained
        unconstrainedTasks.add(task);
      }
    }
    /**
     * NOTE,TODO: This may have undesired consequences. One issue is that this policy
     * ranks all nodes in terms of (CPU, queue length) ordering. That means a node with
     * a positive queue length might be assigned over one which has an extra free queue
     * (but does have a queue which has already been assigned). This might be a place
     * to look if we are seeing below-expected performance.
     */
    AssignmentPolicy delegate = new ComparatorAssignmentPolicy(
        new TResources.CPUThenQueueComparator());
    out.addAll(delegate.assignTasks(unconstrainedTasks, nodes));
    return out;
  }
}
