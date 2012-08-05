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
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class ConstrainedTaskAssignmentPolicy implements AssignmentPolicy {
  private final static Logger LOG = 
      Logger.getLogger(ConstrainedTaskAssignmentPolicy.class);
  
  private AssignmentPolicy delegate;
  
  public ConstrainedTaskAssignmentPolicy(AssignmentPolicy delegate) {
    this.delegate = delegate;
  }
  
  /**
   * Task assigner that takes into account tasks which may have constraints. This 
   * implements a heuristic for finding the optimal assignment as follows:
   * 
   * 1) For each task which has constraints:
   *    - Assign that task to the optimal choice amongst its feasible set according
   *      to the passed in policy.
   * 2) For all remaining tasks:
   *    - Assign to nodes based on the passed in policy.
   */
  public Collection<TaskPlacementResponse> assignTasks(
      Collection<TTaskSpec> tasks, Map<InetSocketAddress, TResourceUsage> nodes) {
    HashMap<InetAddress, InetSocketAddress> addrToSocket = Maps.newHashMap();
    
    for (InetSocketAddress node: nodes.keySet()) {
      addrToSocket.put(node.getAddress(), node);
    }
    Set<TaskPlacementResponse> out = Sets.newHashSet();
    List<TTaskSpec> unconstrainedTasks = Lists.newLinkedList();
    ArrayList<TTaskSpec> taskShuffled = Lists.newArrayList(tasks);
    Collections.shuffle(taskShuffled);
    for (TTaskSpec task : tasks) {
      List<InetSocketAddress> interests = Lists.newLinkedList();
      if (task.preference != null && task.preference.nodes != null) {
        for (String node : task.preference.nodes) {
          try {
            InetAddress addr = InetAddress.getByName(node);
            if (addrToSocket.containsKey(addr)) {
              interests.add(addrToSocket.get(addr));
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
        // TODO: this should really return something saying the constraints are unsatisfiable
        if (choices.size() == 0) LOG.fatal("No information pertaining to task: " + task); 
        out.addAll(delegate.assignTasks(Lists.newArrayList(task), choices));
        
        } else { // We are not constrained
        unconstrainedTasks.add(task);
      }
    }

    AssignmentPolicy delegate = new WaterLevelAssignmentPolicy();
    out.addAll(delegate.assignTasks(unconstrainedTasks, nodes));
    return out;
  }
}
