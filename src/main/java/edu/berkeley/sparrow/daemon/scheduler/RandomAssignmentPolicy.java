package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.Lists;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class RandomAssignmentPolicy implements AssignmentPolicy {

  @Override
  public Collection<TaskPlacementResponse> assignTasks(
      Collection<TTaskSpec> tasks, Map<InetSocketAddress, TResourceUsage> nodes) {
    Collection<TaskPlacementResponse> out = new HashSet<TaskPlacementResponse>();
    ArrayList<InetSocketAddress> orderedNodes = Lists.newArrayList(nodes.keySet());
    Collections.shuffle(orderedNodes);
    
    int i = 0;
    for (TTaskSpec task : tasks) {
      InetSocketAddress addr = orderedNodes.get(i++ % nodes.size());
      TaskPlacementResponse response = new TaskPlacementResponse(task, addr);
      out.add(response);
    }
    return out;
  }

}
