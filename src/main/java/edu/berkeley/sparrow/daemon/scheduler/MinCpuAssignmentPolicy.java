package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class MinCpuAssignmentPolicy implements AssignmentPolicy {
  /**
   * A comparator for (node, resource vector) pairs, based on CPU usage.
   */
  public static class CPUNodeComparator implements Comparator<
      Entry<InetSocketAddress, TResourceVector>> {
    @Override
    public int compare(Entry<InetSocketAddress, TResourceVector> e1, 
        Entry<InetSocketAddress, TResourceVector> e2) {
      int c1 = e1.getValue().cores;
      int c2 = e2.getValue().cores;
      if (c1 > c2) { return 1; }
      if (c1 < c2) { return -1; }
      return 0;
    }
  }
  
  /**
   * Sorts nodes by CPU usage, then assigns tasks to the nodes with most lightly
   * loaded CPU. Will only assign more than one task to each node if there are fewer
   * nodes than tasks.
   */
  @Override
  public Collection<TaskPlacementResponse> assignTasks(Collection<TTaskSpec> tasks,
      Map<InetSocketAddress, TResourceVector> nodes) {
    List<Entry<InetSocketAddress, TResourceVector>> results = 
        new ArrayList<Entry<InetSocketAddress, TResourceVector>>(nodes.entrySet());
    Collections.sort(results, new CPUNodeComparator());
    
    ArrayList<TaskPlacementResponse> out = new ArrayList<TaskPlacementResponse>();
    
    int i = 0;
    for (TTaskSpec task : tasks) {
      Entry<InetSocketAddress, TResourceVector> entry = results.get(i++ % results.size());
      
      TaskPlacementResponse place = new TaskPlacementResponse(
          task, entry.getKey());
      out.add(place);
    }
    return out;
  }

}
