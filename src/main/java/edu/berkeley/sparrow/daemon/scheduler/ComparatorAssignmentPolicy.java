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
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * A class that performs assignment based on an arbitrary {@link Comparator} which
 * sorts nodes in some order. Tasks are assigned to nodes in the lowest to highest
 * order, with tasks potentially wrapping around if more tasks exist than nodes.
 */
public class ComparatorAssignmentPolicy implements AssignmentPolicy {
  
  private Comparator<TResourceUsage> comp;

  public ComparatorAssignmentPolicy(Comparator<TResourceUsage> comp) {
    this.comp = comp;
  }
  

  /** Sorts nodes according to comparator order, then assigns tasks to the nodes
   *  with the lowest-to-highest value.  */
  public Collection<TaskPlacementResponse> assignTasks(Collection<TTaskSpec> tasks,
      Map<InetSocketAddress, TResourceUsage> nodes) {
    List<Entry<InetSocketAddress, TResourceUsage>> results = 
        new ArrayList<Entry<InetSocketAddress, TResourceUsage>>(nodes.entrySet());
    Collections.sort(results, TResources.makeEntryComparator(comp));
    
    ArrayList<TaskPlacementResponse> out = new ArrayList<TaskPlacementResponse>();
    
    int i = 0;
    for (TTaskSpec task : tasks) {
      Entry<InetSocketAddress, TResourceUsage> entry = results.get(i++ % results.size());
      
      TaskPlacementResponse place = new TaskPlacementResponse(
          task, entry.getKey());
      out.add(place);
    }
    return out;
  }
  
}
