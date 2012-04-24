package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class WaterLevelAssignmentPolicy implements AssignmentPolicy {
  private static int getNodeLevel(TResourceUsage u) {
    return u.getQueueLength() + u.getResources().cores;
  }
  
  private class EntryWrapper implements Comparable<EntryWrapper> {
    Entry<InetSocketAddress, TResourceUsage> entry;
    EntryWrapper(Entry<InetSocketAddress, TResourceUsage> in) {
      this.entry = in;
    }
    
    @Override
    public boolean equals(Object other) {
      return this == other;
    }
    
    @Override
    public int compareTo(EntryWrapper other) {
      int wl1 = getNodeLevel(this.entry.getValue());
      int wl2 = getNodeLevel(other.entry.getValue());
      int res = new Integer(wl1).compareTo(new Integer(wl2));
      if (res == 0) {
        return this.hashCode() - other.hashCode();
      }
      return res;
    }
    
    public TResourceUsage getResUsage() { return entry.getValue(); }
    
    public InetSocketAddress getSocket() { return entry.getKey(); }
  }
  
  @Override
  /**
   * Note that this will actually modify in-place the TResourceUsage of the nodes as
   * tasks are assigned.
   */
  public Collection<TaskPlacementResponse> assignTasks(
      Collection<TTaskSpec> tasks, Map<InetSocketAddress, TResourceUsage> nodes) {
    SortedSet<EntryWrapper> nodeSet = new TreeSet<EntryWrapper>();
    for (Entry<InetSocketAddress, TResourceUsage> entry : nodes.entrySet()) {
      nodeSet.add(new EntryWrapper(entry));
    }
    ArrayList<TaskPlacementResponse> out = Lists.newArrayList();
    Set<EntryWrapper> waiting = Sets.newHashSet();
    
    int currLevel = getNodeLevel(nodeSet.first().getResUsage());
    for (TTaskSpec task : tasks) {
      EntryWrapper entry = nodeSet.first();
      nodeSet.remove(entry);

      int nodeLevel = getNodeLevel(entry.getResUsage());

      TResources.addTo(entry.getResUsage().getResources(), task.estimatedResources);
      if (nodeSet.size() > 0 && 
          getNodeLevel(entry.getResUsage()) >= getNodeLevel(nodeSet.first().getResUsage())) {
        waiting.add(entry);
      } else {
        nodeSet.add(entry);
      }
      
      if (nodeLevel > currLevel) {
        currLevel = nodeLevel;
        nodeSet.addAll(waiting);
        waiting.removeAll(waiting);
      }
      out.add(new TaskPlacementResponse(task, entry.getSocket()));
    }
    return out;
  }

}
