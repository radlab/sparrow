package edu.berkeley.sparrow.daemon.scheduler;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TPlacementPreference;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class TestConstrainedTaskAssignmentPolicy {
  @Test
  public void testThreeNodesAssignment() {
    AssignmentPolicy policy = new ConstrainedTaskAssignmentPolicy();
    
    // Create three task requests
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    
    TTaskSpec task1 = new TTaskSpec();
    task1.taskID = "1";
    task1.preference = new TPlacementPreference();
    task1.preference.nodes = Arrays.asList("1.1.1.1", "2.2.2.2", "3.3.3.3");
    task1.estimatedResources = TResources.createResourceVector(0, 1);
    tasks.add(task1);
    
    TTaskSpec task2 = new TTaskSpec();
    task2.taskID = "2";
    task2.preference = new TPlacementPreference();
    task2.preference.nodes = Arrays.asList("1.1.1.1", "2.2.2.2", "3.3.3.3");
    task2.estimatedResources = TResources.createResourceVector(0, 1);
    tasks.add(task2);
    
    TTaskSpec task3 = new TTaskSpec();
    task3.taskID = "3";
    task3.preference = new TPlacementPreference();
    task3.preference.nodes = Arrays.asList("1.1.1.1", "2.2.2.2", "3.3.3.3");
    task3.estimatedResources = TResources.createResourceVector(0, 1);
    tasks.add(task3);
    
    Map<InetSocketAddress, TResourceUsage> usage = Maps.newHashMap();
    
    // Create three nodes
    InetSocketAddress socket1 = new InetSocketAddress("1.1.1.1", 1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 4;
    usage.put(socket1, TResources.createResourceUsage(resource1, 0));
    
    InetSocketAddress socket2 = new InetSocketAddress("2.2.2.2", 1);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 4;
    usage.put(socket2, TResources.createResourceUsage(resource2, 0));
    
    InetSocketAddress socket3 = new InetSocketAddress("3.3.3.3", 1);
    TResourceVector resource3 = new TResourceVector();
    resource3.cores = 4;
    usage.put(socket3, TResources.createResourceUsage(resource3, 0));
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    
    int assignedToNode1 = 0;
    int assignedToNode2 = 0;
    int assignedToNode3 = 0;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(socket1)) { assignedToNode1++; }
      if (resp.getNodeAddr().equals(socket2)) { assignedToNode2++; }
      if (resp.getNodeAddr().equals(socket3)) { assignedToNode3++; }
    }
    
    assertEquals(1, assignedToNode1);
    assertEquals(1, assignedToNode2);
    assertEquals(1, assignedToNode3);
  }
}
