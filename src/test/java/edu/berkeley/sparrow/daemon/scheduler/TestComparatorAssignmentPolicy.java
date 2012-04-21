package edu.berkeley.sparrow.daemon.scheduler;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class TestComparatorAssignmentPolicy {  
  @Test
  public void testBasicAssignment() {
    AssignmentPolicy policy = new ComparatorAssignmentPolicy(
        new TResources.MinCPUComparator());
    
    // Create two task requests
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    TTaskSpec task1 = new TTaskSpec();
    task1.taskID = "1";
    tasks.add(task1);
    
    TTaskSpec task2 = new TTaskSpec();
    task2.taskID = "2";
    tasks.add(task2);
    
    Map<InetSocketAddress, TResourceUsage> usage = Maps.newHashMap();
    
    // Create four nodes
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 1;
    usage.put(socket1, TResources.createResourceUsage(resource1, 0));
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 2;
    usage.put(socket2, TResources.createResourceUsage(resource2, 0));
    
    InetSocketAddress socket3 = new InetSocketAddress(3);
    TResourceVector resource3 = new TResourceVector();
    resource3.cores = 3;
    usage.put(socket3, TResources.createResourceUsage(resource3, 0));
    
    InetSocketAddress socket4 = new InetSocketAddress(4);
    TResourceVector resource4 = new TResourceVector();
    resource4.cores = 4;
    usage.put(socket4, TResources.createResourceUsage(resource4, 0));
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    
    assertEquals(2, out.size());
    // We expect tasks 1 and 2 to collectively be assigned to nodes 1 and 2. Since this 
    // allows for two possible assignments, we effectively check that we are in 
    // one of those cases.
    boolean seenTaskOne = false;
    boolean seenTaskTwo = false;
    boolean seenNodeOne = false;
    boolean seenNodeTwo = false;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(socket1)) { seenNodeOne = true; }
      if (resp.getNodeAddr().equals(socket2)) { seenNodeTwo = true; }
      if (resp.getTaskSpec().taskID.equals("1")) { seenTaskOne = true; }
      if (resp.getTaskSpec().taskID.equals("2")) { seenTaskTwo = true; }
    }
    
    assertTrue(seenTaskOne);
    assertTrue(seenTaskTwo);
    assertTrue(seenNodeOne);
    assertTrue(seenNodeTwo);
  }
  
  @Test
  public void testFewerMachinesThanNodes() {
    AssignmentPolicy policy = new ComparatorAssignmentPolicy(
        new TResources.MinCPUComparator());
    
    // Create three task requests
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    TTaskSpec task1 = new TTaskSpec();
    task1.taskID = "1";
    tasks.add(task1);
    
    TTaskSpec task2 = new TTaskSpec();
    task2.taskID = "2";
    tasks.add(task2);
    
    TTaskSpec task3 = new TTaskSpec();
    task3.taskID = "3";
    tasks.add(task3);
    
    Map<InetSocketAddress, TResourceUsage> usage = Maps.newHashMap();
    
    // Create two nodes
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 1;
    usage.put(socket1, TResources.createResourceUsage(resource1, 0));
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 2;
    usage.put(socket2, TResources.createResourceUsage(resource2, 0));
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    assertEquals(3, out.size());
    
    // We expect two tasks assigned to node 1 and one to node 2
    int assignedToNode1 = 0;
    int assignedToNode2 = 0;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(new InetSocketAddress(1))) { assignedToNode1++; }
      if (resp.getNodeAddr().equals(new InetSocketAddress(2))) { assignedToNode2++; }
    }
    
    assertEquals(2, assignedToNode1);
    assertEquals(1, assignedToNode2);
  }
  
  @Test
  public void testQueueLengths() {
    AssignmentPolicy policy = new ComparatorAssignmentPolicy(
        new TResources.MinQueueComparator());
    
    // Create two task requests
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    TTaskSpec task1 = new TTaskSpec();
    task1.taskID = "1";
    tasks.add(task1);
    
    TTaskSpec task2 = new TTaskSpec();
    task2.taskID = "2";
    tasks.add(task2);
    
    Map<InetSocketAddress, TResourceUsage> usage = Maps.newHashMap();
    
    // Create four nodes, one free, three back-logged
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 1;
    usage.put(socket1, TResources.createResourceUsage(resource1, 0));
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 4;
    usage.put(socket2, TResources.createResourceUsage(resource2, 4));
    
    InetSocketAddress socket3 = new InetSocketAddress(3);
    TResourceVector resource3 = new TResourceVector();
    resource3.cores = 4;
    usage.put(socket3, TResources.createResourceUsage(resource3, 2));
    
    InetSocketAddress socket4 = new InetSocketAddress(4);
    TResourceVector resource4 = new TResourceVector();
    resource4.cores = 4;
    usage.put(socket4, TResources.createResourceUsage(resource4, 1));
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    
    assertEquals(2, out.size());
    // We expect tasks 1 and 2 to collectively be assigned to nodes 1 and 4. Since this 
    // allows for two possible assignments, we effectively check that we are in 
    // one of those cases.
    boolean seenTaskOne = false;
    boolean seenTaskTwo = false;
    boolean seenNodeOne = false;
    boolean seenNodeFour = false;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(socket1)) { seenNodeOne = true; }
      if (resp.getNodeAddr().equals(socket4)) { seenNodeFour = true; }
      if (resp.getTaskSpec().taskID.equals("1")) { seenTaskOne = true; }
      if (resp.getTaskSpec().taskID.equals("2")) { seenTaskTwo = true; }
    }
    
    assertTrue(seenTaskOne);
    assertTrue(seenTaskTwo);
    assertTrue(seenNodeOne);
    assertTrue(seenNodeFour);
  }
}
