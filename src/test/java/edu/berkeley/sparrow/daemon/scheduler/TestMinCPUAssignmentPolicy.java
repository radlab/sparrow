package edu.berkeley.sparrow.daemon.scheduler;
import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Test;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TResourceVector;


public class TestMinCPUAssignmentPolicy {  
  @Test
  public void testBasicAssignment() {
    AssignmentPolicy policy = new MinCPUAssignmentPolicy();
    
    // Create two task requests
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    TTaskSpec task1 = new TTaskSpec();
    task1.taskID = "1";
    tasks.add(task1);
    
    TTaskSpec task2 = new TTaskSpec();
    task2.taskID = "2";
    tasks.add(task2);
    
    Map<InetSocketAddress, TResourceVector> usage = 
        new HashMap<InetSocketAddress, TResourceVector>();
    
    // Create four nodes
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 1;
    usage.put(socket1, resource1);
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 2;
    usage.put(socket2, resource2);
    
    InetSocketAddress socket3 = new InetSocketAddress(3);
    TResourceVector resource3 = new TResourceVector();
    resource3.cores = 3;
    usage.put(socket3, resource3);
    
    InetSocketAddress socket4 = new InetSocketAddress(4);
    TResourceVector resource4 = new TResourceVector();
    resource4.cores = 4;
    usage.put(socket4, resource4);
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    
    assertEquals(2, out.size());
    // We expect tasks 1 and 2 to be assigned to nodes 1 and 2
    boolean seenTaskOne = false;
    boolean seenTaskTwo = false;
    boolean seenNodeOne = false;
    boolean seenNodeTwo = false;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(new InetSocketAddress(1))) { seenNodeOne = true; }
      if (resp.getNodeAddr().equals(new InetSocketAddress(2))) { seenNodeTwo = true; }
      if (resp.getTaskSpec().taskID == "1") { seenTaskOne = true; }
      if (resp.getTaskSpec().taskID == "2") { seenTaskTwo = true; }
    }
    
    assertTrue(seenTaskOne);
    assertTrue(seenTaskTwo);
    assertTrue(seenNodeOne);
    assertTrue(seenNodeTwo);
  }
  
  @Test
  public void testFewerMachinesThanNodes() {
    AssignmentPolicy policy = new MinCPUAssignmentPolicy();
    
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
    
    Map<InetSocketAddress, TResourceVector> usage = 
        new HashMap<InetSocketAddress, TResourceVector>();
    
    // Create two nodes
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 1;
    usage.put(socket1, resource1);
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 2;
    usage.put(socket2, resource2);
    
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
}
