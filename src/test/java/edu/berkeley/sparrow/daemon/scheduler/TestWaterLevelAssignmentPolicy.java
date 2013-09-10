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

public class TestWaterLevelAssignmentPolicy {

  /**
   * Test an assignment where all nodes are back-logged and queue length
   * is the only deciding factor.
   */
  @Test
  public void testQueueOnlyAssignment() {
    AssignmentPolicy policy = new WaterLevelAssignmentPolicy();
    
    Map<InetSocketAddress, TResourceUsage> usage = Maps.newHashMap();

    // Create four nodes
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 4;
    usage.put(socket1, TResources.createResourceUsage(resource1, 65));
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 4;
    usage.put(socket2, TResources.createResourceUsage(resource2, 63));
    
    InetSocketAddress socket3 = new InetSocketAddress(3);
    TResourceVector resource3 = new TResourceVector();
    resource3.cores = 4;
    usage.put(socket3, TResources.createResourceUsage(resource3, 98));
    
    InetSocketAddress socket4 = new InetSocketAddress(4);
    TResourceVector resource4 = new TResourceVector();
    resource4.cores = 4;
    usage.put(socket4, TResources.createResourceUsage(resource4, 99));
    
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    // Create five task requests
    TTaskSpec task1 = new TTaskSpec();
    task1.taskId = "1";
    task1.setEstimatedResources(TResources.createResourceVector(0, 1));
    tasks.add(task1);
    TTaskSpec task2 = new TTaskSpec();
    task2.taskId = "2";
    tasks.add(task2);
    task2.setEstimatedResources(TResources.createResourceVector(0, 1));
    TTaskSpec task3 = new TTaskSpec();
    task3.taskId = "3";
    tasks.add(task3);
    task3.setEstimatedResources(TResources.createResourceVector(0, 1));
    TTaskSpec task4 = new TTaskSpec();
    task4.taskId = "4";
    tasks.add(task4);
    task4.setEstimatedResources(TResources.createResourceVector(0, 1));
    TTaskSpec task5 = new TTaskSpec();
    task5.taskId = "5";
    tasks.add(task5);
    task5.setEstimatedResources(TResources.createResourceVector(0, 1));
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    assertEquals(5, out.size());
    
    int nodeOneCount = 0;
    int nodeTwoCount = 0;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(socket1)) { nodeOneCount++; }
      if (resp.getNodeAddr().equals(socket2)) { nodeTwoCount++; }
    }
    assertTrue(nodeTwoCount == 3 || nodeOneCount == 4);
    assertTrue(nodeOneCount == 1 || nodeOneCount == 2);
  }
  
  /**
   * Test a common case where you are avoiding a few bad queues.
   */
  @Test
  public void testLowUtilizationAssignment() {
    AssignmentPolicy policy = new WaterLevelAssignmentPolicy();
    
    Map<InetSocketAddress, TResourceUsage> usage = Maps.newHashMap();

    // Create four nodes
    InetSocketAddress socket1 = new InetSocketAddress(1);
    TResourceVector resource1 = new TResourceVector();
    resource1.cores = 1;
    usage.put(socket1, TResources.createResourceUsage(resource1, 0));
    
    InetSocketAddress socket2 = new InetSocketAddress(2);
    TResourceVector resource2 = new TResourceVector();
    resource2.cores = 0;
    usage.put(socket2, TResources.createResourceUsage(resource2, 0));
    
    InetSocketAddress socket3 = new InetSocketAddress(3);
    TResourceVector resource3 = new TResourceVector();
    resource3.cores = 2;
    usage.put(socket3, TResources.createResourceUsage(resource3, 0));
    
    InetSocketAddress socket4 = new InetSocketAddress(4);
    TResourceVector resource4 = new TResourceVector();
    resource4.cores = 0;
    usage.put(socket4, TResources.createResourceUsage(resource4, 0));
    
    Collection<TTaskSpec> tasks = new LinkedList<TTaskSpec>();
    // Create three task requests
    TTaskSpec task1 = new TTaskSpec();
    task1.taskId = "1";
    task1.setEstimatedResources(TResources.createResourceVector(0, 1));
    tasks.add(task1);
    TTaskSpec task2 = new TTaskSpec();
    task2.taskId = "2";
    tasks.add(task2);
    task2.setEstimatedResources(TResources.createResourceVector(0, 1));
    TTaskSpec task3 = new TTaskSpec();
    task3.taskId = "3";
    tasks.add(task3);
    task3.setEstimatedResources(TResources.createResourceVector(0, 1));
    
    Collection<TaskPlacementResponse> out = policy.assignTasks(tasks, usage);
    assertEquals(3, out.size());
    
    int nodeOneCount = 0;
    int nodeTwoCount = 0;
    int nodeThreeCount = 0;
    int nodeFourCount = 0;
    
    for (TaskPlacementResponse resp : out) {
      if (resp.getNodeAddr().equals(socket1)) { nodeOneCount++; }
      if (resp.getNodeAddr().equals(socket2)) { nodeTwoCount++; }
      if (resp.getNodeAddr().equals(socket3)) { nodeThreeCount++; }
      if (resp.getNodeAddr().equals(socket4)) { nodeFourCount++; }
    }
    assertEquals(1, nodeOneCount);
    assertEquals(1, nodeTwoCount);
    assertEquals(0, nodeThreeCount);
    assertEquals(1, nodeFourCount);
  }
}
