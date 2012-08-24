package edu.berkeley.sparrow.daemon.scheduler;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TPlacementPreference;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestConstrainedTaskPlacer {
  private static final String APP_ID = "test app";
  private static final String USER = "user";
  private static final String GROUP = "group";
  private static final String REQUEST_ID = "request id";
  private static final int MEMORY = 10;
  private static final int CORES = 1;
  private static final THostPort SCHEDULER_ADDRESS = new THostPort("localhost", 12345);
  
  @Before
  public void setUp() {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
  }

  /**
   * Creates a scheduling request with a single task that has 3 preferred nodes, and ensures
   * that repeated calls to getEnqueueTaskReservationsRequests always returns requests for one
   * of those three nodes.
   */
  @Test
  public void testGetEnqueueTaskReservationsRequestsSimple() {
    ConstrainedTaskPlacer taskPlacer = new ConstrainedTaskPlacer(REQUEST_ID, 2);
    
    Set<InetSocketAddress> preferredNodes = new HashSet<InetSocketAddress>();
    preferredNodes.add(new InetSocketAddress("127.0.0.1", 22));
    preferredNodes.add(new InetSocketAddress("123.4.5.6", 20000));
    preferredNodes.add(new InetSocketAddress("7.0.0.9", 45));
    
    // Create a single task with three placement preferences.
    ByteBuffer message = ByteBuffer.allocate(1);
    TPlacementPreference placementPreference = new TPlacementPreference();
    for (InetSocketAddress address : preferredNodes) {
      placementPreference.addToNodes(address.getAddress().getHostAddress());
    }
    TResourceVector estimatedResources = new TResourceVector(MEMORY, CORES);
    TTaskSpec task = new TTaskSpec("test task", placementPreference, estimatedResources, message);
    
    // Create the scheduling request.
    List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
    tasks.add(task);
    TUserGroupInfo user = new TUserGroupInfo(USER, GROUP);
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);
    
    // Create a set of backends, with all of the preferred nodes and some extra nodes too.
    List<InetSocketAddress> backendNodes = new ArrayList<InetSocketAddress>();
    backendNodes.add(new InetSocketAddress("3,4,5,6", 174));
    for (InetSocketAddress node : preferredNodes) {
      backendNodes.add(node);
    }
    backendNodes.add(new InetSocketAddress("234.5.6.7", 22));
    backendNodes.add(new InetSocketAddress("9.8.7.6", 1));
    
    final int NUM_ITERATIONS = 100;
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, backendNodes,
                                                        SCHEDULER_ADDRESS);
      // Sanity check the list of requests.
      int totalEnqueuedTasks = 0;
      for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : requests.entrySet()) {
        // The node monitor the request is being sent to should be among the list of preferred
        // nodes.
        assertTrue("Expect " + entry.getKey() + " to be among the set of preferred nodes",
                   preferredNodes.contains(entry.getKey()));
        TEnqueueTaskReservationsRequest request = entry.getValue();
        assertEquals(request.getAppId(), APP_ID);
        assertEquals(request.getUser(), user);
        assertEquals(request.getRequestId(), REQUEST_ID);
        assertEquals(request.getEstimatedResources(), estimatedResources);
        assertEquals(request.getSchedulerAddress(), SCHEDULER_ADDRESS);
        assertTrue(request.getNumTasks() > 0);
        totalEnqueuedTasks += request.getNumTasks();
      }
      assertEquals(totalEnqueuedTasks, 2);
    }
  }
  
  /**
   * Creates a scheduling request with 3 tasks, and sanity checks the result of
   * getEnqueueTaskReservationsRequests(). Then verifies the result of assignTask().
   */
  @Test
  public void testMultipleTasks() {
    List<InetSocketAddress> nodes = new ArrayList<InetSocketAddress>();
    nodes.add(new InetSocketAddress("123.4.5.6", 1));
    nodes.add(new InetSocketAddress("3.4.5.6", 56));
    nodes.add(new InetSocketAddress("4.2.3.67", 89));
    nodes.add(new InetSocketAddress("4.1.23.45", 100));
    nodes.add(new InetSocketAddress("9.0.0.0", 204));
    
    // Create 3 tasks with overlapping placement preferences (all tasks include nodes[2] in their
    // placement preferences).
    ByteBuffer message = ByteBuffer.allocate(1);
    TResourceVector estimatedResources = new TResourceVector(MEMORY, CORES);
    List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
    final int NUM_TASKS = 3;
    Set<String> taskIds = new HashSet<String>();
    for (int i = 0; i < NUM_TASKS; ++i) {
      TPlacementPreference placementPreference = new TPlacementPreference();
      for (int j = i; j < i + 3; ++j) {
        placementPreference.addToNodes(nodes.get(j).getAddress().getHostAddress());
      }
      String id = "test task " + i;
      taskIds.add(id);
      tasks.add(new TTaskSpec(id, placementPreference, estimatedResources, message));
    }
    
    TUserGroupInfo user = new TUserGroupInfo(USER, GROUP);
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);
    
    // Create list of available backend nodes, with some additional nodes in additional to the
    // preferred ones.
    List<InetSocketAddress> backendNodes = new ArrayList<InetSocketAddress>(nodes);
    backendNodes.add(new InetSocketAddress("1.2.3.4", 345));
    backendNodes.add(new InetSocketAddress("5.6.7.81", 2000));
    
    final int NUM_ITERATIONS = 1;
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      ConstrainedTaskPlacer taskPlacer = new ConstrainedTaskPlacer(REQUEST_ID, 2);
      
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID,
                                                        backendNodes, SCHEDULER_ADDRESS);
      
      // Sanity check the list of requests.
      int totalEnqueuedTasks = 0;
      for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : requests.entrySet()) {
        assertTrue("Expect " + entry.getKey() + " to be among the set of preferred nodes",
            nodes.contains(entry.getKey()));
        TEnqueueTaskReservationsRequest request = entry.getValue();
        assertEquals(request.getAppId(), APP_ID);
        assertEquals(request.getUser(), user);
        assertEquals(request.getRequestId(), REQUEST_ID);
        assertEquals(request.getEstimatedResources(), estimatedResources);
        assertEquals(request.getSchedulerAddress(), SCHEDULER_ADDRESS);
        assertTrue(request.getNumTasks() > 0);
        totalEnqueuedTasks += request.getNumTasks();
      }
      // Expect two choices for each of the 3 tasks.
      assertEquals(totalEnqueuedTasks, 6);
      
      // Try to get tasks for the non-preferred machines. This should return null.
      for (int j = nodes.size(); j < backendNodes.size(); ++j) {
        THostPort hostPort = Network.socketAddressToThrift(backendNodes.get(j));
        assertEquals(taskPlacer.assignTask(hostPort), null);
      }
      
      // Now try to get the three tasks, all for the node that all three tasks prefer. Even if
      // this node wasn't selected to probe, UnconstrainedTaskPlacer should return a task that
      // can run there (which it does as an optimization).
      Set<String> taskIdsCopy = new HashSet<String>(taskIds);
      THostPort preferredHostPort = Network.socketAddressToThrift(nodes.get(2));
      for (int j = 0; j < NUM_TASKS; ++j) {
        TTaskLaunchSpec spec = taskPlacer.assignTask(preferredHostPort);
        assertTrue(spec != null);
        assertTrue(taskIdsCopy.contains(spec.getTaskId()));
        taskIdsCopy.remove(spec.getTaskId());
      }
      
      // Trying to get any more tasks should return null.
      for (int j = 0; j < backendNodes.size(); ++j) {
        THostPort hostPort = Network.socketAddressToThrift(backendNodes.get(j));
        assertEquals(taskPlacer.assignTask(hostPort), null);
      }
    }
    
  }

}
