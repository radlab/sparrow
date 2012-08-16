package edu.berkeley.sparrow.daemon.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TPlacementPreference;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestUnconstrainedTaskPlacer {
  private static final String APP_ID = "test app";
  private static final String USER = "user";
  private static final String GROUP = "group";
  private static final String REQUEST_ID = "request id";
  private static final int MEMORY = 10;
  private static final int CORES = 1;
  private static final THostPort SCHEDULER_ADDRESS = new THostPort("localhost", 12345);

  @Before
  public void setUp() throws Exception {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
  }

  /**
   * Calls getEnqueueTaskReservations() for two tasks, with 6 possible backend nodes, and ensures
   * that the reservations returned are for distinct nodes in the set of backends.
   */
  @Test
  public void sanityCheckEnqueueTaskReservations() {
    final double PROBE_RATIO = 1.5;
    
    final int NUM_TASKS = 2;
    List<TTaskSpec> tasks = Lists.newArrayList();
    ByteBuffer message = ByteBuffer.allocate(1);
    TPlacementPreference placementPreference = new TPlacementPreference();
    TResourceVector estimatedResources = new TResourceVector(MEMORY, CORES);
    Set<String> taskIds = Sets.newHashSet();
    for (int i = 0; i < NUM_TASKS; ++i) {
      String id = "test task " + i;
      taskIds.add(id);
      tasks.add(new TTaskSpec(id, placementPreference, estimatedResources, message));
    }
    
    TUserGroupInfo user = new TUserGroupInfo(USER, GROUP);
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);
    
    List<InetSocketAddress> backendNodes = Lists.newArrayList();
    backendNodes.add(new InetSocketAddress("3,4,5,6", 174));
    backendNodes.add(new InetSocketAddress("127.0.0.1", 22));
    backendNodes.add(new InetSocketAddress("123.4.5.6", 20000));
    backendNodes.add(new InetSocketAddress("7.0.0.9", 45));
    backendNodes.add(new InetSocketAddress("234.5.6.7", 22));
    backendNodes.add(new InetSocketAddress("9.8.7.6", 1));
    
    final int NUM_ITERATIONS = 100;
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(PROBE_RATIO);
      
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, backendNodes,
                                                        SCHEDULER_ADDRESS);
      // Sanity check the list of requests.
      final int EXPECTED_REQUESTS = 3;
      assertEquals(EXPECTED_REQUESTS, requests.size());
      for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : requests.entrySet()) {
        // The node monitor the request is being sent to should be among the list of backend nodes.
        assertTrue("Expect " + entry.getKey() + " to be among the set of backend nodes",
                   backendNodes.contains(entry.getKey()));
        TEnqueueTaskReservationsRequest request = entry.getValue();
        assertEquals(request.getAppId(), APP_ID);
        assertEquals(request.getUser(), user);
        assertEquals(request.getRequestId(), REQUEST_ID);
        assertEquals(request.getEstimatedResources(), estimatedResources);
        assertEquals(request.getSchedulerAddress(), SCHEDULER_ADDRESS);
        assertEquals(1, request.getNumTasks());
      }
    }
  }

  /** 
   * First, calls getEnqueueTaskReservationsRequests() for a job with two tasks.  Then, calls
   * assignTasks() for the backends returned by getEnqueueTaskReservationsRequests() and ensures
   * that the tasks returned are consistent with the original scheduling request.
   */
  @Test
  public void testAssignTask() {
    final double probeRatio = 1.5;
    
    final int numTasks = 2;
    List<TTaskSpec> tasks = Lists.newArrayList();
    ByteBuffer message = ByteBuffer.allocate(1);
    TPlacementPreference placementPreference = new TPlacementPreference();
    TResourceVector estimatedResources = new TResourceVector(MEMORY, CORES);
    Set<String> taskIds = Sets.newHashSet();
    for (int i = 0; i < numTasks; ++i) {
      String id = "test task " + i;
      taskIds.add(id);
      tasks.add(new TTaskSpec(id, placementPreference, estimatedResources, message));
    }
    
    TUserGroupInfo user = new TUserGroupInfo(USER, GROUP);
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);
    
    List<InetSocketAddress> backendNodes = Lists.newArrayList();
    backendNodes.add(new InetSocketAddress("3,4,5,6", 174));
    backendNodes.add(new InetSocketAddress("127.0.0.1", 22));
    backendNodes.add(new InetSocketAddress("123.4.5.6", 20000));
    backendNodes.add(new InetSocketAddress("7.0.0.9", 45));
    backendNodes.add(new InetSocketAddress("234.5.6.7", 22));
    backendNodes.add(new InetSocketAddress("9.8.7.6", 1));
    
    final int numIterations = 100;
    final int expectedReservations = 3;
    for (int i = 0; i < numIterations; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(probeRatio);
      
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, backendNodes,
                                                        SCHEDULER_ADDRESS);

      // Now try to get tasks, and ensure that the task placer will return exactly 2 tasks.
      List<InetSocketAddress> nodes = Lists.newArrayList(requests.keySet());
      assertEquals(nodes.size(), expectedReservations);
      Collections.shuffle(nodes);
      Set<String> taskIdsCopy = Sets.newHashSet(taskIds);
      for (int j = 0; j < expectedReservations; ++j) {
        THostPort hostPort = new THostPort(nodes.get(j).getHostName(), nodes.get(j).getPort());
        TTaskLaunchSpec spec = taskPlacer.assignTask(hostPort);
        if (j < numTasks) {
          assertTrue("Expect to receive a task spec for task " + j + " at " +
                     hostPort.getHost() + ":" + hostPort.getPort(), spec != null);
          assertTrue("Expect list of unlaunched tasks to contain " + spec.getTaskId(),
                     taskIdsCopy.contains(spec.getTaskId()));
          taskIdsCopy.remove(spec.getTaskId());
        } else {
          assertEquals(null, spec);
        }
      }
      assertTrue(taskPlacer.allResponsesReceived());
    }
    
  }

}
