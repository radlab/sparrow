/*
 * Copyright 2013 The Regents of The University California
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestUnconstrainedTaskPlacer {
  private static final String APP_ID = "test app";
  private static final String USER = "user";
  private static final String GROUP = "group";
  private static final int PRIORITY = 0;
  private static final String REQUEST_ID = "request id";
  private static final THostPort SCHEDULER_ADDRESS = new THostPort("localhost", 12345);
  private static final int NUM_TASKS = 2;
  private static final double PROBE_RATIO = 1.5;
  private static TUserGroupInfo user = new TUserGroupInfo(USER, GROUP, PRIORITY);
  private static List<TTaskSpec> tasks;
  Set<String> taskIds;
  private static List<InetSocketAddress> backendNodes;

  @Before
  public void setUp() throws Exception {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
    tasks = Lists.newArrayList();
    taskIds = Sets.newHashSet();
    ByteBuffer message = ByteBuffer.allocate(1);
    TPlacementPreference placementPreference = new TPlacementPreference();
    for (int i = 0; i < NUM_TASKS; ++i) {
      String id = "test task " + i;
      taskIds.add(id);
      tasks.add(new TTaskSpec(id, placementPreference, message));
    }

    backendNodes = Lists.newArrayList();
    backendNodes.add(new InetSocketAddress("3.4.5.6", 174));
    backendNodes.add(new InetSocketAddress("127.124.0.1", 22));
    backendNodes.add(new InetSocketAddress("123.4.5.6", 20000));
    backendNodes.add(new InetSocketAddress("7.0.0.9", 45));
    backendNodes.add(new InetSocketAddress("234.5.6.7", 22));
    backendNodes.add(new InetSocketAddress("9.8.7.6", 1));
  }
  
  @Test
  public void enqueueTaskReservationsWithMoreReservationsThanNodes() {
	final double PROBE_RATIO = 2;
	final int NUM_TASKS = 14;

    ByteBuffer message = ByteBuffer.allocate(1);
    TPlacementPreference placementPreference = new TPlacementPreference();
    while (tasks.size() < NUM_TASKS) {
      String id = "test task " + tasks.size();
      taskIds.add(id);
      tasks.add(new TTaskSpec(id, placementPreference, message));
    }

    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);
    
    final int NUM_ITERATIONS = 100;
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(REQUEST_ID, PROBE_RATIO);

      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, backendNodes,
                                                        SCHEDULER_ADDRESS);
      // Sanity check the list of requests.
      final int EXPECTED_RESERVATIONS = 28;
      int reservationsCount = 0;
      for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : requests.entrySet()) {
        // The node monitor the request is being sent to should be among the list of backend nodes.
        assertTrue("Expect " + entry.getKey() + " to be among the set of backend nodes",
                   backendNodes.contains(entry.getKey()));
        TEnqueueTaskReservationsRequest request = entry.getValue();
        assertEquals(request.getAppId(), APP_ID);
        assertEquals(request.getUser(), user);
        assertEquals(request.getRequestId(), REQUEST_ID);
        assertEquals(request.getSchedulerAddress(), SCHEDULER_ADDRESS);
        // Expect the reservations to be evenly balanced over the machines.
        assertTrue(request.getNumTasks() == 4 || request.getNumTasks() == 5);
        reservationsCount += request.getNumTasks();
      }
      assertEquals(reservationsCount, EXPECTED_RESERVATIONS);
    }
  }


  /**
   * Calls getEnqueueTaskReservations() for two tasks, with 6 possible backend nodes, and ensures
   * that the reservations returned are for distinct nodes in the set of backends.
   */
  @Test
  public void sanityCheckEnqueueTaskReservations() {
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    final int NUM_ITERATIONS = 100;
    for (int i = 0; i < NUM_ITERATIONS; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(REQUEST_ID, PROBE_RATIO);

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
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    final int numIterations = 100;
    final int expectedReservations = 3;
    for (int i = 0; i < numIterations; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(REQUEST_ID, PROBE_RATIO);

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
        List<TTaskLaunchSpec> specs = taskPlacer.assignTask(hostPort);
        assertTrue(specs != null);
        if (j < NUM_TASKS) {
          assertEquals(specs.size(), 1);
          TTaskLaunchSpec spec = specs.get(0);
          assertTrue("Expect to receive a task spec for task " + j + " at " +
                     hostPort.getHost() + ":" + hostPort.getPort(), spec != null);
          assertTrue("Expect list of unlaunched tasks to contain " + spec.getTaskId(),
                     taskIdsCopy.contains(spec.getTaskId()));
          taskIdsCopy.remove(spec.getTaskId());
        } else {
          assertEquals(specs.size(), 0);
        }
      }
    }
  }

  /**
   * Verifies that allTasksPlaced() works correctly by first calling
   * getEnqueueTaskReservationsRequests() for a job with two tasks. Then, dovetails calls to
   * assignTasks() to calls of allTasksPlaced() to ensure that allTasksPlaced() returns
   * the correct answer.
   */
  @Test
  public void testAllTasksPlaced() {
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    final int numIterations = 100;
    final int expectedReservations = 3;
    for (int i = 0; i < numIterations; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(REQUEST_ID, PROBE_RATIO);

      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, backendNodes,
                                                        SCHEDULER_ADDRESS);

      // Now try to get tasks, and ensure that the task placer will return exactly 2 tasks.
      List<InetSocketAddress> nodes = Lists.newArrayList(requests.keySet());
      assertEquals(nodes.size(), expectedReservations);
      Collections.shuffle(nodes);
      Set<String> taskIdsCopy = Sets.newHashSet(taskIds);
      for (int j = 0; j < expectedReservations; ++j) {
        if (j < NUM_TASKS) {
          assertTrue(!taskPlacer.allTasksPlaced());
        }
        THostPort hostPort = new THostPort(nodes.get(j).getHostName(), nodes.get(j).getPort());
        List<TTaskLaunchSpec> specs = taskPlacer.assignTask(hostPort);
        assertTrue(specs != null);
        if (j < NUM_TASKS) {
          assertEquals(1, specs.size());
          TTaskLaunchSpec spec = specs.get(0);
          assertTrue("Expect to receive a task spec for task " + j + " at " +
                     hostPort.getHost() + ":" + hostPort.getPort(), spec != null);
          assertTrue("Expect list of unlaunched tasks to contain " + spec.getTaskId(),
                     taskIdsCopy.contains(spec.getTaskId()));
          taskIdsCopy.remove(spec.getTaskId());
        } else {
          assertTrue(taskPlacer.allTasksPlaced());
          assertEquals(0, specs.size());
        }
      }
    }
  }

  @Test
  public void testCancellation() {
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    final int numIterations = 100;
    for (int i = 0; i < numIterations; ++i) {
      UnconstrainedTaskPlacer taskPlacer = new UnconstrainedTaskPlacer(REQUEST_ID, PROBE_RATIO);
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, backendNodes,
              SCHEDULER_ADDRESS);

      List<InetSocketAddress> nodes = Lists.newArrayList(requests.keySet());
      for (int taskIndex = 0; taskIndex < NUM_TASKS; ++taskIndex) {
        THostPort hostPort = new THostPort(nodes.get(0).getAddress().getHostAddress(),
                                           nodes.get(0).getPort());
        nodes.remove(0);
        taskPlacer.assignTask(hostPort);
      }

      // Get the node monitors that should be cancelled.  At this point, the remaining entries in
      // nodes should be the set of nodes where requests should be cancelled.
      Set<THostPort> remainingNodeMonitors = taskPlacer.getOutstandingNodeMonitorsForCancellation();
      assertEquals(nodes.size(), remainingNodeMonitors.size());
      for (InetSocketAddress node : nodes) {
        THostPort hostPort = new THostPort(node.getAddress().getHostAddress(), node.getPort());
        assertTrue(remainingNodeMonitors.contains(hostPort));
      }

      // Trying to get the outstanding NMs for cancellation again should return an empty set.
      assertEquals(0, taskPlacer.getOutstandingNodeMonitorsForCancellation().size());
    }
  }
}
