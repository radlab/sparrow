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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TPlacementPreference;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskLaunchSpec;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestConstrainedTaskPlacer {
  private static final String APP_ID = "test app";
  private static final String USER = "user";
  private static final String GROUP = "group";
  private static final int PRIORITY = 0;
  private static final String REQUEST_ID = "request id";
  private static final THostPort SCHEDULER_ADDRESS = new THostPort("localhost", 12345);
  private static final int ITERATIONS = 1;

  private static final TUserGroupInfo user = new TUserGroupInfo(USER, GROUP, PRIORITY);
  private static final Set<InetSocketAddress> preferredNodes = new HashSet<InetSocketAddress>();
  private static final Set<InetSocketAddress> allBackends = new HashSet<InetSocketAddress>();

  @Before
  public void setUp() {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
  }

  @After
  public void tearDown() {
    preferredNodes.clear();
    allBackends.clear();
  }

  /** Sanity checks a list of requests returned by {@code getEnqueueTaskReservationsRequests()}.
   *
   * For each request, ensures that the reservation is on one of the preferred nodes, and that it
   * has the appropriate app id/user/request id/scheduler address.
   */
  private void sanityCheckRequests(
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests,
      int expectedTotalReservations) {
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
      assertEquals(request.getSchedulerAddress(), SCHEDULER_ADDRESS);
      assertTrue(request.getNumTasks() > 0);
      totalEnqueuedTasks += request.getNumTasks();
    }
    assertEquals(expectedTotalReservations, totalEnqueuedTasks);
  }

  /** Generates a scheduling request with 3 tasks, and populates preferredNodes/allBackends. */
  private TSchedulingRequest generateSchedulingRequests() {
    preferredNodes.add(new InetSocketAddress("123.4.5.6", 1));
    preferredNodes.add(new InetSocketAddress("3.4.5.6", 56));
    preferredNodes.add(new InetSocketAddress("4.2.3.67", 89));
    preferredNodes.add(new InetSocketAddress("4.1.23.45", 100));
    preferredNodes.add(new InetSocketAddress("9.0.0.0", 204));

    List<InetSocketAddress> preferredNodesList = new ArrayList<InetSocketAddress>(preferredNodes);

    // Create 3 tasks with overlapping placement preferences (all tasks include
    // preferredNodesList[2] in their placement preferences).
    ByteBuffer message = ByteBuffer.allocate(1);
    List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
    final int NUM_TASKS = 3;
    for (int i = 0; i < NUM_TASKS; ++i) {
      TPlacementPreference placementPreference = new TPlacementPreference();
      for (int j = i; j < i + 3; ++j) {
        placementPreference.addToNodes(preferredNodesList.get(j).getAddress().getHostAddress());
      }
      String id = "test task " + i;
      tasks.add(new TTaskSpec(id, placementPreference, message));
    }

    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    // Create list of available backend nodes, with some additional nodes in addition to the
    // preferred ones.
    allBackends.addAll(preferredNodes);
    allBackends.add(new InetSocketAddress("1.2.3.4", 345));
    allBackends.add(new InetSocketAddress("5.6.7.81", 2000));
    return schedulingRequest;
  }

  /**
   * Creates a scheduling request with a single task that has 3 preferred nodes, and ensures
   * that repeated calls to getEnqueueTaskReservationsRequests always returns requests for one
   * of those three nodes.
   */
  @Test
  public void testGetEnqueueTaskReservationsRequestsSingleTask() {
    ConstrainedTaskPlacer taskPlacer = new ConstrainedTaskPlacer(REQUEST_ID, 2);

    preferredNodes.add(new InetSocketAddress("127.0.0.1", 22));
    preferredNodes.add(new InetSocketAddress("123.4.5.6", 20000));
    preferredNodes.add(new InetSocketAddress("7.0.0.9", 45));

    // Create a single task with three placement preferences.
    ByteBuffer message = ByteBuffer.allocate(1);
    TPlacementPreference placementPreference = new TPlacementPreference();
    for (InetSocketAddress address : preferredNodes) {
      placementPreference.addToNodes(address.getAddress().getHostAddress());
    }
    TTaskSpec task = new TTaskSpec("test task", placementPreference, message);

    // Create the scheduling request.
    List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
    tasks.add(task);
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    // Create a set of backends, including all of the preferred nodes and some extra nodes too.
    allBackends.add(new InetSocketAddress("3.4.5.6", 174));
    for (InetSocketAddress node : preferredNodes) {
      allBackends.add(node);
    }
    allBackends.add(new InetSocketAddress("234.5.6.7", 22));
    allBackends.add(new InetSocketAddress("9.8.7.6", 1));

    for (int i = 0; i < ITERATIONS; ++i) {
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID, allBackends,
                                                        SCHEDULER_ADDRESS);
      sanityCheckRequests(requests, 2);
    }
  }

  /**
   * Creates a scheduling request with 3 tasks, and sanity checks the result of
   * getEnqueueTaskReservationsRequests()
   */
  @Test
  public void testMultipleTasks() {
    TSchedulingRequest schedulingRequest = generateSchedulingRequests();

    for (int i = 0; i < ITERATIONS; ++i) {
      ConstrainedTaskPlacer taskPlacer = new ConstrainedTaskPlacer(REQUEST_ID, 2);

      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID,
                                                        allBackends, SCHEDULER_ADDRESS);
      sanityCheckRequests(requests, 6);
      sanityCheckGetTasksAndCancellation(schedulingRequest, taskPlacer,
          requests);
    }

  }

  private void sanityCheckGetTasksAndCancellation(
      TSchedulingRequest schedulingRequest, ConstrainedTaskPlacer taskPlacer,
      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests) {
    Set<String> taskIds = new HashSet<String>();
    for (TTaskSpec task : schedulingRequest.tasks) {
      taskIds.add(task.taskId);
    }

    // Now try to get the three tasks.
    Set<String> unlaunchedTaskIds = new HashSet<String>(taskIds);
    int numGetTasks = 0;
    Set<THostPort> cancellations = null;
    for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : requests.entrySet()) {
      // For each entry, try to assign the tasks.  Once all tasks have been assigned, cancel the
      // remaining reservations.

      THostPort hostPort = Network.socketAddressToThrift(entry.getKey());
      // If the request has been cancelled, remove the current node monitor from the
      // cancellations set, so we can check at the end that the cancellations set is empty to
      // ensure that the cancellations returned were correct.
      if (cancellations != null) {
        assertTrue(cancellations.contains(hostPort));
        cancellations.remove(hostPort);
      }

      for (int taskIndex = 0; taskIndex < entry.getValue().numTasks;
          taskIndex++) {
        ++numGetTasks;
        List<TTaskLaunchSpec> specs = taskPlacer.assignTask(hostPort);
        assertTrue(specs != null);
        if (numGetTasks == 0) {
          // If this is the first attempt, we should definitely get a task.
          assertEquals(specs.size(), 1);
        }
        if (specs.size() == 1) {
          assertTrue("assignTask() returned a task after all tasks were launched!",
              unlaunchedTaskIds.size() > 0);
          TTaskLaunchSpec spec = specs.get(0);
          assertTrue(spec != null);
          assertTrue(unlaunchedTaskIds.contains(spec.getTaskId()));
          unlaunchedTaskIds.remove(spec.getTaskId());
        }

        // If all of the tasks have been launched, cancel the outstanding ones.
        if (unlaunchedTaskIds.size() == 0 && cancellations == null) {
          assertTrue(taskPlacer.allTasksPlaced());
          cancellations = new HashSet<THostPort>(
              taskPlacer.getOutstandingNodeMonitorsForCancellation());
          assertTrue(cancellations.size() > 0);

          // If not all of the reservations at the current node monitor have been replied to yet,
          // there should be a cancellation for the current node monitor.
          if (taskIndex < entry.getValue().numTasks - 1) {
            assertTrue(cancellations.contains(hostPort));
            cancellations.remove(hostPort);
          }

          // Another cancellation should lead to no outstanding node monitors.
          Set<THostPort> secondCancellations = new HashSet<THostPort>(
              taskPlacer.getOutstandingNodeMonitorsForCancellation());
          assertEquals(secondCancellations.size(), 0);

          // Keep going after the cancellation, since this can happen if a getTask() is in flight
          // when the original cancellation occurs.
        }
      }
    }
  }

  /**
   * Creates a scheduling request with 3 constrained tasks and 1 unconstrained task, and sanity
   * checks the result of getEnqueueTaskReservationsRequests(). Then verifies the result of
   * assignTask().
   */
  @Test
  public void testConstrainedAndUnconstrainedTasks() {
    List<InetSocketAddress> preferredNodes = new ArrayList<InetSocketAddress>();
    preferredNodes.add(new InetSocketAddress("123.4.5.6", 1));
    preferredNodes.add(new InetSocketAddress("3.4.5.6", 56));
    preferredNodes.add(new InetSocketAddress("4.2.3.67", 89));
    preferredNodes.add(new InetSocketAddress("4.1.23.45", 100));
    preferredNodes.add(new InetSocketAddress("9.0.0.0", 204));

    // Create 3 tasks with overlapping placement preferences (all tasks include nodes[2] in their
    // placement preferences).
    ByteBuffer message = ByteBuffer.allocate(1);
    List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
    final int NUM_TASKS = 3;
    Set<String> constrainedTaskIds = new HashSet<String>();
    for (int i = 0; i < NUM_TASKS; ++i) {
      TPlacementPreference placementPreference = new TPlacementPreference();
      for (int j = i; j < i + 3; ++j) {
        placementPreference.addToNodes(preferredNodes.get(j).getAddress().getHostAddress());
      }
      String id = "constrained test task " + i;
      constrainedTaskIds.add(id);
      tasks.add(new TTaskSpec(id, placementPreference, message));
    }

    String unconstrainedTaskId = "unconstrained test task";
    tasks.add(new TTaskSpec(unconstrainedTaskId, null, message));

    TUserGroupInfo user = new TUserGroupInfo(USER, GROUP, PRIORITY);
    TSchedulingRequest schedulingRequest = new TSchedulingRequest(APP_ID, tasks, user);

    // Create list of available backend nodes, with some additional nodes in additional to the
    // preferred ones.
    List<InetSocketAddress> backendNodes = new ArrayList<InetSocketAddress>(preferredNodes);
    backendNodes.add(new InetSocketAddress("1.2.3.4", 345));
    backendNodes.add(new InetSocketAddress("5.6.7.81", 2000));

    for (int i = 0; i < ITERATIONS; ++i) {
      ConstrainedTaskPlacer taskPlacer = new ConstrainedTaskPlacer(REQUEST_ID, 2);

      Map<InetSocketAddress, TEnqueueTaskReservationsRequest> requests =
          taskPlacer.getEnqueueTaskReservationsRequests(schedulingRequest, REQUEST_ID,
                                                        backendNodes, SCHEDULER_ADDRESS);

      // Sanity check the list of requests.
      int totalEnqueuedTasks = 0;
      /* Number of requests not in the list of preferred nodes (expect at most 2, corresponding
       * to the two reservations for the unconstrained task). */
      int nonPreferredNodes = 0;
      for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : requests.entrySet()) {
        if (!preferredNodes.contains(entry.getKey())) {
          nonPreferredNodes++;
          assertTrue("Expect at most two entrys to be outside the set of preferred nodes",
              nonPreferredNodes <= 2);
        }
        TEnqueueTaskReservationsRequest request = entry.getValue();
        assertEquals(request.getAppId(), APP_ID);
        assertEquals(request.getUser(), user);
        assertEquals(request.getRequestId(), REQUEST_ID);
        assertEquals(request.getSchedulerAddress(), SCHEDULER_ADDRESS);
        assertTrue(request.getNumTasks() > 0);
        totalEnqueuedTasks += request.getNumTasks();
      }
      // Expect two choices for each of the 4 tasks.
      assertEquals(totalEnqueuedTasks, 8);

      sanityCheckGetTasksAndCancellation(schedulingRequest, taskPlacer, requests);
    }

  }

}
