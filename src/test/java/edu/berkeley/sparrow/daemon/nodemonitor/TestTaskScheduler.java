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

package edu.berkeley.sparrow.daemon.nodemonitor;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import edu.berkeley.sparrow.daemon.nodemonitor.TaskScheduler.TaskSpec;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestTaskScheduler {
  int requestId;

  @Before
  public void setUp() {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
    requestId = 1;
  }

  private TEnqueueTaskReservationsRequest createTaskReservationRequest(
      int numTasks, TaskScheduler scheduler, String userId) {
    return createTaskReservationRequest(numTasks, scheduler, userId, 0);
  }

  private TEnqueueTaskReservationsRequest createTaskReservationRequest(
      int numTasks, TaskScheduler scheduler, String userId, int priority) {
    String idStr = Integer.toString(requestId++);
    TUserGroupInfo user = new TUserGroupInfo(userId, "group", priority);
    THostPort schedulerAddress = new THostPort("1.2.3.4", 52);
    return new TEnqueueTaskReservationsRequest(
        "appId", user, idStr, schedulerAddress, numTasks);
  }

  /**
   * Tests the fifo task scheduler.
   */
  @Test
  public void testFifo() {
    TaskScheduler scheduler = new FifoTaskScheduler(4);
    scheduler.initialize(new PropertiesConfiguration(), 12345);

    final String testApp = "test app";
    final InetSocketAddress backendAddress = new InetSocketAddress("123.4.5.6", 2);

    // Make sure that tasks are launched right away, if resources are available.
    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, testApp),
                                     backendAddress);
    assertEquals(1, scheduler.runnableTasks());
    TaskSpec task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(2, scheduler, testApp),
                                     backendAddress);
    assertEquals(2, scheduler.runnableTasks());

    // Make sure the request to schedule 3 tasks is appropriately split, with one task running
    // now and others started later.
    scheduler.submitTaskReservations(createTaskReservationRequest(3, scheduler, testApp),
                                     backendAddress);
    /* 4 tasks have been launched but one was already removed from the runnable queue using
     * getTask(), leaving 3 runnable tasks. */
    assertEquals(3, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("2", task.requestId);
    task = scheduler.getNextTask();
    assertEquals("2", task.requestId);
    /* Make a list of task ids to use in every call to tasksFinished, and just update the request
     * id for each call. */
    TFullTaskId fullTaskId = new TFullTaskId();
    fullTaskId.taskId = "";
    List<TFullTaskId> completedTasks = Lists.newArrayList();
    completedTasks.add(fullTaskId);

    // Have a few tasks complete before the last runnable task is removed from the queue.
    fullTaskId.requestId = "2";
    scheduler.tasksFinished(completedTasks);
    scheduler.tasksFinished(completedTasks);
    fullTaskId.requestId = "1";
    scheduler.tasksFinished(completedTasks);

    task = scheduler.getNextTask();
    assertEquals("3", task.requestId);
    task = scheduler.getNextTask();
    assertEquals("3", task.requestId);
    task = scheduler.getNextTask();
    assertEquals("3", task.requestId);
    assertEquals(0, scheduler.runnableTasks());
  }

  /**
   * Tests the round robin task scheduler.
   */
  @Test
  public void testBasicRoundRobin() {
    TaskScheduler scheduler = new RoundRobinTaskScheduler(4);
    scheduler.initialize(new PropertiesConfiguration(), 12345);

    final String user1 = "user1";
    final InetSocketAddress address1 = new InetSocketAddress("localhost", 1);
    final String user2 = "user2";
    final InetSocketAddress address2 = new InetSocketAddress("localhost", 1);
    final String user3 = "user3";
    final InetSocketAddress address3 = new InetSocketAddress("localhost", 1);
    final String user4 = "user4";
    final InetSocketAddress address4 = new InetSocketAddress("localhost", 1);

    // Submit enough tasks to saturate the existing capacity.
    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user1), address1);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user2), address2);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user3), address3);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user4), address4);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    /* Create the following backlogs.
     * user1: 2 tasks
     * user2: 3 tasks
     * user3: 4 tasks
     */
    scheduler.submitTaskReservations(
        createTaskReservationRequest(2, scheduler, user1), address1);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user2), address2);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user2), address2);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user2), address2);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(4, scheduler, user3), address3);

    assertEquals(0, scheduler.runnableTasks());

    /* Make sure that as tasks finish (and space is freed up) new tasks are added to the runqueue
     * in round-robin order.
     * Make a list of task ids to use in every call to tasksFinished, and just update the request
     * id. */
    TFullTaskId fullTaskId = new TFullTaskId();
    fullTaskId.taskId = "";
    List<TFullTaskId> completedTasks = Lists.newArrayList();
    completedTasks.add(fullTaskId);
    fullTaskId.requestId = "1";

    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    TaskSpec task = scheduler.getNextTask();
    assertEquals("5", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("6", task.requestId);
    assertEquals(0, scheduler.runnableTasks());
    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("9", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("5", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("7", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("9", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("8", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("9", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    fullTaskId.requestId = task.requestId;
    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("9", task.requestId);
    assertEquals(0, scheduler.runnableTasks());
  }

  @Test
  public void testRoundRobinDoesNotGiveUpUserSpotWhenGetTaskFails() {
    TaskScheduler scheduler = new RoundRobinTaskScheduler(4);
    scheduler.initialize(new PropertiesConfiguration(), 12345);

    final String user1 = "user1";
    final InetSocketAddress address1 = new InetSocketAddress("localhost", 1);
    final String user2 = "user2";
    final InetSocketAddress address2 = new InetSocketAddress("localhost", 1);
    final String user3 = "user3";
    final InetSocketAddress address3 = new InetSocketAddress("localhost", 1);
    final String user4 = "user4";
    final InetSocketAddress address4 = new InetSocketAddress("localhost", 1);

    // Submit enough tasks to saturate the existing capacity.
    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user1), address1);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user2), address2);
    assertEquals(1, scheduler.runnableTasks());
    TaskSpec user2Task = scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user3), address3);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, scheduler, user4), address4);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    /* Create the following backlogs.
     * user1: 2 tasks
     * user2: 3 tasks
     * user3: 4 tasks
     */
    scheduler.submitTaskReservations(
        createTaskReservationRequest(2, scheduler, user1), address1);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user2), address2);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user2), address2);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user2), address2);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(4, scheduler, user3), address3);

    assertEquals(0, scheduler.runnableTasks());

    // If the getTask() for user 2 fails, the scheduler should try to get another task
    // for that user rather than using the usual round-robin ordering.
    scheduler.noTaskForReservation(user2Task);
    assertEquals(1, scheduler.runnableTasks());
    user2Task = scheduler.getNextTask();
    assertEquals(user2, user2Task.user.user);
    assertEquals("6", user2Task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    // The scheduler should resume round-robin from user 1 (and not start the round-robin
    // again after user 2).
    TFullTaskId fullTaskId = new TFullTaskId();
    fullTaskId.taskId = "";
    List<TFullTaskId> completedTasks = Lists.newArrayList();
    completedTasks.add(fullTaskId);
    fullTaskId.requestId = "1";

    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    TaskSpec task = scheduler.getNextTask();
    assertEquals(user1, task.user.user);
    assertEquals("5", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    // If the scheduler eventually exhausts the queue of tasks for user 2, and cannot launch any,
    // it sound continue in round-robin order.
    scheduler.noTaskForReservation(user2Task);
    assertEquals(1, scheduler.runnableTasks());
    user2Task = scheduler.getNextTask();
    assertEquals(user2, user2Task.user.user);
    assertEquals("7", user2Task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.noTaskForReservation(user2Task);
    assertEquals(1, scheduler.runnableTasks());
    user2Task = scheduler.getNextTask();
    assertEquals(user2, user2Task.user.user);
    assertEquals("8", user2Task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.noTaskForReservation(user2Task);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals(user3, task.user.user);
    assertEquals("9", task.requestId);
    assertEquals(0, scheduler.runnableTasks());
  }

  @Test
  public void testPriority() {
    /* Submit tasks at priority 0, 1, and 2 and ensure that the highest priority tasks are
     * run soonest.
     */
    TaskScheduler scheduler = new PriorityTaskScheduler(4);
    scheduler.initialize(new PropertiesConfiguration(), 12345);

    final InetSocketAddress appBackendAddress = new InetSocketAddress("localhost", 1);
    final String user = "user";

    // Submit enough tasks to saturate the existing capacity (with one task queued).
    scheduler.submitTaskReservations(
        createTaskReservationRequest(5, scheduler, user, 2), appBackendAddress);
    assertEquals(4, scheduler.runnableTasks());

    // Submit 3 tasks for higher priority users (2 at priority 1 and 1 at priority 0).
    scheduler.submitTaskReservations(
        createTaskReservationRequest(2, scheduler, user, 1), appBackendAddress);
    scheduler.submitTaskReservations(
        createTaskReservationRequest(1, scheduler, user, 0), appBackendAddress);

    TaskSpec task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(3, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(2, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    /* Make sure that as tasks finish, new tasks are added to the runqueue in strictly priority
     * order.
     */
    TFullTaskId fullTaskId = new TFullTaskId();
    fullTaskId.taskId = "";
    List<TFullTaskId> completedTasks = Lists.newArrayList();
    completedTasks.add(fullTaskId);
    fullTaskId.requestId = "1";

    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("3", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("2", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("2", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.tasksFinished(completedTasks);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(0, scheduler.runnableTasks());
  }
}
