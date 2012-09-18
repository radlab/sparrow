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
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.THostPort;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestTaskScheduler {
  @Before
  public void setUp() {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
  }

  private TEnqueueTaskReservationsRequest createTaskReservationRequest(
      int numTasks, int requestId, TaskScheduler scheduler, String appId) {
    String idStr = Integer.toString(requestId);
    TUserGroupInfo user = new TUserGroupInfo("user", "group");
    TResourceVector estimatedResources = new TResourceVector(0, 1);
    THostPort schedulerAddress = new THostPort("1.2.3.4", 52);
    return new TEnqueueTaskReservationsRequest(
        appId, user, idStr, estimatedResources, schedulerAddress, numTasks);
  }

  /**
   * Tests the fifo task scheduler.
   */
  @Test
  public void testFifo() {
    TaskScheduler scheduler = new FifoTaskScheduler();
    TResourceVector capacity = TResources.createResourceVector(0, 4);
    scheduler.initialize(capacity, new PropertiesConfiguration());

    final String testApp = "test app";
    final InetSocketAddress backendAddress = new InetSocketAddress("123.4.5.6", 2);

    // Make sure that tasks are launched right away, if resources are available.
    scheduler.submitTaskReservations(createTaskReservationRequest(1, 1, scheduler, testApp),
                                     backendAddress);
    assertEquals(1, scheduler.runnableTasks());
    TaskSpec task = scheduler.getNextTask();
    assertEquals("1", task.requestId);
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(2, 2, scheduler, testApp),
                                     backendAddress);
    assertEquals(2, scheduler.runnableTasks());

    // Make sure the request to schedule 3 tasks is appropriately split, with one task running
    // now and others started later.
    scheduler.submitTaskReservations(createTaskReservationRequest(3, 3, scheduler, testApp),
                                     backendAddress);
    /* 4 tasks have been launched but one was already removed from the runnable queue using
     * getTask(), leaving 3 runnable tasks. */
    assertEquals(3, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("2", task.requestId);
    task = scheduler.getNextTask();
    assertEquals("2", task.requestId);
    /* Make a list of task ids to use in every call to tasksFinished, and just update the request
     * id. */
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
    TaskScheduler scheduler = new RoundRobinTaskScheduler();
    TResourceVector capacity = TResources.createResourceVector(0, 4);
    scheduler.initialize(capacity, new PropertiesConfiguration());

    final String app1 = "app1";
    final InetSocketAddress address1 = new InetSocketAddress("localhost", 1);
    final String app2 = "app2";
    final InetSocketAddress address2 = new InetSocketAddress("localhost", 1);
    final String app3 = "app3";
    final InetSocketAddress address3 = new InetSocketAddress("localhost", 1);
    final String app4 = "app4";
    final InetSocketAddress address4 = new InetSocketAddress("localhost", 1);

    // Submit enough tasks to saturate the existing capacity.
    scheduler.submitTaskReservations(createTaskReservationRequest(1, 1, scheduler, app1), address1);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, 2, scheduler, app2), address2);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, 3, scheduler, app3), address3);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTaskReservations(createTaskReservationRequest(1, 4, scheduler, app4), address4);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    /* Create the following backlogs.
     * app1: 2 tasks
     * app2: 3 tasks
     * app3: 4 tasks
     */
    scheduler.submitTaskReservations(createTaskReservationRequest(2, 5, scheduler, app1), address1);
    scheduler.submitTaskReservations(createTaskReservationRequest(1, 6, scheduler, app2), address2);
    scheduler.submitTaskReservations(createTaskReservationRequest(1, 7, scheduler, app2), address2);
    scheduler.submitTaskReservations(createTaskReservationRequest(1, 8, scheduler, app2), address2);
    scheduler.submitTaskReservations(createTaskReservationRequest(4, 9, scheduler, app3), address3);

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
}
