package edu.berkeley.sparrow.daemon.nodemonitor;
import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import edu.berkeley.sparrow.daemon.nodemonitor.TaskScheduler.TaskDescription;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestRoundRobinScheduler {
  private TaskDescription createTaskDescription(int id, TUserGroupInfo user, 
      TaskScheduler scheduler) {
    String idStr = Integer.toString(id);
    return scheduler.new TaskDescription(
            idStr, idStr, ByteBuffer.wrap(new byte[] {(byte) id}), 
            TResources.createResourceVector(0, 1), 
            user, InetSocketAddress.createUnresolved("localhost", 1234));
  }
  
  @Test
  public void testBasicRoundRobin() {
    TaskScheduler scheduler = new RoundRobinTaskScheduler();
    TResourceVector capacity = TResources.createResourceVector(0, 4);
    scheduler.initialize(capacity, new PropertiesConfiguration());
    
    TUserGroupInfo user1 = new TUserGroupInfo();
    user1.user = "user1";
    user1.group = "user1";
    
    TUserGroupInfo user2 = new TUserGroupInfo();
    user2.user = "user2";
    user2.group = "user2";
    
    TUserGroupInfo user3 = new TUserGroupInfo();
    user3.user = "user3";
    user3.group = "user3";
        
    // Submit enough tasks to saturate the existing capacity.
    scheduler.submitTask(createTaskDescription(1, user1, scheduler));
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTask(createTaskDescription(2, user2, scheduler));
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.submitTask(createTaskDescription(3, user3, scheduler));
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.submitTask(createTaskDescription(4, user1, scheduler));
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());
    
    /* Create the following backlogs.
     * user1: 2 tasks
     * user2: 3 tasks
     * user3: 4 tasks
     */
    scheduler.submitTask(createTaskDescription(5, user1, scheduler));
    scheduler.submitTask(createTaskDescription(6, user1, scheduler));
    scheduler.submitTask(createTaskDescription(7, user2, scheduler));
    scheduler.submitTask(createTaskDescription(8, user2, scheduler));
    scheduler.submitTask(createTaskDescription(9, user2, scheduler));
    scheduler.submitTask(createTaskDescription(10, user3, scheduler));
    scheduler.submitTask(createTaskDescription(11, user3, scheduler));
    scheduler.submitTask(createTaskDescription(12, user3, scheduler));
    scheduler.submitTask(createTaskDescription(13, user3, scheduler));

    assertEquals(0, scheduler.runnableTasks());
    
    // Make sure that as tasks finish (and space is freed up) new tasks are added to the
    // runqueue in round-robin order.
    scheduler.taskCompleted("1");
    assertEquals(1, scheduler.runnableTasks());
    TaskDescription task = scheduler.getNextTask();
    assertEquals("user1", task.user.user);
    assertEquals(0, scheduler.runnableTasks()); 
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user2", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user3", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("user1", task.user.user);
    assertEquals(0, scheduler.runnableTasks()); 
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user2", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user3", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user2", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user3", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("user3", task.user.user);
    assertEquals(0, scheduler.runnableTasks());
  }
}
