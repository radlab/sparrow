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
  private TaskDescription createTaskDescription(int id, TaskScheduler scheduler) {
    String idStr = Integer.toString(id);
    return scheduler.new TaskDescription(
            idStr, idStr, ByteBuffer.wrap(new byte[] {(byte) id}), 
            TResources.createResourceVector(0, 1), 
            new TUserGroupInfo(), InetSocketAddress.createUnresolved("localhost", 1234));
  }
  
  @Test
  public void testBasicRoundRobin() {
    TaskScheduler scheduler = new RoundRobinTaskScheduler();
    TResourceVector capacity = TResources.createResourceVector(0, 4);
    scheduler.initialize(capacity, new PropertiesConfiguration());
        
    final String app1 = "app1";
    final String app2 = "app2";
    final String app3 = "app3";
    final String app4 = "app4";
    
    // Submit enough tasks to saturate the existing capacity.
    scheduler.submitTask(createTaskDescription(1, scheduler), app1);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());

    scheduler.submitTask(createTaskDescription(2, scheduler), app2);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.submitTask(createTaskDescription(3, scheduler), app3);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.submitTask(createTaskDescription(4, scheduler), app4);
    assertEquals(1, scheduler.runnableTasks());
    scheduler.getNextTask();
    assertEquals(0, scheduler.runnableTasks());
    
    /* Create the following backlogs.
     * app1: 2 tasks
     * app2: 3 tasks
     * app3: 4 tasks
     */
    scheduler.submitTask(createTaskDescription(5, scheduler), app1);
    scheduler.submitTask(createTaskDescription(6, scheduler), app1);
    scheduler.submitTask(createTaskDescription(7, scheduler), app2);
    scheduler.submitTask(createTaskDescription(8, scheduler), app2);
    scheduler.submitTask(createTaskDescription(9, scheduler), app2);
    scheduler.submitTask(createTaskDescription(10, scheduler), app3);
    scheduler.submitTask(createTaskDescription(11, scheduler), app3);
    scheduler.submitTask(createTaskDescription(12, scheduler), app3);
    scheduler.submitTask(createTaskDescription(13, scheduler), app3);

    assertEquals(0, scheduler.runnableTasks());
    
    // Make sure that as tasks finish (and space is freed up) new tasks are added to the
    // runqueue in round-robin order.
    scheduler.taskCompleted("1");
    assertEquals(1, scheduler.runnableTasks());
    TaskDescription task = scheduler.getNextTask();
    assertEquals("5", task.taskId);
    assertEquals(0, scheduler.runnableTasks()); 
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("7", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("10", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks());
    task = scheduler.getNextTask();
    assertEquals("6", task.taskId);
    assertEquals(0, scheduler.runnableTasks()); 
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("8", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("11", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("9", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("12", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
    
    scheduler.taskCompleted(task.taskId);
    assertEquals(1, scheduler.runnableTasks()); 
    task = scheduler.getNextTask();
    assertEquals("13", task.taskId);
    assertEquals(0, scheduler.runnableTasks());
  }
}
