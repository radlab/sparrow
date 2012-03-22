package edu.berkeley.sparrow.daemon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskPlacement;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class TestSparrowStandalone {
  private static int backendOnePort = 12347;
  private static int backendTwoPort = 12348;
  
  @BeforeClass
  /**
   * Set up a Sparrow daemon with two node monitors and two backends.
   */
  public static void setUp() throws Exception {
    File config = File.createTempFile("sparrowConfig", ".txt");
    config.deleteOnExit();
    BufferedWriter out = new BufferedWriter(new FileWriter(config));
    out.write(SparrowConf.DEPLYOMENT_MODE + " = standalone\n");
    out.write(SparrowConf.NM_THRIFT_PORTS + " = 12345,12346\n");
    out.write(SparrowConf.INTERNAL_THRIFT_PORTS + " = 12347,12348\n");
    out.close();
    SparrowDaemon.main(new String[] {"-c", config.getAbsolutePath()});
    
    NodeMonitorService.Client client1 = TClients.createBlockingNmClient(
        "localhost", 12345);
    client1.registerBackend("testApp", "localhost:" + backendOnePort);

    NodeMonitorService.Client client2 = TClients.createBlockingNmClient(
        "localhost", 12346);
    client2.registerBackend("testApp", "localhost:" + backendTwoPort);
  }
  
  @Test
  public void testScheduleTwoTasks() throws TException, IOException {
    SchedulerService.Client fe1 = TClients.createBlockingSchedulerClient("localhost", 
        SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
    fe1.registerFrontend("testApp", "unused:12345");
    
    // Create scheduling request with two tasks
    TSchedulingRequest req = new TSchedulingRequest();
    req.app = "testApp";
    req.tasks = new ArrayList<TTaskSpec>();
    TTaskSpec t1 = new TTaskSpec();
    t1.taskID = "1";
    TTaskSpec t2 = new TTaskSpec();
    t2.taskID = "2";
    req.tasks.add(t1);
    req.tasks.add(t2);
    List<TTaskPlacement> result = fe1.getJobPlacement(req);
    
    // Make sure the two tasks were assigned to the two backends
    boolean taskOnePlaced = false;
    boolean taskTwoPlaced = false;
    boolean nodeOneAssigned = false;
    boolean nodeTwoAssigned = false;
    for (TTaskPlacement placement : result) {
      if (placement.taskID.equals("1")) { taskOnePlaced = true; }
      if (placement.taskID.equals("2")) { taskTwoPlaced = true; }
      if (placement.node.contains(Integer.toString(backendOnePort))) { 
        nodeOneAssigned = true; 
      }
      if (placement.node.contains(Integer.toString(backendTwoPort))) {
        nodeTwoAssigned = true;
      }
      assertTrue(placement.node.contains(InetAddress.getLocalHost().getHostName()));
    }
    assertTrue(taskOnePlaced);
    assertTrue(taskTwoPlaced);
    assertTrue(nodeOneAssigned);
    assertTrue(nodeTwoAssigned);
  }
  
  @Test
  public void testScheduleMoreTasksThanBackends() throws TException, IOException {
    SchedulerService.Client fe1 = TClients.createBlockingSchedulerClient("localhost", 
        SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
    fe1.registerFrontend("testApp", "unused:12345");
    
    // Create scheduling request with four tasks
    TSchedulingRequest req = new TSchedulingRequest();
    req.app = "testApp";
    req.tasks = new ArrayList<TTaskSpec>();
    TTaskSpec t1 = new TTaskSpec();
    t1.taskID = "1";
    TTaskSpec t2 = new TTaskSpec();
    t2.taskID = "2";
    TTaskSpec t3= new TTaskSpec();
    t3.taskID = "3";
    TTaskSpec t4 = new TTaskSpec();
    t4.taskID = "4";
    req.tasks.add(t1);
    req.tasks.add(t2);
    req.tasks.add(t3);
    req.tasks.add(t4);
    List<TTaskPlacement> result = fe1.getJobPlacement(req);
    
    // Make sure each backend received two tasks
    int backendOneTasks = 0;
    int backendTwoTasks = 0;
    
    for (TTaskPlacement placement : result) {
      if (placement.node.contains(Integer.toString(backendOnePort))) { 
        backendOneTasks++; 
      }
      if (placement.node.contains(Integer.toString(backendTwoPort))) {
        backendTwoTasks++; 
      }
      assertTrue(placement.node.contains(InetAddress.getLocalHost().getHostName()));
    }
    assertEquals(2, backendOneTasks);
    assertEquals(2, backendTwoTasks);
  }
}
