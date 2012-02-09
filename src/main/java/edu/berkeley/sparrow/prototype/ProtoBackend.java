package edu.berkeley.sparrow.prototype;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * A prototype Sparrow backend. This backend executes tasks assuming the task message
 * has a single integer conveying amount of time to sleep in ms. Each task executes
 * in a new thread, and uses all of the resources estimated in the scheduling request.
 */
public class ProtoBackend implements BackendService.Iface {
  private static final int LISTEN_PORT = 54321;
  
  /**
   * This is just how many threads can concurrently be answering function calls
   * from the NM. Each task is launched in its own from one of these threads.
   */
  private static final int WORKER_THREADS = 2;
  private static final String APP_ID = "testApp";
  
  /** We assume we are speaking to local Node Manager. */
  private static final String NM_HOST = "localhost";
  private static final int NM_PORT = NodeMonitorThrift.DEFAULT_NM_THRIFT_PORT;
  
  private static final Logger AUDIT_LOG = Logging.getAuditLogger(ProtoBackend.class);
  
  /**
   * Create a thrift client connection to the Node Monitor.
   */
  private static NodeMonitorService.Client createNMClient() {
    TTransport tr = new TFramedTransport(
        new TSocket(ProtoBackend.NM_HOST, ProtoBackend.NM_PORT));
    try {
      tr.open();
    } catch (TTransportException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    TProtocol proto = new TBinaryProtocol(tr);
    NodeMonitorService.Client client = new NodeMonitorService.Client(proto);
    return client;
  }
  
  /**
   * Thread spawned for each task. It runs for a given amount of time (and adds
   * its resources to the total resources for that time) then stops. It updates
   * the NodeMonitor when it launches and again when it finishes.
   */
  private class TaskRunnable implements Runnable {
    private int sleepMs;
    private TResourceVector taskResources;
    private String requestId;
    private String taskId;
    
    public TaskRunnable(String requestId, String taskId, int sleepMs,
        TResourceVector taskResources) {
      this.sleepMs = sleepMs;
      this.taskResources = taskResources;
      this.requestId = requestId;
      this.taskId = taskId;
    }
    
    @Override
    public void run() {
      NodeMonitorService.Client client = createNMClient();
      
      ArrayList<String> tasksCopy = null;
      
      // Update bookkeeping for task start
      synchronized(resourceUsage) {
        TResources.addTo(resourceUsage, taskResources);
      }
      
      HashMap<TUserGroupInfo, TResourceVector> out = 
          new HashMap<TUserGroupInfo, TResourceVector>();
      
      synchronized(ongoingTasks) {
        ongoingTasks.add(this.taskId);
        tasksCopy = new ArrayList<String>(ongoingTasks); 
        
        // Inform NM of resource usage
        out.put(user, resourceUsage);
        try {
          client.updateResourceUsage(ProtoBackend.APP_ID, out, ongoingTasks);
        } catch (TException e) {
          e.printStackTrace();
        }
      }
 
      // Sleep
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
      }
      System.out.println("Task finished");
      // Log task finish before updating bookkeeping, in case bookkeeping ends up being
      // expensive.
      AUDIT_LOG.info(Logging.auditEventString("task_completion", this.requestId,
                                              this.taskId));
      
      // Update bookkeeping for task finish
      synchronized(resourceUsage) {
        TResources.subtractFrom(resourceUsage, taskResources);
      }
      synchronized(ongoingTasks) {
        ongoingTasks.remove(this.taskId);
        tasksCopy = new ArrayList<String>(ongoingTasks);
      }
      
      // Inform NM of resource usage
      out.put(user, resourceUsage);
      try {
        client.updateResourceUsage(ProtoBackend.APP_ID, out, tasksCopy);
      } catch (TException e) {
        e.printStackTrace();
      }
      client.getInputProtocol().getTransport().close();
      client.getOutputProtocol().getTransport().close(); 
    }
  }
  
  private TUserGroupInfo user; // We force all tasks to be run by same user
  private TResourceVector resourceUsage = TResources.createResourceVector(0, 0);
  private List<String> ongoingTasks = new ArrayList<String>();
  
  public ProtoBackend() {
    this.user = new TUserGroupInfo();
    user.setUser("*");
    user.setGroup("*");
  }
  
  @Override
  public void updateResourceLimits(
      Map<TUserGroupInfo, TResourceVector> resources) throws TException {
    // Does nothing
  }

  @Override
  public void launchTask(ByteBuffer message, String requestId, String taskId,
      TUserGroupInfo user, TResourceVector estimatedResources) throws TException {
    int sleepDuration = message.getInt();
    // Note we ignore user here
    new Thread(new TaskRunnable(
        requestId, taskId, sleepDuration, estimatedResources)).start();
  }
  
  public static void main(String[] args) throws IOException, TException {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
    
    Logging.configureAuditLogging();
   
    // Start backend server
    BackendService.Processor<BackendService.Iface> processor =
        new BackendService.Processor<BackendService.Iface>(new ProtoBackend());
   
    TServers.launchThreadedThriftServer(LISTEN_PORT, WORKER_THREADS, processor);
    
    // Register server
    createNMClient().registerBackend(APP_ID, "localhost:" + LISTEN_PORT);
  }
}
