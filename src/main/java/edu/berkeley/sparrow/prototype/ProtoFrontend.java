package edu.berkeley.sparrow.prototype;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * This is the frontend for the prototype implementation. Right now this does almost
 * nothing except launch a single job with one task of random deviation.
 */
public class ProtoFrontend {
  public static List<TTaskSpec> generateJob() {
    TResourceVector resources = TResources.createResourceVector(100, 1);
    Random r = new Random();
    
    // Duration of tasks (each task is the same)
    int msToSleep = 10000 + r.nextInt(500);
    byte[] message = ByteBuffer.allocate(4).putInt(msToSleep).array();

    List<TTaskSpec> out = new ArrayList<TTaskSpec>();
    for (int i = 0; i < 1; i++) {
      TTaskSpec spec = new TTaskSpec();
      byte[] taskId = ByteBuffer.allocate(4).putInt(i).array();
      spec.setTaskID(taskId);
      spec.setMessage(message);
      spec.setEstimatedResources(resources);
      out.add(spec);
    }
    return out;
  }
  
  public static void main(String[] args) throws TException {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
    
    SparrowFrontendClient client = new SparrowFrontendClient();
    TUserGroupInfo user = new TUserGroupInfo();
    user.setUser("*");
    user.setGroup("*");
    client.initialize(new InetSocketAddress("localhost", 
        SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT), "testApp");
    System.out.println(client.submitJob("testApp", generateJob(), user));
  }
}
