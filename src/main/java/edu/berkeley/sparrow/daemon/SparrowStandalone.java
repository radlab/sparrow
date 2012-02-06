package edu.berkeley.sparrow.daemon;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskPlacement;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/***
 * Provides a standalone executable for use in testing third party applications
 * against Sparrow. Currently, this instantiates a static set of Sparrow
 * services. Eventually it will be configurable which services are started
 * and on which ports.
 */
public class SparrowStandalone {
  public static void main(String[] args) throws IOException, TException {
    Configuration conf1 = new BaseConfiguration();
    conf1.setProperty(SparrowConf.DEPLYOMENT_MODE, "standalone");
    conf1.setProperty(SparrowConf.NM_THRIFT_PORT, 20001);
    conf1.setProperty(SparrowConf.SCHEDULER_THRIFT_PORT, 20002);
    conf1.setProperty(SparrowConf.INTERNAL_THRIFT_PORT, 20003);
    NodeMonitorThrift nodeMonitor1 = new NodeMonitorThrift();
    SchedulerThrift scheduler1 = new SchedulerThrift();
    
    Configuration conf2 = new BaseConfiguration();
    conf2.setProperty(SparrowConf.DEPLYOMENT_MODE, "standalone");
    conf2.setProperty(SparrowConf.NM_THRIFT_PORT, 20004);
    conf2.setProperty(SparrowConf.SCHEDULER_THRIFT_PORT, 20005);
    conf2.setProperty(SparrowConf.INTERNAL_THRIFT_PORT, 20006);
    NodeMonitorThrift nodeMonitor2 = new NodeMonitorThrift();
    SchedulerThrift scheduler2 = new SchedulerThrift();
    
    nodeMonitor1.initialize(conf1);
    scheduler1.initialize(conf1);
    
    nodeMonitor2.initialize(conf2);
    scheduler2.initialize(conf2);
    
    // SOME BASIC TESTING, WILL BE REMOVED
    TResourceVector backendResource = new TResourceVector();
    backendResource.setMemory(1024);
    //nodeMonitor1.registerBackend("foo");
    //nodeMonitor2.registerBackend("foo");
    //nodeMonitor1.updateResourceUsage("foo", backendResource);
    System.out.println(StandaloneStateStore.getInstance().getBackends("foo"));
    
    TTaskSpec task1 = new TTaskSpec();
    task1.setTaskID("1");
    TResourceVector requiredResources = new TResourceVector();
    requiredResources.setMemory(1024);
    task1.setEstimatedResources(requiredResources);
    TSchedulingRequest req = new TSchedulingRequest();
    req.setApp("foo");
    req.setTasks(Arrays.asList(new TTaskSpec[] {task1}));
    List<TTaskPlacement> result = scheduler1.getJobPlacement(req);
    for (TTaskPlacement placement : result) {
      System.out.println(placement);
    }
  }
}
