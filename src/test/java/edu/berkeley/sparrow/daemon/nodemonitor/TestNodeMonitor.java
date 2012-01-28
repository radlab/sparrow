package edu.berkeley.sparrow.daemon.nodemonitor;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

public class TestNodeMonitor {
  private NodeMonitor nodeMonitor;
  
  @Before
  public void setUp() {
    nodeMonitor = new NodeMonitor();
    Configuration conf = new BaseConfiguration();
    conf.setProperty(SparrowConf.DEPLYOMENT_MODE, "standalone");
    nodeMonitor.initialize(conf);
  }
  
  @After
  public void tearDown() {
    nodeMonitor = null;
  }
  
  @Test
  public void testInvalidLoadReturnedForNonExistentApp() {
    TResourceVector load = nodeMonitor.getLoad("fakeApp");
    assertEquals(false, TResources.isValid(load));
  }
  
  @Test
  public void testEmptyResourcesForNewBackend() {
    TResourceVector initialCapacity = TResources.createResourceVector(1024);
    nodeMonitor.registerBackend("app1", 
        new InetSocketAddress("localhost", 1234));
    TResourceVector resource = nodeMonitor.getLoad("app1");
    assertTrue(TResources.equal(TResources.createResourceVector(0), resource));
  }
  
  @Test
  public void testResourceGetAfterSet () {
    TResourceVector initialCapacity = TResources.createResourceVector(1024);
    nodeMonitor.registerBackend("app1", 
        new InetSocketAddress("localhost", 1234));
    
    // Set load manually
    TResourceVector resPerUser = TResources.createResourceVector(256);
    TUserGroupInfo user1 = new TUserGroupInfo();
    user1.setUser("patrick");
    user1.setGroup("berkeley");
    
    TUserGroupInfo user2 = new TUserGroupInfo();
    user1.setUser("kay");
    user1.setGroup("berkeley");
    
    Map<TUserGroupInfo, TResourceVector> load = 
        new HashMap<TUserGroupInfo, TResourceVector>();
    
    load.put(user1, resPerUser);
    load.put(user2, resPerUser);
    
    nodeMonitor.updateResourceUsage("app1", load);
    
    TResourceVector resource = nodeMonitor.getLoad("app1");
    assertTrue(TResources.equal(
        TResources.createResourceVector(512), resource));
  }
}
