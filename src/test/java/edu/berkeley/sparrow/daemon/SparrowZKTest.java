package edu.berkeley.sparrow.daemon;
import org.apache.zookeeper.server.ZooKeeperServerMain;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This is an experiment with integration tests using Zookeeper.
 */
public class SparrowZKTest {
  
  private Thread zkThread;
  
  private class ZkRunnable implements Runnable {
    @Override
    public void run() {
      String[] zooConfig = new String[] {"9000", "/tmp/"}; 
      ZooKeeperServerMain.main(zooConfig);
    }
  }
  
  @Before
  public void setUp() throws Exception {
    zkThread = new Thread(new ZkRunnable());
    zkThread.start();
    Thread.sleep(2000);
  }
  
  @Test
  public void testZookeeperSimple() throws Exception {

  }
  
  @After
  public void tearDown() {
    zkThread.stop(); // Kill Zookeeper
  }
}
