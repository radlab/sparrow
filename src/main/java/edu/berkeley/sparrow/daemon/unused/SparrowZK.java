package edu.berkeley.sparrow.daemon.unused;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.SparrowDaemon;
import edu.berkeley.sparrow.daemon.scheduler.Scheduler;
import edu.berkeley.sparrow.thrift.LoadSpec;

/**
 * This class maintains Sparrow's connection to Zookeeper and updates
 * a {@link SparrowDaemon}'s state when it receives notification from 
 * Zookeeper. The state includes:
 * 
 *   - The list of backends running for each application
 *   - The load on cluster machines imposed by exogenous services, such as
 *     Hadoop.
 */
public class SparrowZK {
  protected static String NODES_PREFIX = "/nodes/";
  protected static String APPS_PREFIX = "/apps/";
  
  // Helper functions for coralling local datatypes into ZK-friendly byte
  // arrays. We should consider moving to thrift serialization for this.
  protected static byte[] loadToBytes(LoadSpec load) {
    ByteBuffer b = ByteBuffer.allocate(8);
    b.putDouble(load.getLoad());
    return b.array();
  }
  
  protected static LoadSpec bytesToLoad(byte[] bytes) {
    ByteBuffer b = ByteBuffer.wrap(bytes);
    LoadSpec out = new LoadSpec();
    out.setLoad(b.getFloat());
    return out;
  }
  
  public boolean initialized = false;
  private String zkHostString;
  private int zkTimeout; 
  private ZooKeeper zk;
  private Watcher watcher;
  
  /**
   * Zookeeper watcher for asynchronous events and watches. Zookeeper requires 
   * a watcher implementation for all connections.
   */
  private class SparrowWatcher implements Watcher {
    private boolean initialized = false;
    private Scheduler scheduler;
    
    public SparrowWatcher(Scheduler scheduler) {
      this.scheduler = scheduler;
    }
    
    @Override
    public void process(WatchedEvent event) {
      try {
        if (!initialized) {
          if (event.getState() == Event.KeeperState.SyncConnected) {
            this.initialized = true;
          }
          else {
            return;
          }
        }
  
        switch (event.getType()) {
        case NodeCreated:
          if (event.getPath().startsWith(SparrowZK.NODES_PREFIX)) {
            // Set a watch for this node
            zk.exists(event.getPath(), true);
          }
          break;
        case NodeDataChanged:
          if (event.getPath().startsWith(SparrowZK.NODES_PREFIX)) {
            Stat stat = new Stat();
            byte[] data = zk.getData(event.getPath(), true, stat);
            String[] parts = event.getPath().split("/");
            String node = parts[parts.length - 1];
            LoadSpec load = bytesToLoad(data);
            // scheduler.updateLoad(node, load);
          }
        }
      }
      // TODO more granular exception handling
      catch (Exception e) {
        System.out.println("ZK Error " + e.getMessage());
        System.exit(1);
      }
    }
  }
  
  public SparrowZK(Scheduler scheduler, Configuration conf)
      throws IOException {
    this.watcher = new SparrowWatcher(scheduler);
    zkHostString = conf.getString(SparrowConf.ZK_SERVERS, "localhost");
    zkTimeout = conf.getInt(SparrowConf.ZK_TIMEOUT, 1000);
    zk = new ZooKeeper(zkHostString, zkTimeout, watcher);
  }
  
  /**
   * Record in ZooKeeper the exogenous (non-sparrow) load on a machine.
   */
  public void setExternalLoad(String host, LoadSpec loadSpec) throws IOException {
    try {
      byte[] data = loadToBytes(loadSpec);
      zk.setData(NODES_PREFIX + "host", data, 0);
    } catch (KeeperException e) {
      throw new IOException("Failed to write load value to ZK: " + e.getMessage());
    } catch (InterruptedException e) {
      throw new IOException("Failed to write load value to ZK: " + e.getMessage());
    }
  }
  
  /**
   * Signal Sparrow to start tracking the backends (and associated external
   * load) for {@code application}.  This will also update the load state
   * whenever it changes in Zookeeper.
   */
  public void watchApplication(Application application) throws IOException{
    String appPrefix = APPS_PREFIX + application.getAppId();
    try {
      // TODO(kay): Make all of these zookeeper calls asynchronous!
      Stat exists = new Stat();
      List<String> backends = zk.getChildren(appPrefix, true, exists);
      if (exists != null) {
        for (int i = 0; i < backends.size(); i++) {
          String backendPath = appPrefix + "/" + backends.get(i);
          Stat backendStat = new Stat();
          byte[] loadData = zk.getData(backendPath, true, backendStat);
          if (backendStat != null) {
            application.addBackend(InetAddress.getByName(backends.get(i)),
                bytesToLoad(loadData));
          }
        }
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to read value from ZK: " + e.getMessage());
    } catch (InterruptedException e) {
      throw new IOException("Failed to read value from ZK: " + e.getMessage());
    }
  }
  
  /**
   * Signal Sparrow to start tracking the external load on {@code host}.
   * This will create a ZooKeeper node for {@code host} if it does not exist.
   * It will also install a watch on the node and update the load state whenever
   * it changes in ZooKeeper.
   */
  public LoadSpec watchExternalLoad(String host) throws IOException {
    String nodePrefix = NODES_PREFIX + host;
    try {
      Stat exists = zk.exists(nodePrefix, true);
      // If the node doesn't exist, create it
      if (exists == null) {
        LoadSpec spec = new LoadSpec();
        zk.create(nodePrefix, loadToBytes(spec), 
            new ArrayList<ACL>(), CreateMode.PERSISTENT);
        return spec;
      }

      Stat stat = new Stat();
      byte[] data = zk.getData(nodePrefix, true, stat);
      return bytesToLoad(data);

    } catch (KeeperException e) {
      throw new IOException("Failed to read load value from ZK: " + e.getMessage());
    } catch (InterruptedException e) {
      throw new IOException("Failed to read load value from ZK: " + e.getMessage());
    }
  }

}
