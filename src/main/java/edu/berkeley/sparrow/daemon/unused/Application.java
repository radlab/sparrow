package edu.berkeley.sparrow.daemon.unused;

import java.net.InetAddress;
import java.util.HashMap;

import edu.berkeley.sparrow.thrift.LoadSpec;

/**
 * Stores data pertaining to a particular application using Sparrow.
 */
public class Application {
  private final String appId;
  private HashMap<InetAddress, Node> backends;
  
  public Application(String appId) {
    this.appId = appId;
  }
  
  public String getAppId() {
    return appId;
  }
  
  public void addBackend(InetAddress addr, LoadSpec load) {
    this.backends.put(addr, new Node(load));
  }
  
  public void removeBackend(InetAddress addr) {
    this.backends.remove(addr); // just returns null if not in set
  }
}
