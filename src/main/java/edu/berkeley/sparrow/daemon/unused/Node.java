package edu.berkeley.sparrow.daemon.unused;

import edu.berkeley.sparrow.thrift.LoadSpec;

public class Node {
  private LoadSpec lastLoad;
  private long lastLoadTime;
  
  public Node(LoadSpec currentLoad) {
    this.lastLoad = currentLoad;
    this.lastLoadTime = System.currentTimeMillis();
  }
  
  public void setLoad(LoadSpec load) {
    this.lastLoad = load;
    this.lastLoadTime = System.currentTimeMillis();
  }
}
