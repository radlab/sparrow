package edu.berkeley.sparrow.daemon.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;

/***
 * Thrift servers block when their serve() method is called, so this
 * class wraps a Thrift server, enabling us to run in a new thread.
 */
public class TServerRunnable implements Runnable {
  private final static Logger LOG = Logger.getLogger(TServerRunnable.class);
  private TServer server;
  
  public TServerRunnable(TServer server) {
    LOG.setLevel(Level.DEBUG);
    this.server = server;
  }
  
  public void run() {
    this.server.serve();
  }
}