package edu.berkeley.sparrow.daemon.util;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

/***
 * Helper functions for dispatching Thrift servers.
 */
public class TServers {
  private final static Logger LOG = Logger.getLogger(TServers.class);

  /**
   * Launch a multi-threaded Thrift server with the given {@code processor}. Note that
   * internally this creates an expanding thread pool of at most {@code threads} threads,
   * and requests are queued whenever that thread pool is saturated.
   */
  public static void launchThreadedThriftServer(int port, int threads,
      TProcessor processor) throws IOException {
    LOG.info("Staring async thrift server of type: " + processor.getClass().toString()
    		+ " on port " + port);
    TNonblockingServerTransport serverTransport;
    try {
      serverTransport = new TNonblockingServerSocket(port);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    Args serverArgs = new Args(serverTransport);
    serverArgs.processor(processor);
    serverArgs.workerThreads(threads);
    TServer server = new THsHaServer(serverArgs);
    new Thread(new TServerRunnable(server)).start();
  }

 /**
  * Runnable class to wrap thrift servers in their own thread.
  */
  private static class TServerRunnable implements Runnable {
    private TServer server;

    public TServerRunnable(TServer server) {
      this.server = server;
    }

    public void run() {
      this.server.serve();
    }
  }
}