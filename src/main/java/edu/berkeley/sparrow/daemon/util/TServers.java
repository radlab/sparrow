package edu.berkeley.sparrow.daemon.util;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

/***
 * Helper functions for dispatching Thrift servers.
 */
public class TServers {
  private final static Logger LOG = Logger.getLogger(TServers.class);

  /**
   * Launch a single threaded nonblocking IO server. All requests to this server will be
   * handled in a single thread, so its requests should not contain blocking functions.
   */
  public static void launchSingleThreadThriftServer(int port, TProcessor processor)
      throws IOException {
    LOG.info("Staring async thrift server of type: " + processor.getClass().toString()
        + " on port " + port);
    TNonblockingServerTransport serverTransport;
    try {
      serverTransport = new TNonblockingServerSocket(port);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
    serverArgs.processor(processor);
    TServer server = new TNonblockingServer(serverArgs);
    new Thread(new TServerRunnable(server)).start();
  }

  /**
   * Launch a multi-threaded Thrift server with the given {@code processor}. Note that
   * internally this creates an expanding thread pool of at most {@code threads} threads,
   * and requests are queued whenever that thread pool is saturated.
   */
  public static void launchThreadedThriftServer(int port, int threads,
      TProcessor processor) throws IOException {
    LOG.info("Staring async thrift server of type: " + processor.getClass().toString()
    		+ " on port " + port + " with " + threads + " threads.");
    TServerSocket serverSocket;
    try {
      serverSocket = new TServerSocket(port);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverSocket);
    serverArgs.processor(processor);
    /* TThreadPoolServer uses a ThreadPoolExecutor under the hood; minWorkerThreads sets
     * the corePoolSize for the ThreadPoolExecutor and maxWorkerThreads sets the maxPoolSize.
     * When the ThreadPoolExecutor gets a new task to execute and fewer than corePoolSize
     * threads are running, it will create a new thread to handle the task, even if there are other
     * idle threads.  Setting maxWorkerThreads = minWorkerThreads creates a fixed-size thread
     * pool.
     */
    serverArgs.minWorkerThreads(threads);
    serverArgs.maxWorkerThreads(threads);
    /* Need to be sure to use a framed transport, so that the server is compatible with the
     * asynchronous client. Because the server uses a framed transport, all clients must used
     * framed transports as well. */
    serverArgs.inputTransportFactory(new TFramedTransport.Factory());
    serverArgs.outputTransportFactory(new TFramedTransport.Factory());
    TServer server = new TThreadPoolServer(serverArgs);
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