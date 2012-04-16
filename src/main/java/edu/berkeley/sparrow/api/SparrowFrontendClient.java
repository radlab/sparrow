package edu.berkeley.sparrow.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.SchedulerService.Client;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskPlacement;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Java client to Sparrow scheduling service. Once a client is initialize()'d it
 * can be used safely from multiple threads.
 */
public class SparrowFrontendClient {
  private final static Logger LOG = Logger.getLogger(SparrowFrontendClient.class);
  private final static int NUM_CLIENTS = 8; // Number of concurrent requests we support
  private final static int DEFAULT_LISTEN_PORT = 50201;
  
  BlockingQueue<SchedulerService.Client> clients = 
      new LinkedBlockingQueue<SchedulerService.Client>();
  
  /**
   * Initialize a connection to a sparrow scheduler.
   * @param sparrowSchedulerAddr. The socket address of the Sparrow scheduler.
   * @param app. The application id. Note that this must be consistent across frontends
   *             and backends.
   * @param frontendServer. A class which implements the frontend server interface (for
   *                        communication from Sparrow). 
   * @throws IOException 
   */
  public void initialize(InetSocketAddress sparrowSchedulerAddr, String app, 
      FrontendService.Iface frontendServer)
      throws TException, IOException {
    initialize(sparrowSchedulerAddr, app, frontendServer, DEFAULT_LISTEN_PORT);
  }
  
  /**
   * Initialize a connection to a sparrow scheduler.
   * @param sparrowSchedulerAddr. The socket address of the Sparrow scheduler.
   * @param app. The application id. Note that this must be consistent across frontends
   *             and backends.
   * @param frontendServer. A class which implements the frontend server interface (for
   *                        communication from Sparrow). 
   * @param listenPort. The port on which to listen for request from the scheduler.
   * @throws IOException 
   */
  public void initialize(InetSocketAddress sparrowSchedulerAddr, String app, 
      FrontendService.Iface frontendServer, int listenPort) 
      throws TException, IOException {

    FrontendService.Processor<FrontendService.Iface> processor =
        new FrontendService.Processor<FrontendService.Iface>(frontendServer);
    try {
      TServers.launchThreadedThriftServer(listenPort, 5, processor);
    } catch (IOException e) {
      LOG.fatal("Couldn't launch server side of frontend", e);
    }
    
    for (int i = 0; i < NUM_CLIENTS; i++) {
      Client client = TClients.createBlockingSchedulerClient(
          sparrowSchedulerAddr.getHostName(), sparrowSchedulerAddr.getPort());
      clients.add(client);
    }
    clients.peek().registerFrontend(app, "localhost:" + listenPort); 
  }
  
  public synchronized boolean submitJob(String app, 
      List<edu.berkeley.sparrow.thrift.TTaskSpec> tasks, TUserGroupInfo user) 
          throws TException {
    TSchedulingRequest request = new TSchedulingRequest();
    request.setTasks(tasks);
    request.setApp(app);
    request.setUser(user);
    boolean result = false;
    try {
      Client client = clients.take();
      result = client.submitJob(request);
      clients.put(client);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return result;
  }
  
  public synchronized List<TTaskPlacement> getJobPlacement(String app,
      List<TTaskSpec> tasks, TUserGroupInfo user) throws TException {
    TSchedulingRequest request = new TSchedulingRequest();
    request.setTasks(tasks);
    request.setApp(app);
    request.setUser(user);
    List<TTaskPlacement> result = null;
    try {
      Client client = clients.take();
      result = client.getJobPlacement(request);
      clients.put(client);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return result;
  }
  
  public void close() {
    for (int i = 0; i < NUM_CLIENTS; i++) {
      clients.poll().getOutputProtocol().getTransport().close();
    }
  }
}
