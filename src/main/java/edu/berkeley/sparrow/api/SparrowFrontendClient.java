package edu.berkeley.sparrow.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.util.Network;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.IncompleteRequestException;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.SchedulerService.Client;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
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
      TServers.launchThreadedThriftServer(listenPort, 8, processor);
    } catch (IOException e) {
      LOG.fatal("Couldn't launch server side of frontend", e);
    }

    for (int i = 0; i < NUM_CLIENTS; i++) {
      Client client = TClients.createBlockingSchedulerClient(
          sparrowSchedulerAddr.getAddress().getHostAddress(), sparrowSchedulerAddr.getPort(),
          60000);
      clients.add(client);
    }
    clients.peek().registerFrontend(app, Network.getIPAddress(new PropertiesConfiguration())
        + ":" + listenPort);
  }

  public boolean submitJob(String app,
      List<edu.berkeley.sparrow.thrift.TTaskSpec> tasks, TUserGroupInfo user)
          throws TException {
    TSchedulingRequest request = new TSchedulingRequest(app, tasks, user);
    try {
      Client client = clients.take();
      client.submitJob(request);
      clients.put(client);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    } catch (TException e) {
      LOG.error("Thrift exception when submitting job: ");
      return false;
    } catch (IncompleteRequestException e) {
      LOG.error(e);
    }
    return true;
  }

  public boolean submitJob(String app,
      List<edu.berkeley.sparrow.thrift.TTaskSpec> tasks, TUserGroupInfo user,
      double probeRatio)
          throws TException {
    TSchedulingRequest request = new TSchedulingRequest(app, tasks, user);
    request.setProbeRatio(probeRatio);
    try {
      Client client = clients.take();
      client.submitJob(request);
      clients.put(client);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    } catch (TException e) {
      LOG.error(e);
      return false;
    } catch (IncompleteRequestException e) {
      LOG.error(e);
    }
    return true;
  }

  public void close() {
    for (int i = 0; i < NUM_CLIENTS; i++) {
      clients.poll().getOutputProtocol().getTransport().close();
    }
  }
}
