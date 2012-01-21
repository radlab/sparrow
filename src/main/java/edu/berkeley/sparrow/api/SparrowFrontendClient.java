package edu.berkeley.sparrow.api;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskPlacement;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Java client to Sparrow scheduling service.
 */
public class SparrowFrontendClient {
  private SchedulerService.Client client; // Thrift client
  
  /**
   * Initialize a connection to a sparrow scheduler.
   * @param sparrowSchedulerAddr. The socket address of the Sparrow scheduler.
   * @throws TException  // TODO throw Sparrow exception
   */
  public void initialize(InetSocketAddress sparrowSchedulerAddr, String app) 
      throws TException {
    TTransport tr = new TFramedTransport(
        new TSocket(sparrowSchedulerAddr.getHostName(),
            sparrowSchedulerAddr.getPort()));
    tr.open();
    TProtocol proto = new TBinaryProtocol(tr);
    client = new SchedulerService.Client(proto);
    client.registerFrontend(app);
  }
  
  public boolean submitJob(String app, 
      List<edu.berkeley.sparrow.thrift.TTaskSpec> tasks, TUserGroupInfo user) 
          throws TException {
    TSchedulingRequest request = new TSchedulingRequest();
    request.setTasks(tasks);
    request.setApp(app);
    request.setUser(user);
    return client.submitJob(request);
  }
  
  public List<TTaskPlacement> getJobPlacement(String app,
      List<TTaskSpec> tasks, TUserGroupInfo user) throws TException {
    TSchedulingRequest request = new TSchedulingRequest();
    request.setTasks(tasks);
    request.setApp(app);
    request.setUser(user);
    return client.getJobPlacement(request);
  }
}
