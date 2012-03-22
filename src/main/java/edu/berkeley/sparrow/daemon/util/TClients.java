package edu.berkeley.sparrow.daemon.util;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.StateStoreService;

/**
 * Helper functions for creating Thrift clients for various Sparrow interfaces.
 */
public class TClients {
  private final static Logger LOG = Logger.getLogger(TClients.class);
  
  public static NodeMonitorService.Client createBlockingNmClient(String host, int port) 
      throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating node monitor client to " + host + ":" + port);
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    NodeMonitorService.Client client = new NodeMonitorService.Client(proto);
    return client;
  }
  
  public static SchedulerService.Client createBlockingSchedulerClient(
      InetSocketAddress socket) throws IOException {
    return createBlockingSchedulerClient(socket.getHostName(), socket.getPort());
  }
  
  public static SchedulerService.Client createBlockingSchedulerClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating scheduler client to " + host + ":" + port);
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    SchedulerService.Client client = new SchedulerService.Client(proto);
    return client;
  }
  
  public static BackendService.Client createBlockingBackendClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating backend client to " + host + ":" + port);
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    BackendService.Client client = new BackendService.Client(proto);
    return client;
  }
  
  public static StateStoreService.Client createBlockingStateStoreClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating state store client to " + host + ":" + port);
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    StateStoreService.Client client = new StateStoreService.Client(proto);
    return client;
  }
  
  public static FrontendService.Client createBlockingFrontendClient(
      InetSocketAddress socket) throws IOException {
    return createBlockingFrontendClient(socket.getHostName(), socket.getPort());
  }
  
  public static FrontendService.Client createBlockingFrontendClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating state store client to " + host + ":" + port);
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    FrontendService.Client client = new FrontendService.Client(proto);
    return client;
  }
}
