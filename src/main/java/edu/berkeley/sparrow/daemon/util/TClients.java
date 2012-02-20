package edu.berkeley.sparrow.daemon.util;

import java.io.IOException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.SchedulerService;

/**
 * Helper functions for creating Thrift clients for various Sparrow interfaces.
 */
public class TClients {
  public static NodeMonitorService.Client createBlockingNmClient(String host, int port) 
      throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    NodeMonitorService.Client client = new NodeMonitorService.Client(proto);
    return client;
  }
  
  public static SchedulerService.Client createBlockingSchedulerClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(
        new TSocket(host, port));
    try {
      tr.open();
    } catch (TTransportException e) {
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
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    BackendService.Client client = new BackendService.Client(proto);
    return client;
  }
}
