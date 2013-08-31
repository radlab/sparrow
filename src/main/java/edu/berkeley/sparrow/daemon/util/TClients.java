/*
 * Copyright 2013 The Regents of The University California
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import edu.berkeley.sparrow.thrift.GetTaskService;
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
    return createBlockingNmClient(host, port, 0);
  }

  public static NodeMonitorService.Client createBlockingNmClient(String host, int port,
      int timeout)
      throws IOException {
    TTransport tr = new TFramedTransport(new TSocket(host, port, timeout));
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
    return createBlockingSchedulerClient(socket.getAddress().getHostAddress(), socket.getPort());
  }

  public static SchedulerService.Client createBlockingSchedulerClient(
      String host, int port) throws IOException {
    return createBlockingSchedulerClient(host, port, 0);
  }

  public static SchedulerService.Client createBlockingSchedulerClient(
      String host, int port, int timeout) throws IOException {
    TTransport tr = new TFramedTransport(new TSocket(host, port, timeout));
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

  public static GetTaskService.Client createBlockingGetTaskClient(
      InetSocketAddress socket) throws IOException {
    return createBlockingGetTaskClient(socket.getAddress().getHostAddress(), socket.getPort());
  }

  public static GetTaskService.Client createBlockingGetTaskClient(
      String host, int port) throws IOException {
    return createBlockingGetTaskClient(host, port, 0);
  }

  public static GetTaskService.Client createBlockingGetTaskClient(
      String host, int port, int timeout) throws IOException {
    TTransport tr = new TFramedTransport(new TSocket(host, port, timeout));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating scheduler client to " + host + ":" + port);
      throw new IOException(e);
    }
    TProtocol proto = new TBinaryProtocol(tr);
    GetTaskService.Client client = new GetTaskService.Client(proto);
    return client;
  }

  public static BackendService.Client createBlockingBackendClient(
      InetSocketAddress socket) throws IOException {
    return createBlockingBackendClient(socket.getAddress().getHostAddress(), socket.getPort());
  }

  public static BackendService.Client createBlockingBackendClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(new TSocket(host, port));
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
    TTransport tr = new TFramedTransport(new TSocket(host, port));
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
    return createBlockingFrontendClient(socket.getAddress().getHostAddress(), socket.getPort());
  }

  public static FrontendService.Client createBlockingFrontendClient(
      String host, int port) throws IOException {
    TTransport tr = new TFramedTransport(new TSocket(host, port));
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
