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

package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.TCancelTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.TEnqueueTaskReservationsRequest;
import edu.berkeley.sparrow.thrift.TFullTaskId;

/**
 * This class extends the thrift Sparrow node monitor interface. It wraps the
 * {@link NodeMonitor} class and delegates most calls to that class.
 */
public class NodeMonitorThrift implements NodeMonitorService.Iface,
                                          InternalService.Iface {
  // Defaults if not specified by configuration
  public final static int DEFAULT_NM_THRIFT_PORT = 20501;
  public final static int DEFAULT_NM_THRIFT_THREADS = 32;
  public final static int DEFAULT_INTERNAL_THRIFT_PORT = 20502;
  public final static int DEFAULT_INTERNAL_THRIFT_THREADS = 8;

  private NodeMonitor nodeMonitor = new NodeMonitor();
  // The socket addr (ip:port) where we listen for requests from other Sparrow daemons.
  // Used when registering backends with the state store.
  private InetSocketAddress internalAddr;

  /**
   * Initialize this thrift service.
   *
   * This spawns 2 multi-threaded thrift servers, one exposing the app-facing
   * agent service and the other exposing the internal-facing agent service,
   * and listens for requests to both servers. We require explicit specification of the
   * ports for these respective interfaces, since they cannot always be determined from
   * within this class under certain configurations (e.g. a config file specifies
   * multiple NodeMonitors).
   */
  public void initialize(Configuration conf, int nmPort, int internalPort)
      throws IOException {
    nodeMonitor.initialize(conf, internalPort);

    // Setup application-facing agent service.
    NodeMonitorService.Processor<NodeMonitorService.Iface> processor =
        new NodeMonitorService.Processor<NodeMonitorService.Iface>(this);

    int threads = conf.getInt(SparrowConf.NM_THRIFT_THREADS,
        DEFAULT_NM_THRIFT_THREADS);
    TServers.launchThreadedThriftServer(nmPort, threads, processor);

    // Setup internal-facing agent service.
    InternalService.Processor<InternalService.Iface> internalProcessor =
        new InternalService.Processor<InternalService.Iface>(this);
    int internalThreads = conf.getInt(
        SparrowConf.INTERNAL_THRIFT_THREADS,
        DEFAULT_INTERNAL_THRIFT_THREADS);
    TServers.launchThreadedThriftServer(internalPort, internalThreads, internalProcessor);

    internalAddr = new InetSocketAddress(InetAddress.getLocalHost(), internalPort);
  }

  @Override
  public boolean registerBackend(String app, String backendSocket) throws TException {
    Optional<InetSocketAddress> backendAddr = Serialization.strToSocket(backendSocket);
    if (!backendAddr.isPresent()) {
      return false; // TODO: maybe we should throw some exception here?
    }
    return nodeMonitor.registerBackend(app, internalAddr, backendAddr.get());
  }

  @Override
  public void sendFrontendMessage(String app, TFullTaskId taskId,
      int status, ByteBuffer message) throws TException {
    nodeMonitor.sendFrontendMessage(app, taskId, status, message);
  }

  @Override
  public void tasksFinished(List<TFullTaskId> tasks) throws TException {
    nodeMonitor.tasksFinished(tasks);
  }

  @Override
  public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request)
      throws TException {
    return nodeMonitor.enqueueTaskReservations(request);
  }

  @Override
  public void cancelTaskReservations(TCancelTaskReservationsRequest request)
      throws TException {
    nodeMonitor.cancelTaskReservations(request.requestId);
  }
}
