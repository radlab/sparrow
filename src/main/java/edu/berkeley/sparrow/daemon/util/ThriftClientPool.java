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
import java.util.HashMap;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool.Config;
import org.apache.log4j.Logger;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.GetTaskService;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.PongService;
import edu.berkeley.sparrow.thrift.SchedulerService;

/** A pool of nonblocking thrift async connections. */
public class ThriftClientPool<T extends TAsyncClient> {
  // Default configurations for underlying pool
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int MIN_IDLE_CLIENTS_PER_ADDR = 0;
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int EVICTABLE_IDLE_TIME_MILLIS = 1000;
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int TIME_BETWEEN_EVICTION_RUNS_MILLIS = -1;
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int MAX_ACTIVE_CLIENTS_PER_ADDR = 16;

  private static final Logger LOG = Logger.getLogger(ThriftClientPool.class);

  /** Get the configuration parameters used on the underlying client pool. */
  protected static Config getPoolConfig() {
    Config conf = new Config();
    conf.minIdle = MIN_IDLE_CLIENTS_PER_ADDR;
    conf.maxIdle = -1;
    conf.minEvictableIdleTimeMillis = EVICTABLE_IDLE_TIME_MILLIS;
    conf.timeBetweenEvictionRunsMillis = TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    conf.maxActive = MAX_ACTIVE_CLIENTS_PER_ADDR;
    conf.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
    return conf;
  }

  /** Clients need to provide an instance of this factory which is capable of creating
   * the a thrift client of type <T>. */
  public interface MakerFactory<T> {
    public T create(TNonblockingTransport tr, TAsyncClientManager mgr,
        TProtocolFactory factory);
  }

  // MakerFactory implementations for Sparrow interfaces...
  public static class InternalServiceMakerFactory
    implements MakerFactory<InternalService.AsyncClient> {
    @Override
    public InternalService.AsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new InternalService.AsyncClient(factory, mgr, tr);
    }
  }

  public static class NodeMonitorServiceMakerFactory
    implements MakerFactory<NodeMonitorService.AsyncClient> {
    @Override
    public NodeMonitorService.AsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new NodeMonitorService.AsyncClient(factory, mgr, tr);
    }
  }

  public static class SchedulerServiceMakerFactory
    implements MakerFactory<SchedulerService.AsyncClient> {
    @Override
    public SchedulerService.AsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new SchedulerService.AsyncClient(factory, mgr, tr);
    }
  }

  public static class FrontendServiceMakerFactory
    implements MakerFactory<FrontendService.AsyncClient> {
    @Override
    public FrontendService.AsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new FrontendService.AsyncClient(factory, mgr, tr);
    }
  }

  public static class GetTaskServiceMakerFactory
  implements MakerFactory<GetTaskService.AsyncClient> {
    @Override
    public GetTaskService.AsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new GetTaskService.AsyncClient(factory, mgr, tr);
    }
  }

  public static class PongServiceMakerFactory
  implements MakerFactory<PongService.AsyncClient> {
    @Override
    public PongService.AsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new PongService.AsyncClient(factory, mgr, tr);
    }
  }

  private class PoolFactory implements KeyedPoolableObjectFactory<InetSocketAddress, T> {
    // Thrift clients do not expose their underlying transports, so we track them
    // separately here to let us call close() on the transport associated with a
    // particular client.
    private HashMap<T, TNonblockingTransport> transports =
        new HashMap<T, TNonblockingTransport>();
    private MakerFactory<T> maker;

    public PoolFactory(MakerFactory<T> maker) {
      this.maker = maker;
    }

    @Override
    public void destroyObject(InetSocketAddress socket, T client) throws Exception {
      transports.get(client).close();
      transports.remove(client);
    }

    @Override
    public T makeObject(InetSocketAddress socket) throws Exception {
      TNonblockingTransport nbTr = new TNonblockingSocket(
          socket.getAddress().getHostAddress(), socket.getPort());
      TProtocolFactory factory = new TBinaryProtocol.Factory();
      T client = maker.create(nbTr, clientManager, factory);
      transports.put(client, nbTr);
      return client;
    }

    @Override
    public boolean validateObject(InetSocketAddress socket, T client) {
      return transports.get(client).isOpen();
    }

    @Override
    public void activateObject(InetSocketAddress socket, T client) throws Exception {
      // Nothing to do here
    }

    @Override
    public void passivateObject(InetSocketAddress socket, T client)
        throws Exception {
      // Nothing to do here
    }
  }

  /** Pointer to shared selector thread. */
  TAsyncClientManager clientManager;

  /** Underlying object pool. */
  private GenericKeyedObjectPool<InetSocketAddress, T> pool;

  public ThriftClientPool(MakerFactory<T> maker) {
    pool = new GenericKeyedObjectPool<InetSocketAddress, T>(new PoolFactory(maker),
        getPoolConfig());
    try {
      clientManager = new TAsyncClientManager();
    } catch (IOException e) {
      LOG.fatal(e);
    }
  }

  /** Constructor (for unit tests) which overrides default configuration. */
  protected ThriftClientPool(MakerFactory<T> maker, Config conf) {
    this(maker);
    pool.setConfig(conf);
  }

  /** Borrows a client from the pool. */
  public T borrowClient(InetSocketAddress socket)
      throws Exception {
    return pool.borrowObject(socket);
  }

  /** Returns a client to the pool. */
  public void returnClient(InetSocketAddress socket, T client)
      throws Exception {
    pool.returnObject(socket, client);
  }

  public int getNumActive(InetSocketAddress socket) {
    return pool.getNumActive(socket);
  }

  public int getNumIdle(InetSocketAddress socket) {
    return pool.getNumIdle(socket);
  }
}
