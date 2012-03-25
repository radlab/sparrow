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

import edu.berkeley.sparrow.thrift.InternalService;

/** A pool of nonblocking thrift async connections. */
public class ThriftClientPool<T extends TAsyncClient> {
  // Default configurations for underlying pool
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int MIN_IDLE_CLIENTS_PER_ADDR = 0;
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int EVICTABLE_IDLE_TIME_MS = 1000;
  /** See {@link GenericKeyedObjectPool.Config} */
  public static int TIME_BETWEEN_EVICTION_RUNS_MILLIS = 1000;
  
  private static final Logger LOG = Logger.getLogger(ThriftClientPool.class);
  
  /** Get the configuration parameters used on the underlying client pool. */
  protected static Config getPoolConfig() {
    Config conf = new Config();
    conf.minIdle = MIN_IDLE_CLIENTS_PER_ADDR;
    conf.minEvictableIdleTimeMillis = EVICTABLE_IDLE_TIME_MS;
    conf.timeBetweenEvictionRunsMillis = TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    return conf;
  }
  
  /** Clients need to provide an instance of this factory which is capable of creating
   * the a thrift client of type <T>. */
  public interface MakerFactory<T> {
    public T create(TNonblockingTransport tr, TAsyncClientManager mgr, 
        TProtocolFactory factory);
  }
  
  // MakerFactory implementations for Sparrow interfaces...
  public static class InernalServiceMakerFactory 
    implements MakerFactory<InternalService.AsyncClient> {
    @Override
    public InternalService.AsyncClient create(TNonblockingTransport tr, 
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return new InternalService.AsyncClient(factory, mgr, tr);
    }
  }
  
  private class PoolFactory implements KeyedPoolableObjectFactory<InetSocketAddress, T> {
    // Thrift clients to not expose their underlying transports, so we track them
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
          socket.getHostName(), socket.getPort());
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
    pool = new GenericKeyedObjectPool<InetSocketAddress, T>(new PoolFactory(maker));
    pool.setConfig(getPoolConfig());
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
  
  protected int getNumActive(InetSocketAddress socket) {
    return pool.getNumActive(socket);
  }
  
  protected int getNumIdle(InetSocketAddress socket) {
    return pool.getNumIdle(socket);
  }
}