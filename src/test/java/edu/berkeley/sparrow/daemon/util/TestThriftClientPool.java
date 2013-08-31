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
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;

import org.apache.commons.pool.impl.GenericKeyedObjectPool.Config;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;
import org.junit.Test;

import edu.berkeley.sparrow.daemon.util.ThriftClientPool.MakerFactory;

public class TestThriftClientPool {

  private class MockedMakerFactory implements MakerFactory<TAsyncClient> {
    @Override
    public TAsyncClient create(TNonblockingTransport tr,
        TAsyncClientManager mgr, TProtocolFactory factory) {
      return mock(TAsyncClient.class);
    }
  }
  
  /** Test a very common scenario where we make two thrift function calls to the same
      node in rapid succession. This test ensures that a single client is created and 
      used for both calls. */
  @Test
  public void ensureConnectionReUsed() throws Exception {
    InetSocketAddress sock = new InetSocketAddress(12345);
    ThriftClientPool<TAsyncClient> pool = new ThriftClientPool<TAsyncClient>(
        new MockedMakerFactory());
    assertEquals(0, pool.getNumIdle(sock));
    assertEquals(0, pool.getNumActive(sock));

    TAsyncClient client1 = pool.borrowClient(sock);
    
    assertEquals(0, pool.getNumIdle(sock));
    assertEquals(1, pool.getNumActive(sock));
    
    pool.returnClient(sock, client1);
    
    assertEquals(1, pool.getNumIdle(sock));
    assertEquals(0, pool.getNumActive(sock));
    
    TAsyncClient client2 = pool.borrowClient(sock);

    assertEquals(0, pool.getNumIdle(sock));
    assertEquals(1, pool.getNumActive(sock));
    
    assertEquals(client1, client2);
  }
  
  @Test
  public void testPoolExpiration() throws Exception {
    // Makes sure that a thrift client gets evicted (and therefore closed) if it is not 
    // used for a certain amount of time.
    Config conf = ThriftClientPool.getPoolConfig();
    
    // We decrease the defaults here so the test runs in reasonable time
    conf.minEvictableIdleTimeMillis = 10;
    conf.timeBetweenEvictionRunsMillis = 50;
    
    InetSocketAddress sock = new InetSocketAddress(12345);    
    
    ThriftClientPool<TAsyncClient> pool = new ThriftClientPool<TAsyncClient>(
        new MockedMakerFactory(), conf);
    
    assertEquals(0, pool.getNumIdle(sock));
    assertEquals(0, pool.getNumActive(sock));
    
    TAsyncClient client = pool.borrowClient(sock);
    
    assertEquals(0, pool.getNumIdle(sock));
    assertEquals(1, pool.getNumActive(sock));
    
    pool.returnClient(sock, client);
    
    assertEquals(1, pool.getNumIdle(sock));
    assertEquals(0, pool.getNumActive(sock));
    
    Thread.sleep(conf.timeBetweenEvictionRunsMillis * 2);
    
    assertEquals(0, pool.getNumIdle(sock));
    assertEquals(0, pool.getNumActive(sock));
  }
}
