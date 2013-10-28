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

package edu.berkeley.sparrow.examples;

import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.PongService;
import edu.berkeley.sparrow.thrift.PongService.AsyncClient;
import edu.berkeley.sparrow.thrift.PongService.AsyncClient.ping_call;

public class ThriftPongClient {
  private static ThriftClientPool<PongService.AsyncClient> pongClientPool =
      new ThriftClientPool<PongService.AsyncClient>(
          new ThriftClientPool.PongServiceMakerFactory());
  private static boolean USE_SYNCHRONOUS_CLIENT = true;

  private static class Callback implements AsyncMethodCallback<ping_call> {
    private InetSocketAddress address;
    private Long t0;

    Callback(InetSocketAddress address, Long t0) { this.address = address; this.t0 = t0; }

    @Override
    public void onComplete(ping_call response) {
      try {
        pongClientPool.returnClient(address, (AsyncClient) response.getClient());
        System.out.println("Took: " + (System.nanoTime() - t0) / (1000.0 * 1000.0) + "ms");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onError(Exception exception) {
      exception.printStackTrace();
    }

  }

  private static void pongUsingSynchronousClient(String hostname)
      throws TException, InterruptedException {
    TTransport tr = new TFramedTransport(new TSocket(hostname, 12345));
    tr.open();
    TProtocol proto = new TBinaryProtocol(tr);
    PongService.Client client = new PongService.Client(proto);
    while (true) {
      Long t = System.nanoTime();
      client.ping("PING");
      System.out.println("Took: " + (System.nanoTime() - t) / (1000.0 * 1000.0) + "ms");
      Thread.sleep(500);
    }
  }

  private static void pongUsingAsynchronousClient(String hostname) throws Exception {
    InetSocketAddress address = new InetSocketAddress(hostname, 12345);
    while (true) {
      Long t = System.nanoTime();
      AsyncClient client = pongClientPool.borrowClient(address);
      System.out.println("Getting client took " + ((System.nanoTime() - t) / (1000 * 1000)) +
                         "ms");
      client.ping("PING", new Callback(address, System.nanoTime()));
      Thread.sleep(30);
    }
  }

  public static void main(String[] args) throws Exception {
    String hostname = args[0];

    if (USE_SYNCHRONOUS_CLIENT) {
      pongUsingSynchronousClient(hostname);
    } else {
      pongUsingAsynchronousClient(hostname);
    }
  }
}
