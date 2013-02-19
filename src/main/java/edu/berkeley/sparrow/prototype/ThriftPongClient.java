package edu.berkeley.sparrow.prototype;

import java.net.InetSocketAddress;

import org.apache.thrift.async.AsyncMethodCallback;

import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.PongService;
import edu.berkeley.sparrow.thrift.PongService.AsyncClient;
import edu.berkeley.sparrow.thrift.PongService.AsyncClient.ping_call;

public class ThriftPongClient {
  private static ThriftClientPool<PongService.AsyncClient> pongClientPool =
      new ThriftClientPool<PongService.AsyncClient>(
          new ThriftClientPool.PongServiceMakerFactory());

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
        System.out.println("ERROR!!!");
      }
    }

    @Override
    public void onError(Exception exception) {
      System.out.println("ERROR!!!");
    }

  }

  public static void main(String[] args) throws Exception {
    String hostname = args[0];
    InetSocketAddress address = new InetSocketAddress(hostname, 12345);
    while (true) {
      Long t = System.nanoTime();
      AsyncClient client = pongClientPool.borrowClient(address);
      System.out.println("Getting client took " + ((System.nanoTime() - t) / (1000000)));
      client.ping("PING", new Callback(address, System.nanoTime()));
      Thread.sleep(30);
    }
  }
}
