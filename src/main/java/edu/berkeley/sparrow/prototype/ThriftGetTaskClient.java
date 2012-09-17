package edu.berkeley.sparrow.prototype;

import java.net.InetSocketAddress;

import org.apache.thrift.async.AsyncMethodCallback;

import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.GetTaskService;
import edu.berkeley.sparrow.thrift.GetTaskService.AsyncClient;
import edu.berkeley.sparrow.thrift.GetTaskService.AsyncClient.getTask_call;
import edu.berkeley.sparrow.thrift.THostPort;

public class ThriftGetTaskClient {
  private static ThriftClientPool<GetTaskService.AsyncClient> getTaskClientPool =
      new ThriftClientPool<GetTaskService.AsyncClient>(
          new ThriftClientPool.GetTaskServiceMakerFactory());

  private static class Callback implements AsyncMethodCallback<getTask_call> {
    private InetSocketAddress address;
    private Long t0;

    Callback(InetSocketAddress address, Long t0) { this.address = address; this.t0 = t0; }

    @Override
    public void onComplete(getTask_call response) {
      try {
        getTaskClientPool.returnClient(address, (AsyncClient) response.getClient());
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
    InetSocketAddress address = new InetSocketAddress(hostname, SchedulerThrift.DEFAULT_GET_TASK_PORT);
    while (true) {
      AsyncClient client = getTaskClientPool.borrowClient(address);
      client.getTask("BOGUS_ADDR", new THostPort("foo", 1234),
          new Callback(address, System.nanoTime()));
      Thread.sleep(30);
    }
  }
}
