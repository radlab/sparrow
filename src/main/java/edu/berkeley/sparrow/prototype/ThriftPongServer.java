package edu.berkeley.sparrow.prototype;
import java.io.IOException;

import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.PongService;

public class ThriftPongServer implements PongService.Iface {

  @Override
  public String ping(String data) throws TException {
    System.out.println("Got ping: " + data);
    return "PONG";
  }

  public static void main(String[] args) throws IOException {
    PongService.Processor<PongService.Iface> pongProcessor =
        new PongService.Processor<PongService.Iface>(new ThriftPongServer());
    TServers.launchSingleThreadThriftServer(12345, pongProcessor);
  }
}
