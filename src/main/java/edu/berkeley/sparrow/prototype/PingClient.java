package edu.berkeley.sparrow.prototype;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class PingClient {
  public static void main(String[] args) throws UnknownHostException, IOException {
    String server = args[0];
    Socket socket = new Socket();
    socket.setTcpNoDelay(true);
    socket.connect(new InetSocketAddress(server, 12345));
    byte[] buff = new byte[1000];
    while (true) {
      long t0 = System.nanoTime();
      socket.getOutputStream().write("PING".getBytes());
      socket.getInputStream().read(buff);
      double diffMs = (System.nanoTime() - t0) / (1000.0 * 1000.0);
      System.out.println("GOT: " + new String(buff));
      System.out.println(diffMs + " milliseconds");
    }
  }
}
