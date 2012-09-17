package edu.berkeley.sparrow.prototype;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class PongServer {
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(12345);
    while (true) {
      Socket sock = serverSocket.accept();
      sock.setTcpNoDelay(true);
      new Thread(new PongServerRunnable(sock)).start();
    }
  }

  private static class PongServerRunnable implements Runnable {
    private Socket sock;

    PongServerRunnable(Socket sock) { this.sock = sock; }

    @Override
    public void run() {
      byte[] buff = new byte[1000];
      while(true) {
        try {
          sock.getInputStream().read(buff);
        } catch (IOException e) {
          e.printStackTrace();
        }
        System.out.println("GOT: " + new String(buff));
        try {
          sock.getOutputStream().write("PONG".getBytes());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
}
