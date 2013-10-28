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
