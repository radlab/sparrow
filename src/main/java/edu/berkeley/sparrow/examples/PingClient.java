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
