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
