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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Optional;

/** Utility functions for serializing data types to/from string representation .**/
public class Serialization {
  public static Optional<InetSocketAddress> strToSocket(String in) {
    String[] parts = in.split(":");
    if (parts.length != 2) {
      return Optional.absent();
    }
    String host = parts[0];
    // This deals with the wonky way Java InetAddress toString() represents an address:
    // "hostname/IP"
    if (parts[0].contains("/")) {
      host = parts[0].split("/")[1];
    }
    try {
      return Optional.of(new InetSocketAddress(
        host, Integer.parseInt(parts[1])));
    } catch (NumberFormatException e) {
      return Optional.absent();
    }
  }
  
  public static byte[] getByteBufferContents(ByteBuffer buffer) {
    byte[] out = new byte[buffer.limit() - buffer.position()];
    buffer.get(out);
    return out;
  }
}
