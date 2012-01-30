package edu.berkeley.sparrow.daemon.util;

import java.net.InetSocketAddress;

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
}
