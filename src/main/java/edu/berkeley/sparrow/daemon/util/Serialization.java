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
    try {
      return Optional.of(new InetSocketAddress(
        parts[0], Integer.parseInt(parts[1])));
    } catch (NumberFormatException e) {
      return Optional.absent();
    }
  }
}
