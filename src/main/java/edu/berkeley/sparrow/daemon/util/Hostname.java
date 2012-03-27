package edu.berkeley.sparrow.daemon.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;

public class Hostname {

  public static String getHostName(Configuration conf) {
    String defaultHostname = null;
    try {
      defaultHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      defaultHostname = "localhost";
    }
    return conf.getString(SparrowConf.HOSTNAME, defaultHostname); 
  }
}
