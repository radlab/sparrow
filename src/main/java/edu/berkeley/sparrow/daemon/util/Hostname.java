package edu.berkeley.sparrow.daemon.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;

public class Hostname {

  /** Return the hostname of this machine, based on configured value, or system
   * Interrogation. */
  public static String getHostName(Configuration conf) {
    String defaultHostname = null;
    try {
      defaultHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      defaultHostname = "localhost";
    }
    return conf.getString(SparrowConf.HOSTNAME, defaultHostname); 
  }
  
  /**
   * Return the IP address of this machine, as determined from the hostname
   * specified in configuration or from querying the machine.
   */
  public static String getIPAddress(Configuration conf) {
    String hostname = getHostName(conf);
    try {
      return InetAddress.getByName(hostname).getHostAddress();
    } catch (UnknownHostException e) {
      return "IP UNKNOWN";
    }
  }
}
