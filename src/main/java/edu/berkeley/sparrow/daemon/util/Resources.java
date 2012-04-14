package edu.berkeley.sparrow.daemon.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowConf;

/** Utilities for interrogating system resources. */
public class Resources {
  public static int getSystemMemoryMb(Configuration conf) {
    int systemMemory = -1;
    try {
      Process p = Runtime.getRuntime().exec("cat /proc/meminfo");  
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = in.readLine();
      while (line != null) {
        if (line.contains("MemTotal")) { 
          String[] parts = line.split("\\s+");
          if (parts.length > 1) {
            int memory = Integer.parseInt(parts[1]) / 1000;
            systemMemory = memory;
          }
        }
        line = in.readLine();
      }
    } catch (IOException e) {}
    if (conf.containsKey(SparrowConf.SYSTEM_MEMORY)) {
      return conf.getInt(SparrowConf.SYSTEM_MEMORY);
    } else {
      if (systemMemory != -1) {
        return systemMemory;
      } else {
        return SparrowConf.DEFAULT_SYSTEM_MEMORY;
      }
    }
  }
  
  public static int getSystemCPUCount(Configuration conf) {
    // No system interrogation yet
    return conf.getInt(SparrowConf.SYSTEM_CPUS, SparrowConf.DEFAULT_SYSTEM_CPUS);
  }
}
