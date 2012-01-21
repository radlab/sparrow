package edu.berkeley.sparrow.daemon;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;

/**
 * Class representing the Sparrow daemon. This stores state about the frontends
 * and backends running on the particular node and global cluster state,
 * such as the backends running on other nodes.
 */
public class SparrowDaemon {
  
  private SchedulerThrift scheduler;
  private NodeMonitorThrift nodeMonitor;
  
  public void initialize(Configuration conf) throws Exception {    
    // Start thrift daemons for scheduler and nodemonitor services
    scheduler = new SchedulerThrift();
    scheduler.initialize(conf);
    nodeMonitor = new NodeMonitorThrift();
    nodeMonitor.initialize(conf);
  }
  
  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    parser.accepts("c", "configuration file (required)").
      withRequiredArg().ofType(String.class);
    parser.accepts("help", "print help statement");
    OptionSet options = parser.parse(args);
    
    if (options.has("help") || !options.has("c")) {
      parser.printHelpOn(System.out);
      System.exit(-1);
    }
    
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
        
    String configFile = (String) options.valueOf("c");
    Configuration conf = new PropertiesConfiguration(configFile);
    SparrowDaemon sparrowDaemon = new SparrowDaemon();
    sparrowDaemon.initialize(conf);
  }
}
