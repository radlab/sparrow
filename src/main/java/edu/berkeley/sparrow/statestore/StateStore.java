package edu.berkeley.sparrow.statestore;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;

import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.SchedulerStateStoreService;
import edu.berkeley.sparrow.thrift.TNodeState;

/**
 * The State Store is the key centralized component of Sparrow. It periodically updates
 * remote Schedulers with information about resource usage on each machine. This includes
 * stale estimates of what Sparrow traffic is being run on the machine, in addition to 
 * information about external workloads, such as those observed through a central 
 * scheduler.
 * 
 * NOTE: currently this is only a stub implementation and does not live up to the above
 *       doc-string
 */
public class StateStore {
  private static enum EventType { QUERY, UPDATE };
  /** Micro event queue lets us schedule node check-ins at arbitrary intervals. **/
  private class Event implements Comparable<Event> {
    public long time;               // When this event is scheduled for
    public InetSocketAddress node;  // Which socket should this contact
    public EventType event;         // Whether this is a load query or update
    
    public Event(long time, InetSocketAddress node, EventType event) {
      this.time = time;
      this.node = node;
      this.event = event;
    }
    
    public int compareTo(Event that) {
      if (this.time > that.time) return 1;
      else if (this.time == that.time) return 0;
      return -1;
    }
  }
  private StateStoreState state;
  
  // For each node, the quantity of resource usage at last check-in. This is what we
  // broadcast to all nodes.
  private HashMap<InetAddress, TNodeState> currentUsage;
  
  // Event queue dictating when a state store should contact other nodes
  private PriorityQueue<Event> events = 
      new PriorityQueue<Event>();
  
  // Thrift clients
  private Map<InetAddress, InternalService.AsyncIface> internalClients =
      new HashMap<InetAddress, InternalService.AsyncIface>();
  private Map<InetAddress, SchedulerStateStoreService.AsyncIface> schedulerClients =
      new HashMap<InetAddress, SchedulerStateStoreService.AsyncIface>();
  
  public void initialize(Configuration conf) {
    this.state = new ConfigStateStoreState();
    for (InetSocketAddress monitor : state.getNodeMonitors()) {
      events.add(new Event(0, monitor, EventType.QUERY));
    }
    
    // After a few seconds (to let updates accumulate) start informing schedulers
    for (InetSocketAddress scheduler : state.getSchedulers()) {
      events.add(
          new Event(System.currentTimeMillis() + 10 * 1000, scheduler, EventType.UPDATE));
    }
    
    // We basically want to block on having events which are in the future
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
    StateStore stateStore = new StateStore();
    stateStore.initialize(conf);
  }
  
  
}
