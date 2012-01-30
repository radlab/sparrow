package edu.berkeley.sparrow.statestore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.getLoad_call;
import edu.berkeley.sparrow.thrift.SchedulerStateStoreService;
import edu.berkeley.sparrow.thrift.SchedulerStateStoreService.AsyncClient.updateNodeState_call;
import edu.berkeley.sparrow.thrift.TNodeState;
import edu.berkeley.sparrow.thrift.TResourceVector;

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
  private static final Logger LOG = Logger.getLogger(StateStore.class);
  
  // Delay between consecutive updates to a given scheduler
  private static final int SCHEDULER_DELAY_MS = 5000;
  // Delay between consecutive queries to a given node monitor
  private static final int NODE_MANAGER_DELAY_MS = 5000;
  
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
    
    @Override
    public int compareTo(Event that) {
      // Priority queue uses this ordering to return event with earliest time.
      if (this.time > that.time) return 1;
      else if (this.time == that.time) return 0;
      return -1;
    }
  }
  
  /** Async callback for node monitor query. */
  private class NMCallBack implements AsyncMethodCallback<getLoad_call> {
    private InetSocketAddress node;
    
    public NMCallBack(InetSocketAddress node) {
      this.node = node;
    }
    
    @Override
    public void onComplete(getLoad_call response) {
      TNodeState state = new TNodeState(); // TODO: look into object reuse
      try {
        // Aggregate resource usage from all applications
        TResourceVector sparrowUsage = TResources.none();
        for (TResourceVector res : response.getResult().values()) {
          TResources.addTo(sparrowUsage, res);
        }
        state.sparrowUsage = sparrowUsage;
      } catch (TException e) {
        state.sparrowUsage = TResources.none();
      }
      state.externalUsage = TResources.none(); // TODO: set this based on
                                               //       external scheduler
      currentUsage.put(node.toString(), state);
      
      LOG.debug("Polled node monitor " + node);
      
      // Add event to poll this node manager again later
      addEvent(new Event(System.currentTimeMillis() + NODE_MANAGER_DELAY_MS,
          node, EventType.QUERY));
    }

    @Override
    public void onError(Exception e) {
      LOG.warn("Error receiving node monitor status: " + node, e);
      // Thrift clients can never be used again once an error has occurred
      internalClients.remove(node);
      addEvent(new Event(System.currentTimeMillis() + NODE_MANAGER_DELAY_MS,
          node, EventType.QUERY));
    }
  }
  
  /** Async callback for the scheduler query. */
  private class SchedulerCallback implements AsyncMethodCallback<updateNodeState_call> {
    private InetSocketAddress node;
    
    public SchedulerCallback(InetSocketAddress node) {
      this.node = node;
    }
    
    @Override
    public void onComplete(updateNodeState_call response) {
      LOG.debug("Updated scheduler: " + node);
      // Add event to update this scheduler again later
      addEvent(new Event(System.currentTimeMillis() + SCHEDULER_DELAY_MS,
          node, EventType.UPDATE));
    }
    
    @Override
    public void onError(Exception e) {
      LOG.warn("Error updaing loads on scheduler: " + node, e);
      // Thrift clients can never be used again once an error has occurred
      schedulerClients.remove(node);
      addEvent(new Event(System.currentTimeMillis() + SCHEDULER_DELAY_MS,
          node, EventType.UPDATE));
    }
  }
  
  private StateStoreState state;
  
  // For each node monitor (represented by String description of NM socket), the quantity
  // of resource usage at last check-in. This is what we broadcast to all schedulers.
  private HashMap<String, TNodeState> currentUsage = new HashMap<String, TNodeState>();
  
  // Event queue driving actions for the state store
  private PriorityBlockingQueue<Event> events = 
      new PriorityBlockingQueue<Event>();
  
  // Cache of thrift clients, currently this is never evicted
  private Map<InetSocketAddress, InternalService.AsyncClient> internalClients =
      new HashMap<InetSocketAddress, InternalService.AsyncClient>();
  private Map<InetSocketAddress, SchedulerStateStoreService.AsyncClient> schedulerClients =
      new HashMap<InetSocketAddress, SchedulerStateStoreService.AsyncClient>();

  // Thrift managers for each client group. If the same manager is passed to two or more
  // thrift client constructors, those clients process callbacks in the same thread.
  // So overall we have two worker threads, one for each thrift interface we use.
  TAsyncClientManager internalManager;
  TAsyncClientManager schedulerManager;
  
  public void initialize(Configuration conf) throws IOException {
    internalManager = new TAsyncClientManager();
    schedulerManager = new TAsyncClientManager();
    this.state = new ConfigStateStoreState();
    state.initialize(conf);
    
    // Bootstrap the event queue with queries to all node monitors we know about
    // TODO: It's not clear how this will work when the set of node monitors and
    // schedulers is changing.
    for (InetSocketAddress monitor : state.getNodeMonitors()) {
      events.add(new Event(0, monitor, EventType.QUERY));
    }
    
    // After 3 seconds (to let updates accumulate) start informing schedulers
    for (InetSocketAddress scheduler : state.getSchedulers()) {
      events.add(
          new Event(System.currentTimeMillis() + 3 * 1000, scheduler, EventType.UPDATE));
    }
    
    // Main event loop, we rely on a BlockingPriorityQueue to drive this, with some
    // extra code to make sure we don't actually poll() the queue unless the earliest
    // event actually needs to be handled. We could look into a DelayQueue for this,
    // but that is much heavier weight.
    while (true) {
      // Make sure the event queue has something which demands processing before we
      // proceed.
      synchronized (events) {
        Event event = events.peek();
        if (event == null || !eventInPast(event)) {
          try {
            Thread.sleep(10); // Sorta ugly
          } catch (InterruptedException e) { }
          continue;
        }
      }
      Event event = null;
      synchronized (events) {
        event = events.poll();
      }
      if (!eventInPast(event)) { 
        // This should never happen
        throw new RuntimeException("Signaled for future event");
      }
      switch (event.event) { 
        case QUERY:
          try {
            InternalService.AsyncClient client = getInternalClient(event.node);
            client.getLoad("*", new NMCallBack(event.node));
          } catch (IOException e) {
            LOG.warn("Failed to create thrift client to " + event.node, e);
          } catch (TException e) {
            LOG.warn("Thrift client threw exception " + event.node, e);
          }
          break;
        case UPDATE:
          try {
            SchedulerStateStoreService.AsyncClient client = 
                getSchedulerClient(event.node);
            client.updateNodeState(currentUsage, new SchedulerCallback(event.node));
          } catch (IOException e) {
            LOG.warn("Failed to create thrift client to " + event.node, e);
          } catch (TException e) {
            LOG.warn("Thrift client threw exception " + event.node, e);
          }
          break;
      }
    }
  }
  
  /** Add an event to the event queue. */
  private void addEvent(Event event) {
    synchronized (events) {
     events.add(event);
    }
  }
  
  /**
   * Return a Thrift client connected to the node monitor described by {@code addr}. 
   * This might create a new client or return a cached one.
   */
  private InternalService.AsyncClient getInternalClient(InetSocketAddress addr) 
      throws IOException {
    if (!this.internalClients.containsKey(addr)) {
      TNonblockingTransport nbTr = new TNonblockingSocket(
        addr.getHostName(), addr.getPort());
      TProtocolFactory factory = new TBinaryProtocol.Factory();
      InternalService.AsyncClient client = new InternalService.AsyncClient(
        factory, internalManager, nbTr);
      this.internalClients.put(addr, client);
    }
    return this.internalClients.get(addr);
  }
  
  /**
   * Return a Thrift client connected to the scheduler described by {@code addr}. 
   * This might create a new client or return a cached one.
   */
  private SchedulerStateStoreService.AsyncClient getSchedulerClient(
      InetSocketAddress addr) throws IOException {
    if (!this.schedulerClients.containsKey(addr)) {
      TNonblockingTransport nbTr = new TNonblockingSocket(
        addr.getHostName(), addr.getPort());
      TProtocolFactory factory = new TBinaryProtocol.Factory();
      SchedulerStateStoreService.AsyncClient client = 
          new SchedulerStateStoreService.AsyncClient(factory, schedulerManager, nbTr);
      this.schedulerClients.put(addr, client);
    }
    return this.schedulerClients.get(addr);
  }
  
  /** Return whether a given event is in the past (and ready to be processed). */
  private boolean eventInPast(Event event) {
    return (event.time < System.currentTimeMillis()); 
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
