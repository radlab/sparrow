package edu.berkeley.sparrow.statestore;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.configuration.Configuration;

/**
  /***
   * A class for storing coordinating information used by the {@link StateStore}.
   * 
   * The state store is primarily responsible for:
   *   1) Querying NodeMonitors for information
   *   2) Pushing that information to Schedulers
   *   
   * This class mocks out tracking of the schedulers and node monitors. This is helpful
   * if we want to assign a static list of schedulers and node monitors, or if we want
   * to allow some persistent storage of this information across state store
   * instantiations.  
   */
public interface StateStoreState {
  /** Initialize this state. */
  void initialize(Configuration conf);
  
  /**
   * Return the Sparrow internal interface address of known scheduling nodes.
   * 
   * This is called when the StateStore initializes.
   */
  List<InetSocketAddress> getInitialSchedulers();
  
  /**
   * Return the Sparrow internal interface address of known node monitors.
   * 
   * This is called when the StateStore initializes.
   */
  List<InetSocketAddress> getInitialNodeMonitors();
  
  /**
   * Signal that the state store has lost touch with a scheduler.
   */
  void signalInactiveScheduler(InetSocketAddress scheduler);
  
  /**
   * Signal that the state store has lost touch with a node monitor.
   */
  void signalInactiveNodeMonitor(InetSocketAddress nodeMonitor);
  
  /**
   * Signal that the state store has become aware of a new scheduler.
   */
  void signalActiveScheduer(InetSocketAddress scheduler);
  
  /**
   * Signal that the state store has become aware of a new node monitor.
   */
  void signalActiveNodeMonitor(InetSocketAddress nodeMonitor);
  
}
