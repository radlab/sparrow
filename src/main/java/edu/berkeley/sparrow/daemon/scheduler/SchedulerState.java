package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.commons.configuration.Configuration;

/**
 * State storage for the Sparrow {@link Scheduler}. This is stored in its
 * own class since state storage may involve communicating to external services
 * such as ZooKeeper.
 */
public interface SchedulerState {

  /**
   * Initialize state storage. This should open connections to any external
   * services if required.
   * @throws IOException
   */
  public void initialize(Configuration conf) throws IOException;

  /**
   * Signal that state storage will be queried for information about
   * {@code appId} in the future.
   */
  public boolean watchApplication(String appId);

  /**
   * Get the backends available for a particular application. Each backend includes a
   * resource vector giving current available resources. TODO: this might be changed
   * to include more detailed information per-node.
   */
  public Set<InetSocketAddress> getBackends(String appId);
}
