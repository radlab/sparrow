package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * An assignment policy reflects a strategy for assigning tasks to machines given
 * full information about the set of machines (and corresponding capacities) and the
 * set of tasks. This is usually used by a {@link TaskPlacer} once the work of collecting
 * information about the remote machines has been completed.
 */
public interface AssignmentPolicy {
  /** 
   * Return a list of placement responses assigning {@code tasks} to the list of
   * {@code nodes} provided. Note that a policy may or may not modify elements of the
   * nodes list (for instance, it might increase the resource usage of certain nodes
   * to reflect task assignment).
   */
  public Collection<TaskPlacementResponse> assignTasks(Collection<TTaskSpec> tasks, 
      Map<InetSocketAddress, TResourceUsage> nodes);
}
