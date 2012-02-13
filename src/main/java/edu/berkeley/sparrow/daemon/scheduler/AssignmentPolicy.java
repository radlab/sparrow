package edu.berkeley.sparrow.daemon.scheduler;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * An assignment policy reflects a strategy for assigning tasks to machines given
 * full information about the set of machines (and corresponding capacities) and the
 * set of tasks. This is usually used by a {@link TaskPlacer} once the work of collecting
 * information about the remote machines has been completed.
 */
public interface AssignmentPolicy {
  /** Return a list of placement responses assigning {@code tasks} to the list of
   * {@code nodes} provided.
   */
  public Collection<TaskPlacementResponse> assignTasks(Collection<TTaskSpec> tasks, 
      Map<InetSocketAddress, TResourceVector> nodes);
}
