package edu.berkeley.sparrow.daemon.unused;

import java.util.List;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.SparrowDaemon;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * This interface represents a class which is capable of launching tasks on
 * remote Sparrow nodes. Its primary method is 
 * {@link #launchTasks(List<TaskSpec>) launchTasks} which will takes a list of 
 * task prefernece specifications, potentially probe specific ndoes, and launch 
 * those tasks. It is expected to block the caller during this function call.
 *
 */
public interface TaskLauncher {
  public void initialize(Configuration conf, SparrowDaemon sd);
  
  public void launchTasks(List<TTaskSpec> prefs);
}
