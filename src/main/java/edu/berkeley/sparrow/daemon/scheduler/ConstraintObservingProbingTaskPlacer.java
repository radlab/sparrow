package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class ConstraintObservingProbingTaskPlacer extends ProbingTaskPlacer {
  public static int PROBES_PER_TASK = 2; // TODO make this configurable
  
  private final static Logger LOG = 
      Logger.getLogger(ConstraintObservingProbingTaskPlacer.class);
  
  private ThriftClientPool<AsyncClient> clientPool;
  
  @Override
  public void initialize(Configuration conf,
      ThriftClientPool<AsyncClient> clientPool) {
    this.clientPool = clientPool; 
    super.initialize(conf, clientPool);
  }

  @Override
  public Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes,
      Collection<TTaskSpec> tasks) throws IOException {
    Collection<InetSocketAddress> machinesToProbe = getMachinesToProbe(nodes, tasks);
    CountDownLatch latch = new CountDownLatch(machinesToProbe.size());
    Map<InetSocketAddress, TResourceVector> loads = Maps.newHashMap();
    for (InetSocketAddress machine: machinesToProbe) {
      AsyncClient client = null;
      try {
        client = clientPool.borrowClient(machine);
      } catch (Exception e) {
        LOG.fatal(e);
      }
      try {
        client.getLoad(appId, requestId, 
            new ProbeCallback(machine, loads, latch, appId, requestId, client));
      } catch (TException e) {
        LOG.fatal(e);
      }
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    AssignmentPolicy policy = new ConstrainedTaskAssignmentPolicy();
    return policy.assignTasks(tasks, loads);
  }

  
  /** Return the set of machines which we want to probe for a given job. */
  private Collection<InetSocketAddress> getMachinesToProbe(
      Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks) {
    HashMap<InetAddress, InetSocketAddress> addrToSocket = Maps.newHashMap();
    Set<InetSocketAddress> probeSet = Sets.newHashSet();
    
    for (InetSocketAddress node: nodes) {
      addrToSocket.put(node.getAddress(), node);
    }
    List<TTaskSpec> unconstrainedTasks = Lists.newLinkedList();
    for (TTaskSpec task : tasks) {
      List<InetSocketAddress> interests = Lists.newLinkedList();
      if (task.preference != null && task.preference.nodes != null) {
        for (String node : task.preference.nodes) {
          try {
            InetAddress addr = InetAddress.getByName(node);
            if (addrToSocket.containsKey(addr)) {
              interests.add(addrToSocket.get(addr));
            } else {
              LOG.warn("Got placement constraint for unknown node " + node);
            }
          } catch (UnknownHostException e) {
            LOG.warn("Got placement constraint for unresolvable node " + node);
          }
        }
      }
      // We have constraints
      if (interests.size() > 0) {
        int myProbes = 0;
        for (InetSocketAddress addr: interests) {
          if (!probeSet.contains(addr)) { 
            probeSet.add(addr); 
            myProbes++;
          }
          if (myProbes >= PROBES_PER_TASK) break;
        }
      } else { // We are not constrained
        unconstrainedTasks.add(task);
      }
    }
    
    List<InetSocketAddress> nodesLeft = Lists.newArrayList(
        Sets.difference(Sets.newHashSet(nodes), probeSet));
    Collections.shuffle(nodesLeft);
    int numAdditionalNodes = Math.min(unconstrainedTasks.size() * PROBES_PER_TASK, 
        nodesLeft.size());
    probeSet.addAll(nodesLeft.subList(0, numAdditionalNodes));
    return probeSet;
  }
}
