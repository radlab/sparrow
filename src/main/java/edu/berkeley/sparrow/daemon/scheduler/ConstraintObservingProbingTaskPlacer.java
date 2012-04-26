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

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TSchedulingPref;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class ConstraintObservingProbingTaskPlacer extends ProbingTaskPlacer {
  public int probesPerTask;
  
  private final static Logger LOG = 
      Logger.getLogger(ConstraintObservingProbingTaskPlacer.class);
  
  private ThriftClientPool<AsyncClient> clientPool;
  private AssignmentPolicy policy = new ConstrainedTaskAssignmentPolicy(); 
  
  @Override
  public void initialize(Configuration conf,
      ThriftClientPool<AsyncClient> clientPool) {
    this.clientPool = clientPool; 
    probesPerTask = conf.getInt(SparrowConf.SAMPLE_RATIO_CONSTRAINED, 
        SparrowConf.DEFAULT_SAMPLE_RATIO_CONSTRAINED);
    super.initialize(conf, clientPool);
  }

  @Override
  public Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes,
      Collection<TTaskSpec> tasks, TSchedulingPref schedulingPref) throws IOException {
    int probeRatio = Math.max(schedulingPref.probeRatio, probesPerTask);

    LOG.debug("Placing constrained tasks with probe ratio: " + probeRatio); 
    
    // This approximates a "randomized over constraints" approach if we get a trivial
    // probe ratio.
    if (probeRatio < 1.0) {
      Collection<InetSocketAddress> machinesToProbe = getMachinesToProbe(nodes, tasks, 3);
      Map<InetSocketAddress, TResourceUsage> mockedResources = Maps.newHashMap();
      // All machines have uniform resources
      for (InetSocketAddress socket : machinesToProbe) {
        TResourceUsage usage = new TResourceUsage();
        usage.queueLength = 0;
        usage.resources = TResources.createResourceVector(0, 0);
        mockedResources.put(socket, usage);
      }
      return policy.assignTasks(tasks, mockedResources);
    }
    
    Collection<InetSocketAddress> machinesToProbe = getMachinesToProbe(nodes, tasks, 
        probeRatio);
    CountDownLatch latch = new CountDownLatch(machinesToProbe.size());
    Map<InetSocketAddress, TResourceUsage> loads = Maps.newHashMap();
    for (InetSocketAddress machine: machinesToProbe) {
      AsyncClient client = null;
      try {
        client = clientPool.borrowClient(machine);
      } catch (Exception e) {
        LOG.fatal(e);
      }
      try {
        AUDIT_LOG.info(Logging.auditEventString("probe_launch", requestId,
            machine.getAddress().getHostAddress()));
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
    return policy.assignTasks(tasks, loads);
  }

  
  /** Return the set of machines which we want to probe for a given job. */
  private Collection<InetSocketAddress> getMachinesToProbe(
      Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks, int sampleRatio) {
    HashMap<InetAddress, InetSocketAddress> addrToSocket = Maps.newHashMap();
    Set<InetSocketAddress> probeSet = Sets.newHashSet();
    
    for (InetSocketAddress node: nodes) {
      addrToSocket.put(node.getAddress(), node);
    }
    List<TTaskSpec> unconstrainedTasks = Lists.newLinkedList();
    
    for (TTaskSpec task : tasks) {
      List<InetSocketAddress> interests = Lists.newLinkedList();
      if (task.preference != null && task.preference.nodes != null) {
        List<String> prefs = task.preference.nodes;
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
          if (myProbes >= sampleRatio) break;
        }
      } else { // We are not constrained
        unconstrainedTasks.add(task);
      }
    }
    
    List<InetSocketAddress> nodesLeft = Lists.newArrayList(
        Sets.difference(Sets.newHashSet(nodes), probeSet));
    Collections.shuffle(nodesLeft);
    int numAdditionalNodes = Math.min(unconstrainedTasks.size() * sampleRatio, 
        nodesLeft.size());
    probeSet.addAll(nodesLeft.subList(0, numAdditionalNodes));
    return probeSet;
  }
}
