package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.mortbay.log.Log;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.scheduler.TaskPlacer.TaskPlacementResponse;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.thrift.InternalService;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.launchTask_call;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskPlacement;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/**
 * This class implements the Sparrow scheduler functionality.
 */
public class Scheduler {
  private final static Logger LOG = Logger.getLogger(Scheduler.class);
  
  /**
   * A callback handler for asynchronous task launches.
   * 
   * We use the thrift event-based interface for launching tasks. In parallel, we launch
   * several tasks, then we return when all have finished launching.
   */
  private class TaskLaunchCallback implements AsyncMethodCallback<launchTask_call> {
    private CountDownLatch latch;
    public TaskLaunchCallback(CountDownLatch latch) {
      this.latch = latch;
    }
    
    @Override
    public void onComplete(launchTask_call response) {
      latch.countDown(); // TODO, see whether this was successful
    }

    @Override
    public void onError(Exception exception) {
      LOG.error("Error launching task: " + exception);
      // TODO We need to have a story here, right now since we don't decrement the latch
      // any task launch failing will mean the latch will never fully drain.
    }
  }
  
  SchedulerState state;
  TaskPlacer placer = new ProbingTaskPlacer();

  public void initialize(Configuration conf) throws IOException {
    String mode = conf.getString(SparrowConf.DEPLYOMENT_MODE, "unspecified");
    if (mode.equals("standalone")) {
      state = new StandaloneSchedulerState();
    } else if (mode.equals("configbased")) {
      state = new StateStoreSchedulerState(); // TODO figure this out
    } else {
      throw new RuntimeException("Unsupported deployment mode: " + mode);
    }
    
    state.initialize(conf);
  }
  
  public boolean registerFrontEnd(String appId) {
    LOG.debug(Logging.functionCall(appId));
    return state.watchApplication(appId);
  }

  public boolean submitJob(TSchedulingRequest req) throws TException {
    LOG.debug(Logging.functionCall(req));
    Collection<TaskPlacementResponse> placement = null;
    try {
      placement = getJobPlacementResp(req);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    CountDownLatch latch = new CountDownLatch(placement.size());
    for (TaskPlacementResponse response : placement) {
      if (!response.getClient().isPresent()) {
        throw new RuntimeException("TaskPlacer did not return thrift client.");
      }
      LOG.debug("Attempting to launch task on " + response.getNodeAddr());
      InternalService.AsyncClient client = response.getClient().get();
      client.launchTask(req.getApp(), response.getTaskSpec().message, 
         response.getTaskSpec().taskID, req.getUser(), 
         response.getTaskSpec().getEstimatedResources(), new TaskLaunchCallback(latch));
    }
    try {
      LOG.debug("Waiting for " + placement.size() + " tasks to finish launching");
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    LOG.debug("All tasks launched, returning");
    return true;
  }

  public Collection<TTaskPlacement> getJobPlacement(TSchedulingRequest req)
      throws IOException {
    LOG.debug(Logging.functionCall(req));
    // Get placement
    Collection<TaskPlacementResponse> placements = getJobPlacementResp(req);
    
    // Massage into correct Thrift output type
    Collection<TTaskPlacement> out = new HashSet<TTaskPlacement>(placements.size());
    for (TaskPlacementResponse placement : placements) {
      TTaskPlacement tPlacement = new TTaskPlacement();
      tPlacement.node = placement.getNodeAddr().toString();
      tPlacement.taskID = ByteBuffer.wrap(placement.getTaskSpec().getTaskID());
      out.add(tPlacement);
    }
    Log.debug("Returning task placement: " + out);
    return out;
  }
  
  /**
   * Internal method called by both submitJob() and getJobPlacement().
   */
  private Collection<TaskPlacementResponse> getJobPlacementResp(TSchedulingRequest req)
      throws IOException {
    LOG.debug(Logging.functionCall(req));
    String app = req.getApp();
    List<TTaskSpec> tasks = req.getTasks();
    Set<InetSocketAddress> backends = state.getBackends(app).keySet();
    List<InetSocketAddress> backendList = new ArrayList<InetSocketAddress>(backends.size());
    for (InetSocketAddress backend : backends) {
      backendList.add(backend);
    }
    return placer.placeTasks(app, backendList, tasks);
  }
}
