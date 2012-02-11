include 'types.thrift'

namespace java edu.berkeley.sparrow.thrift

# SchedulerService is used by application frontends to communicate with Sparrow
# and place jobs.
service SchedulerService {
  # Register a frontend for the given application.
  bool registerFrontend(1: string app);

  # Submit a job composed of a list of individual tasks. 
  bool submitJob(1: types.TSchedulingRequest req);

  # Get task placement for a specific job. If request.reserve is set to true
  # scheduling will pass through the central scheduler and resources will
  # be reserved (this will take much longer, but guarantee no     
  # preemption). Otherwise, resources are acquired opportunistically.
  list<types.TTaskPlacement> getJobPlacement(1: types.TSchedulingRequest req);
}

# A service used by application backends to coordinate with Sparrow.
service NodeMonitorService {
  # Register this machine as a backend for the given application.
  bool registerBackend(1: string app, 2: string listenSocket);

  # Inform the NodeMonitor of the node's current resource usage.
  void updateResourceUsage(1: string app,
                           2: map<types.TUserGroupInfo, types.TResourceVector> usage,
                           3: list<string> activeTaskIds);
}

# A service that backends are expected to extend. Handles communication
# from a NodeMonitor.
service BackendService {  
  # Update the limit on resources allowed for this application, per user. If 
  # applications exceed their allowed usage, Sparrow may terminate the
  # backend.
  void updateResourceLimits(1: map<types.TUserGroupInfo, types.TResourceVector> resources);
  void launchTask(1: binary message, 2: string requestId, 3: string taskId,
                  4: types.TUserGroupInfo user,
                  5: types.TResourceVector estimatedResources);

}

# The InternalService exposes state about application backends to:
# 1) Other Sparrow daemons
# 2) The central state store
service InternalService {
  map<string, types.TResourceVector> getLoad(1: string app, 2: string requestId);
  bool launchTask(1: string app, 2: binary message, 3: string requestId,
                  4: string taskId, 5: types.TUserGroupInfo user,
                  6: types.TResourceVector estimatedResources);
}

# Message from the state store to update scheduler state. TODO: this should
# be moved to internal interface or its own interface (maybe).
service SchedulerStateStoreService {
  void updateNodeState(1: map<string, types.TNodeState> snapshot);
}

