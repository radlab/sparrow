include 'types.thrift'

namespace java edu.berkeley.sparrow.thrift

# SchedulerService is used by application frontends to communicate with Sparrow
# and place jobs.
service SchedulerService {
  # Register a frontend for the given application.
  bool registerFrontend(1: string app, 2: string socketAddress);

  # Submit a job composed of a list of individual tasks. 
  bool submitJob(1: types.TSchedulingRequest req);

  # Get task placement for a specific job. If request.reserve is set to true
  # scheduling will pass through the central scheduler and resources will
  # be reserved (this will take much longer, but guarantee no     
  # preemption). Otherwise, resources are acquired opportunistically.
  list<types.TTaskPlacement> getJobPlacement(1: types.TSchedulingRequest req);

  # Send a message to be delivered to the frontend for {app} pertaining
  # to the request {request}. For now this is only a task status message.
  void sendFrontendMessage(1: string app, 2: string requestId, 3: binary message);
}

# A service used by application backends to coordinate with Sparrow.
service NodeMonitorService {
  # Register this machine as a backend for the given application.
  bool registerBackend(1: string app, 2: string listenSocket);

  # Inform the NodeMonitor of the node's current resource usage.
  void updateResourceUsage(1: string app,
                           2: map<types.TUserGroupInfo, types.TResourceVector> usage,
                           3: list<types.TFullTaskId> activeTaskIds);

  # Send a message to be delivered to the frontend for {app} pertaining
  # to the request {request}. For now this is only a task status message.
  void sendFrontendMessage(1: string app, 2: string requestId, 3: binary message);
  #
}

# A service that backends are expected to extend. Handles communication
# from a NodeMonitor.
service BackendService {  
  # Update the limit on resources allowed for this application, per user. If 
  # applications exceed their allowed usage, Sparrow may terminate the
  # backend.
  void updateResourceLimits(1: map<types.TUserGroupInfo, types.TResourceVector> resources);
  void launchTask(1: binary message, 2: types.TFullTaskId taskId,
                  3: types.TUserGroupInfo user,
                  4: types.TResourceVector estimatedResources);

}

# A service that frontends are expected to extend. Handles communication from
# a Scheduler.
service FrontendService {
  # Send a message to this frontend pertaining to {requestId} with value
  # {message}.
  void frontendMessage(1: string requestId, 2: binary message);
}

# The InternalService exposes state about application backends to:
# 1) Other Sparrow daemons
# 2) The central state store
service InternalService {
  map<string, types.TResourceUsage> getLoad(1: string app, 2: string requestId);
  bool launchTask(1: binary message, 2: types.TFullTaskId taskId,
                  3: types.TUserGroupInfo user, 
                  4: types.TResourceVector estimatedResources);
}

service SchedulerStateStoreService {
  # Message from the state store giving the scheduler new information
  void updateNodeState(1: map<string, types.TNodeState> snapshot);
}

service StateStoreService {
  # Register a scheduler with the given socket address (IP: Port)
  void registerScheduler(1: string schedulerAddress);

  # Register a node monitor with the given socket address (IP: Port)
  void registerNodeMonitor(1: string nodeMonitorAddress);
}
