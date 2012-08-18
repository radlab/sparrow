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
  # to the task {taskId}. The {status} field allows for application-specific
  # status enumerations. Right now this is used only for Spark, which relies on
  # the scheduler to send task completion messages to frontends.
  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId, 
                           3: i32 status, 4: binary message);
}

# A service used by application backends to coordinate with Sparrow.
service NodeMonitorService {
  # Register this machine as a backend for the given application.
  bool registerBackend(1: string app, 2: string listenSocket);

  # Inform the NodeMonitor that a particular task has finished
  void tasksFinished(1: list<types.TFullTaskId> tasks);

  # See SchedulerService.sendFrontendMessage
  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId,
                           3: i32 status, 4: binary message);
}

# A service that backends are expected to extend. Handles communication
# from a NodeMonitor.
service BackendService {  
  void launchTask(1: binary message, 2: types.TFullTaskId taskId,
                  3: types.TUserGroupInfo user,
                  4: types.TResourceVector estimatedResources);

}

# A service that frontends are expected to extend. Handles communication from
# a Scheduler.
service FrontendService {
  # See SchedulerService.sendFrontendMessage
  void frontendMessage(1: types.TFullTaskId taskId, 2: i32 status, 
                       3: binary message);
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
