namespace java edu.berkeley.sparrow.thrift

exception IncompleteRequestException {
  1: string message;
}

struct THostPort {
  // The host should always be represented as an IP address!
  1: string host;
  2: i32 port;
}

struct TPlacementPreference {
  1: list<string> nodes; // List of preferred nodes, described by their hostname.
  2: list<string> racks; // Not currently supported.
  3: i32 delayThreshold; // Threshold for delay scheduling (not currently supported).
}

struct TResourceVector {
  1: i64 memory;      // Memory, in Mb
  2: i32 cores;       // # Cores
}

// Conveys both a quantity of resources in use and a task queue length
struct TResourceUsage {
  1: TResourceVector resources; // Current resource usage
  2: i32 queueLength;           // Number of queued tasks
}


// A fully-specified Sparrow task has four identifiers
// neeed?
struct TFullTaskId {
  1: string taskId;    // Task ID as reported from the FE
  2: string requestId; // Scheduling request ID as assigned by the FE
  3: string appId;     // ID of the application
  4: THostPort schedulerAddress; // Address of the scheduler that scheduled the task.
}

struct TUserGroupInfo {
  1: string user;
  2: string group;
  // Priority of the user. If the node monitor is using the priority task scheduler,
  // it will place the tasks with the smallest numbered priority first.
  3: i32 priority;
}

struct TTaskSpec {
  1: string taskId;
  2: TPlacementPreference preference;
  3: TResourceVector estimatedResources;
  4: binary message;
}

struct TSchedulingRequest {
  1: string app;
  2: list<TTaskSpec> tasks;
  3: TUserGroupInfo user;
  # Hack to allow us to specify the probe ratio for certain types of requests.
  4: optional double probeRatio;
}

struct TEnqueueTaskReservationsRequest {
  1: string appId;
  2: TUserGroupInfo user;
  3: string requestId;
  4: TResourceVector estimatedResources;
  5: THostPort schedulerAddress;
  6: i32 numTasks;
}

# Information needed to launch a task.  The application and user information are not needed
# because they're included when the task is enqueued, so the node monitor already has them at
# launch time.
struct TTaskLaunchSpec {
  # Task ID (originally assigned by the application)
  1: string taskId;

  # Description of the task passed on to the application backend (opaque to Sparrow).
  2: binary message;
}

struct LoadSpec {
  1: double load;
}

# Represents the State Store's view of resource consumption on a Sparrow node.
# TODO: will include information about per-user accounting.
struct TNodeState {
  1: TResourceVector sparrowUsage;   # Resources used by Sparrow
  2: TResourceVector externalUsage;  # Resources used by other schedulers
}
