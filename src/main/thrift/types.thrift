namespace java edu.berkeley.sparrow.thrift

struct TPlacementPreference {
  1: list<string> nodes;
  2: list<string> racks;
  3: i32 delayThreshold; // Threshold for delay scheduling
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
struct TFullTaskId {
  1: string taskId;    // Task ID as reported from the FE
  2: string requestId; // Scheduling request ID as assigned by the FE
  3: string appId;     // ID of the application
  4: string frontendSocket; // Host:Port of the sparrow frontend
}

struct TUserGroupInfo {
  1: string user;
  2: string group;
}

struct TTaskSpec {
  1: string taskID;
  2: TPlacementPreference preference;
  3: TResourceVector estimatedResources;
  4: optional binary message;
}

enum TSchedulingPref {
  DEFAULT,
  SPREAD # Try to spread tasks across machines
}

struct TSchedulingRequest {
  1: string app;
  2: list<TTaskSpec> tasks;
  3: TUserGroupInfo user;
  4: optional bool reserve;
  5: optional TSchedulingPref schedulingPref; 
}

struct TTaskPlacement {
  1: string taskID;
  2: string node;
  3: optional binary message;
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
