namespace java edu.berkeley.sparrow.thrift

struct TPlacementPreference {
  1: list<string> nodes;
  2: list<string> racks;
  3: i32 delayThreshold; // Threshold for delay scheduling
}

struct TResourceVector {
  1: i64 memory; // Memory, in Mb
  2: i32 cores;    // # Cores
}

struct TUserGroupInfo {
  1: string user;
  2: string group;
}

struct TTaskSpec {
  1: binary taskID;
  2: TPlacementPreference preference;
  3: TResourceVector estimatedResources;
  4: optional binary message;
}

struct TTaskPlacement {
  1: binary taskID;
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
