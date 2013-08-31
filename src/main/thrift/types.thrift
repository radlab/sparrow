#
# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
  3: binary message;
}

struct TSchedulingRequest {
  1: string app;
  2: list<TTaskSpec> tasks;
  3: TUserGroupInfo user;
  # A description that will be logged alongside the requestId that Sparrow assigns.
  4: optional string description;
  # Hack to allow us to specify the probe ratio for certain types of requests.
  5: optional double probeRatio;
}

struct TEnqueueTaskReservationsRequest {
  1: string appId;
  2: TUserGroupInfo user;
  3: string requestId;
  4: THostPort schedulerAddress;
  5: i32 numTasks;
}

struct TCancelTaskReservationsRequest {
  1: string requestId;
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
