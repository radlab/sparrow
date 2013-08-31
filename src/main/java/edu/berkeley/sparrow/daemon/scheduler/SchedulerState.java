/*
 * Copyright 2013 The Regents of The University California
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.commons.configuration.Configuration;

/**
 * State storage for the Sparrow {@link Scheduler}. This is stored in its
 * own class since state storage may involve communicating to external services
 * such as ZooKeeper.
 */
public interface SchedulerState {

  /**
   * Initialize state storage. This should open connections to any external
   * services if required.
   * @throws IOException
   */
  public void initialize(Configuration conf) throws IOException;

  /**
   * Signal that state storage will be queried for information about
   * {@code appId} in the future.
   */
  public boolean watchApplication(String appId);

  /**
   * Get the backends available for a particular application. Each backend includes a
   * resource vector giving current available resources. TODO: this might be changed
   * to include more detailed information per-node.
   */
  public Set<InetSocketAddress> getBackends(String appId);
}
