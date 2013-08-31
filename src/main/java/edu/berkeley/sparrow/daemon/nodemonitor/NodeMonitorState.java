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

package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;


public interface NodeMonitorState {
  /**
   * Initialize state storage. This should open connections to any external
   * services if required.
   * @throws IOException 
   */
  public void initialize(Configuration conf) throws IOException;
  
  /**
   * Register a backend identified by {@code appId} which can be reached via
   * a NodeMonitor running at the given address. The node is assumed to have
   * resources given by {@code capacity}.
   */
  public boolean registerBackend(String appId, InetSocketAddress nodeMonitor);
}
