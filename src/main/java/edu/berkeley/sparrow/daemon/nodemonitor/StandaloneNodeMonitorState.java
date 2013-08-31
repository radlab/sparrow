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

import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;

import edu.berkeley.sparrow.daemon.StandaloneStateStore;

/**
 * A {@link NodeMonitorState} implementation for use in standalone mode.
 */
public class StandaloneNodeMonitorState implements NodeMonitorState {
  private StandaloneStateStore stateStore = StandaloneStateStore.getInstance();
 
  @Override
  public void initialize(Configuration conf) {
    // Nothing required
  }
  
  @Override
  public boolean registerBackend(String appId, InetSocketAddress nmAddr) {
    stateStore.registerBackend(appId, nmAddr);
    return true;
  }
}
