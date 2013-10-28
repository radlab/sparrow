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

package edu.berkeley.sparrow.examples;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Frontend that submits large jobs, for the purpose of testing throughput.
 */
public class ThroughputTestingFrontend implements FrontendService.Iface {

  // Type of benchmark to run, see ProtoBackend static constant for benchmark types
  public static final int DEFAULT_TASK_BENCHMARK = ProtoBackend.BENCHMARK_TYPE_FP_CPU;

  /**
   * Default application name.
   */
  public static final String APPLICATION_ID = "testApp";

  private static final Logger LOG = Logger.getLogger(ThroughputTestingFrontend.class);
  public final static long startTime = System.currentTimeMillis();
  public static AtomicInteger tasksLaunched = new AtomicInteger(0);

  public void run(String[] args) {
    try {
      OptionParser parser = new OptionParser();
      parser.accepts("c", "configuration file").withRequiredArg().ofType(String.class);
      parser.accepts("help", "print help statement");
      OptionSet options = parser.parse(args);

      if (options.has("help")) {
        parser.printHelpOn(System.out);
        System.exit(-1);
      }

      // Logger configuration: log to the console
      BasicConfigurator.configure();
      LOG.setLevel(Level.DEBUG);

      SparrowFrontendClient client = new SparrowFrontendClient();
      int schedulerPort = SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT;
      client.initialize(new InetSocketAddress("localhost", schedulerPort), APPLICATION_ID, this);

      // Generate list of tasks.
      ByteBuffer message = ByteBuffer.allocate(8);
      message.putInt(DEFAULT_TASK_BENCHMARK);
      // Just one iteration!
      message.putInt(1);
      
      List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
      int numTasks = Integer.parseInt(args[0]);
      LOG.info("Launching " + numTasks + " tasks");
      for (int taskId = 0; taskId < numTasks; taskId++) {
        TTaskSpec spec = new TTaskSpec();
        spec.setTaskId(Integer.toString(taskId));
        spec.setMessage(message.array());
        tasks.add(spec);
      }
      TUserGroupInfo userInfo = new TUserGroupInfo("User", "*", 0);
      
      while(true) {
    	  client.submitJob(APPLICATION_ID, tasks, userInfo);
    	  tasksLaunched.addAndGet(tasks.size());
        LOG.info(tasksLaunched.get() + " tasks launched");
      
        Thread.sleep(1000);
      }
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }

  @Override
  public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
      throws TException {
    // We don't use messages here, so just log it.
    LOG.debug("Got unexpected message: " + Serialization.getByteBufferContents(message));
  }

  public static void main(String[] args) {
    new ThroughputTestingFrontend().run(args);
  }
}
