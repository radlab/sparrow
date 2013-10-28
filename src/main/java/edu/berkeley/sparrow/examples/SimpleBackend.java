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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService.Client;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * A prototype Sparrow backend that runs sleep tasks.
 */
public class SimpleBackend implements BackendService.Iface {

  private static final String LISTEN_PORT = "listen_port";
  private static final int DEFAULT_LISTEN_PORT = 20101;

  /**
   * Each task is launched in its own thread from a thread pool with WORKER_THREADS threads,
   * so this should be set equal to the maximum number of tasks that can be running on a worker.
   */
  private static final int WORKER_THREADS = 16;
  private static final String APP_ID = "sleepApp";

  /** Configuration parameters to specify where the node monitor is running. */
  private static final String NODE_MONITOR_HOST = "node_monitor_host";
  private static final String DEFAULT_NODE_MONITOR_HOST = "localhost";
  private static String NODE_MONITOR_PORT = "node_monitor_port";

  private static Client client;

  private static final Logger LOG = Logger.getLogger(SimpleBackend.class);
  private static final ExecutorService executor =
      Executors.newFixedThreadPool(WORKER_THREADS);

  /**
   * Keeps track of finished tasks.
   *
   * A single thread pulls items off of this queue and uses
   * the client to notify the node monitor that tasks have finished.
   */
  private final BlockingQueue<TFullTaskId> finishedTasks = new LinkedBlockingQueue<TFullTaskId>();

  /**
   * Thread that sends taskFinished() RPCs to the node monitor.
   *
   * We do this in a single thread so that we just need a single client to the node monitor
   * and don't need to create a new client for each task.
   */
  private class TasksFinishedRpcRunnable implements Runnable {
	  @Override
	  public void run() {
		  while (true) {
		  	try {
		  		TFullTaskId task = finishedTasks.take();
					client.tasksFinished(Lists.newArrayList(task));
				} catch (InterruptedException e) {
					LOG.error("Error taking a task from the queue: " + e.getMessage());
				} catch (TException e) {
					LOG.error("Error with tasksFinished() RPC:" + e.getMessage());
				}
		  }
	  }
  }

  /**
   * Thread spawned for each task. It runs for a given amount of time (and adds
   * its resources to the total resources for that time) then stops. It updates
   * the NodeMonitor when it launches and again when it finishes.
   */
  private class TaskRunnable implements Runnable {
    private int taskDurationMillis;
    private TFullTaskId taskId;

    public TaskRunnable(String requestId, TFullTaskId taskId, ByteBuffer message) {
      this.taskDurationMillis = message.getInt();
      this.taskId = taskId;
    }

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      try {
        Thread.sleep(taskDurationMillis);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while sleeping: " + e.getMessage());
      }
      LOG.debug("Task completed in " + (System.currentTimeMillis() - startTime) + "ms");
      finishedTasks.add(taskId);
    }
  }

  /**
   * Initializes the backend by registering with the node monitor.
   *
   * Also starts a thread that handles finished tasks (by sending an RPC to the node monitor).
   */
  public void initialize(int listenPort, String nodeMonitorHost, int nodeMonitorPort) {
    // Register server.
    try {
			client = TClients.createBlockingNmClient(nodeMonitorHost, nodeMonitorPort);
		} catch (IOException e) {
			LOG.debug("Error creating Thrift client: " + e.getMessage());
		}

    try {
      client.registerBackend(APP_ID, "localhost:" + listenPort);
      LOG.debug("Client successfully registered");
    } catch (TException e) {
      LOG.debug("Error while registering backend: " + e.getMessage());
    }

    new Thread(new TasksFinishedRpcRunnable()).start();
  }

  @Override
  public void launchTask(ByteBuffer message, TFullTaskId taskId,
      TUserGroupInfo user) throws TException {
    LOG.info("Submitting task " + taskId.getTaskId() + " at " + System.currentTimeMillis());

    executor.submit(new TaskRunnable(
        taskId.requestId, taskId, message));
  }

  public static void main(String[] args) throws IOException, TException {
    OptionParser parser = new OptionParser();
    parser.accepts("c", "configuration file").
      withRequiredArg().ofType(String.class);
    parser.accepts("help", "print help statement");
    OptionSet options = parser.parse(args);

    if (options.has("help")) {
      parser.printHelpOn(System.out);
      System.exit(-1);
    }

    // Logger configuration: log to the console
    BasicConfigurator.configure();
    LOG.setLevel(Level.DEBUG);
    LOG.debug("debug logging on");

    Configuration conf = new PropertiesConfiguration();

    if (options.has("c")) {
      String configFile = (String) options.valueOf("c");
      try {
        conf = new PropertiesConfiguration(configFile);
      } catch (ConfigurationException e) {}
    }
    // Start backend server
    SimpleBackend protoBackend = new SimpleBackend();
    BackendService.Processor<BackendService.Iface> processor =
        new BackendService.Processor<BackendService.Iface>(protoBackend);

    int listenPort = conf.getInt(LISTEN_PORT, DEFAULT_LISTEN_PORT);
    int nodeMonitorPort = conf.getInt(NODE_MONITOR_PORT, NodeMonitorThrift.DEFAULT_NM_THRIFT_PORT);
    String nodeMonitorHost = conf.getString(NODE_MONITOR_HOST, DEFAULT_NODE_MONITOR_HOST);
    TServers.launchSingleThreadThriftServer(listenPort, processor);
    protoBackend.initialize(listenPort, nodeMonitorHost, nodeMonitorPort);
  }
}
