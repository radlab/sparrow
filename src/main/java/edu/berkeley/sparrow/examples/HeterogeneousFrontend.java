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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TPlacementPreference;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Frontend for the prototype implementation.
 */
public class HeterogeneousFrontend implements FrontendService.Iface {
  /** Jobs/second during warmup period. */
  public static final double DEFAULT_WARMUP_JOB_ARRIVAL_RATE_S = 10;

  /** Duration of warmup period. */
  public static final int DEFAULT_WARMUP_S = 10;

  /** Amount of time to wait for queues to drain once warmup period is over. */
  public static final int DEFAULT_POST_WARMUP_S = 60;

  /** Amount of time to launch tasks for (not including the warmup period). */
  public static final int DEFAULT_EXPERIMENT_S = 300;

  public static final double DEFAULT_JOB_ARRIVAL_RATE_S = 10; // Jobs/second
  public static final int DEFAULT_TASKS_PER_JOB = 1;          // Tasks/job

  // Type of benchmark to run, see ProtoBackend static constant for benchmark types
  public static final int DEFAULT_TASK_BENCHMARK = ProtoBackend.BENCHMARK_TYPE_FP_CPU;
  public static final int DEFAULT_BENCHMARK_ITERATIONS = 1000;  // # of benchmark iterations

  /**
   * The default number of preferred nodes for each task. 0 signals that tasks are
   * unconstrained.
   */
  public static final int DEFAULT_NUM_PREFERRED_NODES = 0;

  /**
   * Configuration parameter name for the set of backends (used to set preferred nodes for
   * tasks).
   */
  public static final String BACKENDS = "backends";

  /**
   * Configuration parameter name for the set of users. Users should be specified by three
   * values: a user id, an integral priority, and an integral demand, where the demand of each user
   * is specified relative to the demands of other users. These three values should be semi-colon
   * separated.
   */
  public static final String USERS = "users";

  /**
   * Default application name.
   */
  public static final String APPLICATION_ID = "testApp";

  private static final Logger LOG = Logger.getLogger(HeterogeneousFrontend.class);
  public final static long startTime = System.currentTimeMillis();
  public static AtomicInteger tasksLaunched = new AtomicInteger(0);

  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private class JobLaunchRunnable implements Runnable {
    private List<TTaskSpec> request;
    private SparrowFrontendClient client;
    UserInfo user;

    public JobLaunchRunnable(List<TTaskSpec> request, UserInfo user, SparrowFrontendClient client) {
      this.request = request;
      this.client = client;
      this.user = user;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      TUserGroupInfo userInfo = new TUserGroupInfo(user.user, "*", 0);
      try {
        client.submitJob(APPLICATION_ID, request, userInfo);
        LOG.debug("Submitted job: " + request + " for user " + userInfo);
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
      long end = System.currentTimeMillis();
      LOG.debug("Scheduling request duration " + (end - start));
    }
  }

  public static class UserInfo {
    public String user;
    public int cumulativeWeight;
    public int taskDuration;
    /** Used for debugging purposes, to ensure user weights are being used correctly. */
    public int totalTasksLaunched;

    /** Total weight assigned across all users. */
    public static int totalWeight = 0;

    public UserInfo(String user, int weight, int taskDuration) {
      this.user = user;
      this.cumulativeWeight = totalWeight + weight;
      totalWeight = this.cumulativeWeight;
      this.taskDuration = taskDuration;
      this.totalTasksLaunched = 0;
    }
  }

  public List<TTaskSpec> generateJob(int numTasks, int numPreferredNodes, List<String> backends,
                                     int benchmarkId, int benchmarkIterations) {
    // Pack task parameters
    ByteBuffer message = ByteBuffer.allocate(8);
    message.putInt(benchmarkId);
    message.putInt(benchmarkIterations);

    List<TTaskSpec> out = new ArrayList<TTaskSpec>();
    for (int taskId = 0; taskId < numTasks; taskId++) {
      TTaskSpec spec = new TTaskSpec();
      spec.setTaskId(Integer.toString((new Random().nextInt())));
      spec.setMessage(message.array());
      if (numPreferredNodes > 0) {
        Collections.shuffle(backends);
        TPlacementPreference preference = new TPlacementPreference();
        for (int i = 0; i < numPreferredNodes; i++) {
          preference.addToNodes(backends.get(i));
        }
        spec.setPreference(preference);
      }
      out.add(spec);
    }
    return out;
  }

  /**
   * Generates exponentially distributed interarrival delays.
   */
  public double generateInterarrivalDelay(Random r, double lambda) {
    double u = r.nextDouble();
    return -Math.log(u)/lambda;
  }

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

      Configuration conf = new PropertiesConfiguration();

      if (options.has("c")) {
        String configFile = (String) options.valueOf("c");
        conf = new PropertiesConfiguration(configFile);
      }

      double warmupLambda = conf.getDouble("warmup_job_arrival_rate_s",
                                            DEFAULT_WARMUP_JOB_ARRIVAL_RATE_S);
      int warmupDurationS = conf.getInt("warmup_s", DEFAULT_WARMUP_S);
      int postWarmupS = conf.getInt("post_warmup_s", DEFAULT_POST_WARMUP_S);

      double lambda = conf.getDouble("job_arrival_rate_s", DEFAULT_JOB_ARRIVAL_RATE_S);
      int experimentDurationS = conf.getInt("experiment_s", DEFAULT_EXPERIMENT_S);
      LOG.debug("Using arrival rate of  " + lambda +
          " tasks per second and running experiment for " + experimentDurationS + " seconds.");
      int tasksPerJob = conf.getInt("tasks_per_job", DEFAULT_TASKS_PER_JOB);
      int numPreferredNodes = conf.getInt("num_preferred_nodes", DEFAULT_NUM_PREFERRED_NODES);
      LOG.debug("Using " + numPreferredNodes + " preferred nodes for each task.");
      int benchmarkIterations = conf.getInt("benchmark.iterations",
          DEFAULT_BENCHMARK_ITERATIONS);
      int benchmarkId = conf.getInt("benchmark.id", DEFAULT_TASK_BENCHMARK);

      List<String> backends = new ArrayList<String>();
      if (numPreferredNodes > 0) {
        /* Attempt to parse the list of slaves, which we'll need to (randomly) select preferred
         * nodes. */
        if (!conf.containsKey(BACKENDS)) {
          LOG.fatal("Missing configuration backend list, which is needed to randomly select " +
                    "preferred nodes (num_preferred_nodes set to " + numPreferredNodes + ")");
        }
        for (String node : conf.getStringArray(BACKENDS)) {
          backends.add(node);
        }
        if (backends.size() < numPreferredNodes) {
          LOG.fatal("Number of backends smaller than number of preferred nodes!");
        }
      }

      List<UserInfo> users = new ArrayList<UserInfo>();
      if (conf.containsKey(USERS)) {
        for (String userSpecification : conf.getStringArray(USERS)) {
          LOG.debug("Reading user specification: " + userSpecification);
          String[] parts = userSpecification.split(":");
          if (parts.length != 3) {
            LOG.error("Unexpected user specification string: " + userSpecification +
                "; ignoring user");
            continue;
          }
          users.add(new UserInfo(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2])));
        }
      }
      if (users.size() == 0) {
        // Add a dummy user.
        users.add(new UserInfo("defaultUser", 1, 0));
      }

      SparrowFrontendClient client = new SparrowFrontendClient();
      int schedulerPort = conf.getInt("scheduler_port",
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
      client.initialize(new InetSocketAddress("localhost", schedulerPort), APPLICATION_ID, this);

      if (warmupDurationS > 0) {
        LOG.debug("Warming up for " + warmupDurationS + " seconds at arrival rate of " +
                  warmupLambda + " jobs per second");
        launchTasks(users, warmupLambda, warmupDurationS, tasksPerJob, numPreferredNodes,
            benchmarkIterations, benchmarkId, backends, client);
        LOG.debug("Waiting for queues to drain after warmup (waiting " + postWarmupS +
                 " seconds)");
        Thread.sleep(postWarmupS * 1000);
      }
      LOG.debug("Launching experiment for " + experimentDurationS + " seconds");
      launchTasks(users, lambda, experimentDurationS, tasksPerJob, numPreferredNodes,
          benchmarkIterations, benchmarkId, backends, client);
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }

  private void launchTasks(List<UserInfo> users, double lambda, int launch_duration_s,
      int tasksPerJob, int numPreferredNodes, int benchmarkIterations, int benchmarkId,
      List<String> backends, SparrowFrontendClient client)
      throws InterruptedException {
    /* This is a little tricky.
     *
     * We want to generate inter-arrival delays according to the arrival rate specified.
     * The simplest option would be to generate an arrival delay and then sleep() for it
     * before launching each task. However, this is problematic because sleep() might wait
     * several ms longer than we ask it to. When task arrival rates get really fast,
     * i.e. one task every 10 ms, sleeping an additional few ms will mean we launch
     * tasks at a much lower rate than requested.
     *
     * Instead, we keep track of task launches in a way that does not depend on how long
     * sleep() actually takes. We still might have tasks launch slightly after their
     * scheduled launch time, but we will not systematically "fall behind" due to
     * compounding time lost during sleep()'s;
     */
    //Random r = new Random();
    double mostRecentLaunch = System.currentTimeMillis();
    long end = System.currentTimeMillis() + launch_duration_s * 1000;
    Random r = new Random();
    // Used to determine which user's task to run next. Start from a random place so
    // all of the frontends don't end up synchronized.
    int userIndex = r.nextInt(UserInfo.totalWeight);
    LOG.debug("Starting with user index " + userIndex);
    while (System.currentTimeMillis() < end) {
      // Lambda is the arrival rate in S, so we need to multiply the result here by
      // 1000 to convert to ms.
      double delay = 1000 / lambda;
      double curLaunch = mostRecentLaunch + delay;
      long toWait = Math.max(0,  (long) curLaunch - System.currentTimeMillis());
      mostRecentLaunch = curLaunch;
      if (toWait == 0) {
        LOG.warn("Lanching task after start time in generated workload.");
      }
      Thread.sleep(toWait);

      // Deterministically select which user's task to run, according to weight.
      UserInfo user = null;
      for (UserInfo potentialUser : users) {
        if ((userIndex % UserInfo.totalWeight) < potentialUser.cumulativeWeight) {
          user = potentialUser;
          break;
        }
      }
      assert(user != null);
      ++userIndex;
      ++user.totalTasksLaunched;
      LOG.debug("Launching task for user " + user.user + " with duration " + user.taskDuration +
          " (" + user.totalTasksLaunched + " total tasks launched for this user)");

      Runnable runnable =  new JobLaunchRunnable(
          generateJob(tasksPerJob, numPreferredNodes, backends, benchmarkId,
                      user.taskDuration),
          user, client);
      new Thread(runnable).start();
      int launched = tasksLaunched.addAndGet(1);
      double launchRate = (double) launched * 1000.0 /
          (System.currentTimeMillis() - startTime);
      LOG.debug("Aggregate launch rate: " + launchRate);
    }
  }

  @Override
  public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
      throws TException {
    // We don't use messages here, so just log it.
    LOG.debug("Got unexpected message: " + Serialization.getByteBufferContents(message));
  }

  public static void main(String[] args) {
    new HeterogeneousFrontend().run(args);
  }
}
