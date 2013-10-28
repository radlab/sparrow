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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.SchedulerService.AsyncClient.submitJob_call;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Frontend for the prototype implementation.
 */
public class ProtoFrontendAsync {
  public static final double DEFAULT_JOB_ARRIVAL_RATE_S = 10; // Jobs/second
  public static final int DEFAULT_TASKS_PER_JOB = 1;          // Tasks/job

  // Type of benchmark to run, see ProtoBackend static constant for benchmark types
  public static final int DEFAULT_TASK_BENCHMARK = ProtoBackend.BENCHMARK_TYPE_FP_CPU;
  public static final int DEFAULT_BENCHMARK_ITERATIONS = 10;  // # of benchmark iterations

  private static final Logger LOG = Logger.getLogger(ProtoFrontendAsync.class);

  private static class SubmitCallback implements  AsyncMethodCallback<submitJob_call> {
    TSchedulingRequest req;
    TNonblockingTransport tr;

    public SubmitCallback(TSchedulingRequest req, TNonblockingTransport tr) {
      this.req = req;
      this.tr = tr;
    }

    @Override
    public void onComplete(submitJob_call response) {
      LOG.debug("Submitted job: " + req);
      tr.close();
    }

    @Override
    public void onError(Exception exception) {
      LOG.error("Error submitting job");
      tr.close();
    }

  }

  public static List<TTaskSpec> generateJob(int numTasks, int benchmarkId,
      int benchmarkIterations) {
    // Pack task parameters
    ByteBuffer message = ByteBuffer.allocate(8);
    message.putInt(benchmarkId);
    message.putInt(benchmarkIterations);

    List<TTaskSpec> out = new ArrayList<TTaskSpec>();
    for (int taskId = 0; taskId < numTasks; taskId++) {
      TTaskSpec spec = new TTaskSpec();
      spec.setTaskId(Integer.toString(taskId));
      spec.setMessage(message.array());
      out.add(spec);
    }
    return out;
  }

  public static double generateInterarrivalDelay(Random r, double lambda) {
    double u = r.nextDouble();
    return -Math.log(u)/lambda;
  }

  public static void main(String[] args) {
    try {
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

      Configuration conf = new PropertiesConfiguration();

      if (options.has("c")) {
        String configFile = (String) options.valueOf("c");
        conf = new PropertiesConfiguration(configFile);
      }

      Random r = new Random();
      double lambda = conf.getDouble("job_arrival_rate_s", DEFAULT_JOB_ARRIVAL_RATE_S);
      int tasksPerJob = conf.getInt("tasks_per_job", DEFAULT_TASKS_PER_JOB);
      int benchmarkIterations = conf.getInt("benchmark.iterations",
          DEFAULT_BENCHMARK_ITERATIONS);
      int benchmarkId = conf.getInt("benchmark.id", DEFAULT_TASK_BENCHMARK);

      int schedulerPort = conf.getInt("scheduler_port",
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);

      TProtocolFactory factory = new TBinaryProtocol.Factory();
      TAsyncClientManager manager =  new TAsyncClientManager();

      long lastLaunch = System.currentTimeMillis();
      // Loop and generate tasks launches
      while (true) {
        // Lambda is the arrival rate in S, so we need to multiply the result here by
        // 1000 to convert to ms.
        long delay = (long) (generateInterarrivalDelay(r, lambda) * 1000);
        long curLaunch = lastLaunch + delay;
        long toWait = Math.max(0,  curLaunch - System.currentTimeMillis());
        lastLaunch = curLaunch;
        if (toWait == 0) {
          LOG.warn("Generated workload not keeping up with real time.");
        }
        List<TTaskSpec> tasks = generateJob(tasksPerJob, benchmarkId, benchmarkIterations);
        TUserGroupInfo user = new TUserGroupInfo();
        user.setUser("*");
        user.setGroup("*");
        TSchedulingRequest req = new TSchedulingRequest();
        req.setApp("testApp");
        req.setTasks(tasks);
        req.setUser(user);

        TNonblockingTransport tr = new TNonblockingSocket(
            "localhost", schedulerPort);
        SchedulerService.AsyncClient client = new SchedulerService.AsyncClient(
            factory, manager, tr);
        //client.registerFrontend("testApp", new RegisterCallback());
        client.submitJob(req, new SubmitCallback(req, tr));
      }
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }
}
