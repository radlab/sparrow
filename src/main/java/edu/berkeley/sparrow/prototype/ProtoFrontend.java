package edu.berkeley.sparrow.prototype;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import edu.berkeley.sparrow.thrift.FrontendService;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.Serialization;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TSchedulingPref;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Frontend for the prototype implementation.
 */
public class ProtoFrontend implements FrontendService.Iface {
  public static final double DEFAULT_JOB_ARRIVAL_RATE_S = 10; // Jobs/second
  public static final int DEFAULT_TASKS_PER_JOB = 1;          // Tasks/job

  // Type of benchmark to run, see ProtoBackend static constant for benchmark types
  public static final int DEFAULT_TASK_BENCHMARK = ProtoBackend.BENCHMARK_TYPE_FP_CPU;
  public static final int DEFAULT_BENCHMARK_ITERATIONS = 10;  // # of benchmark iterations

  private static final Logger LOG = Logger.getLogger(ProtoFrontend.class);
  public final static long startTime = System.currentTimeMillis();
  public static AtomicInteger tasksLaunched = new AtomicInteger(0);

  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private class JobLaunchRunnable implements Runnable {
    private List<TTaskSpec> request;
    private SparrowFrontendClient client;

    public JobLaunchRunnable(List<TTaskSpec> request, SparrowFrontendClient client) {
      this.request = request;
      this.client = client;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      TUserGroupInfo user = new TUserGroupInfo();
      user.setUser("*");
      user.setGroup("*");
      try {
        client.submitJob("testApp", request, user, new TSchedulingPref());
        LOG.debug("Submitted job: " + request);
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
      long end = System.currentTimeMillis();
      LOG.debug("Scheduling request duration " + (end - start));
    }
  }

  public List<TTaskSpec> generateJob(int numTasks, int benchmarkId,
      int benchmarkIterations) {
    TResourceVector resources = TResources.createResourceVector(300, 1);

    // Pack task parameters
    ByteBuffer message = ByteBuffer.allocate(8);
    message.putInt(benchmarkId);
    message.putInt(benchmarkIterations);

    List<TTaskSpec> out = new ArrayList<TTaskSpec>();
    for (int taskId = 0; taskId < numTasks; taskId++) {
      TTaskSpec spec = new TTaskSpec();
      spec.setTaskID(Integer.toString((new Random().nextInt())));
      spec.setMessage(message.array());
      spec.setEstimatedResources(resources);
      out.add(spec);
    }
    return out;
  }

  public double generateInterarrivalDelay(Random r, double lambda) {
    double u = r.nextDouble();
    return -Math.log(u)/lambda;
  }

  public void run(String[] args) {
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

      SparrowFrontendClient client = new SparrowFrontendClient();
      int schedulerPort = conf.getInt("scheduler_port",
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
      client.initialize(new InetSocketAddress("localhost", schedulerPort), "testApp", this);
      long lastLaunch = System.currentTimeMillis();

      /* This is a little tricky.
       *
       * We want to generate inter-arrival delays according to the arrival rate specified.
       * The simplest option would be to generate an arrival delay and then sleep() for it
       * before launching each task. This has in issue, however: sleep() might wait
       * several ms longer than we ask it to. When task arrival rates get really fast,
       * i.e. one task every 10 ms, sleeping an additional few ms will mean we launch
       * tasks at a much lower rate than requested.
       *
       * Instead, we keep track of task launches in a way that does not depend on how long
       * sleep() actually takes. We still might have tasks launch slightly after their
       * scheduled launch time, but we will not systematically "fall behind" due to
       * compounding time lost during sleep()'s;
       */
      while (true) {
        // Lambda is the arrival rate in S, so we need to multiply the result here by
        // 1000 to convert to ms.
        long delay = (long) (generateInterarrivalDelay(r, lambda) * 1000);
        long curLaunch = lastLaunch + delay;
        long toWait = Math.max(0,  curLaunch - System.currentTimeMillis());
        lastLaunch = curLaunch;
        if (toWait == 0) {
          LOG.warn("Lanching task after start time in generated workload.");
        }
        Thread.sleep(toWait);
        Runnable runnable =  new JobLaunchRunnable(
            generateJob(tasksPerJob, benchmarkId, benchmarkIterations), client);
        new Thread(runnable).start();
        int launched = tasksLaunched.addAndGet(1);
        double launchRate = (double) launched * 1000.0 /
            (System.currentTimeMillis() - startTime);
        LOG.debug("Aggregate launch rate: " + launchRate);
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
    new ProtoFrontend().run(args);
  }
}
