package edu.berkeley.sparrow.prototype;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.NodeMonitorService.Client;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * A prototype Sparrow backend.
 *
 * This backend is capable of performing a number of benchmark tasks, each representing
 * distinct resource consumption profiles. It initiates a thrift server with a bounded
 * size thread pool (of at most {@code WORKER_THREADS} threads). To makes sure that
 * we never queue tasks, we additionally spawn a new thread each time a task is launched.
 * In the future, we will have launchTask() directly execute the task and rely on queuing
 * in the underlying thread pool to queue if task launches exceed capacity.
 */
public class ProtoBackend implements BackendService.Iface {
  /** Benchmark which, on each iteration, runs 1 million random floating point
   *  multiplications.*/
  public static int BENCHMARK_TYPE_FP_CPU = 1;
  /** Benchmark which allocates a heap buffer of 200 million bytes, then on each iteration
   *  accesses 1 million contiguous bytes of the buffer, starting at a random offset.*/
  public static int BENCHMARK_TYPE_RANDOM_MEMACCESS = 2;
  // NOTE: we do not use an enum for the above because it is not possible to serialize
  // an enum with our current simple serialization technique.

  /** Tracks the total number of tasks launched since execution began. Updated on
   * each task launch. This is helpful for diagnosing unwanted queuing in various parts
   * of the system (i.e. if we notice the backend is launching fewer tasks than we expect
   * based on the frontend task launch rate). */
  public static AtomicInteger numTasks = new AtomicInteger(0);
  public static long startTime = -1;

  private static final int DEFAULT_LISTEN_PORT = 20101;

  /**
   * This is just how many threads can concurrently be answering function calls
   * from the NM. Each task is launched in its own from one of these threads. If tasks
   * launches arrive fast enough that all worker threads are concurrently executing
   * a task, this will queue. We currently launch new threads for each task to prevent
   * this from happening.
   */
  private static final int THRIFT_WORKER_THREADS = 4;
  private static final int TASK_WORKER_THREADS = 4;
  private static final String APP_ID = "testApp";

  /** We assume we are speaking to local Node Manager. */
  private static final String NM_HOST = "localhost";
  private static int NM_PORT;

  private static Client client;

  private static final Logger LOG = Logger.getLogger(ProtoBackend.class);
  private static final ExecutorService executor =
      Executors.newFixedThreadPool(TASK_WORKER_THREADS);

  /**
   * Thread spawned for each task. It runs for a given amount of time (and adds
   * its resources to the total resources for that time) then stops. It updates
   * the NodeMonitor when it launches and again when it finishes.
   */
  private class TaskRunnable implements Runnable {
    private int benchmarkId;
    private int benchmarkIterations;
    private TResourceVector taskResources;
    private TFullTaskId taskId;

    public TaskRunnable(String requestId, TFullTaskId taskId, ByteBuffer message,
        TResourceVector taskResources) {
      this.benchmarkId = message.getInt();
      this.benchmarkIterations = message.getInt();
      this.taskResources = taskResources;
      this.taskId = taskId;
    }

    @Override
    public void run() {
      if (startTime == -1) {
        startTime = System.currentTimeMillis();
      }

      long taskStart = System.currentTimeMillis();
      NodeMonitorService.Client client = null;
      try {
        client = TClients.createBlockingNmClient(NM_HOST, NM_PORT);
      } catch (IOException e) {
        LOG.fatal("Error creating NM client", e);
      }


      int tasks = numTasks.addAndGet(1);
      double taskRate = ((double) tasks) * 1000 /
          (System.currentTimeMillis() - startTime);
      LOG.debug("Aggregate task rate: " + taskRate);

      Random r = new Random();

      long benchmarkStart = System.currentTimeMillis();
      runBenchmark(benchmarkId, benchmarkIterations, r);
      LOG.debug("Benchmark runtime: " + (System.currentTimeMillis() - benchmarkStart));

      // Update bookkeeping for task finish
      synchronized(resourceUsage) {
        TResources.subtractFrom(resourceUsage, taskResources);
      }

      HashMap<TUserGroupInfo, TResourceVector> out =
          new HashMap<TUserGroupInfo, TResourceVector>();
      // Inform NM of resource usage
      out.put(user, resourceUsage);
      try {
        client.tasksFinished(Lists.newArrayList(taskId));
      } catch (TException e) {
        e.printStackTrace();
      }
      client.getInputProtocol().getTransport().close();
      client.getOutputProtocol().getTransport().close();
      LOG.debug("Task running for " + (System.currentTimeMillis() - taskStart) + " ms");
    }
  }

  /**
   * Run the benchmark identified by {@code benchmarkId} for {@code iterations}
   * iterations using random generator {@code r}. Return true if benchmark is recognized
   * and false otherwise.
   */
  public static boolean runBenchmark(int benchmarkId, int iterations, Random r) {
    if (benchmarkId == BENCHMARK_TYPE_RANDOM_MEMACCESS) {
      LOG.debug("Running random access benchmark for " + iterations + " iterations.");
      runRandomMemAcessBenchmark(iterations, r);
    } else if (benchmarkId == BENCHMARK_TYPE_FP_CPU) {
      LOG.debug("Running CPU benchmark for " + iterations + " iterations.");
     runFloatingPointBenchmark(iterations, r);
    } else {
      LOG.error("Received unrecognized benchmark type");
      return false;
    }
    return true;
  }

  /**
   * Benchmark that runs random floating point multiplications for the specified amount of
   * "iterations", where each iteration is one millisecond.
   */
  public static void runFloatingPointBenchmark(int iterations, Random r) {
    int runtimeMillis = iterations;
    long startTime = System.nanoTime();
    int opsPerIteration = 1000;
    /* We keep a running result here and print it out so that the JVM doesn't
     * optimize all this computation away. */
    float result = r.nextFloat();
    while (System.nanoTime() - startTime < runtimeMillis * 1000 * 1000) {
      for (int j = 0; j < opsPerIteration; j++) {
        // On each iteration, perform a floating point multiplication
        float x = r.nextFloat();
        float y = r.nextFloat();
        result += (x * y);
      }
    }
    LOG.debug("Benchmark result " + result);
  }

  /** Benchmark which allocates a heap buffer of 200 million bytes, then on each iteration
   *  accesses 1 million contiguous bytes of the buffer, starting at a random offset.*/
  public static void runRandomMemAcessBenchmark(int iterations, Random r) {
    // 2 hundred million byte buffer
    int buffSize = 1000 * 1000 * 200;
    byte[] buff = new byte[buffSize];
    // scan 1 million bytes at a time
    int runLength = 1000 * 1000;
    // We keep a running result here and print it out so that the JVM doesn't
    // optimize all this computation away.
    byte result = 1;
    for (int i = 0; i < iterations; i++) {
      // On each iteration, start at a random index, and scan runLength contiguous
      // bytes, potentially wrapping if we hit the end of the buffer.
      int start = r.nextInt(buff.length);
      for (int j = 0; j < runLength; j++) {
        result = (byte) (result ^ buff[(start + j) % (buff.length - 1)]);
      }
    }
    LOG.debug("Benchmark result " + result);
  }

  private TUserGroupInfo user; // We force all tasks to be run by same user
  private TResourceVector resourceUsage = TResources.createResourceVector(0, 0);

  public ProtoBackend() {
    LOG.debug("Created");
    this.user = new TUserGroupInfo();
    user.setUser("*");
    user.setGroup("*");
  }

  @Override
  public void launchTask(ByteBuffer message, TFullTaskId taskId,
      TUserGroupInfo user, TResourceVector estimatedResources) throws TException {
    LOG.info("Submitting task " + taskId.getTaskId() + "at " + System.currentTimeMillis());
    // We want to add accounting for task start here, even though the task is actually
    // queued. Note that this won't be propagated to the node monitor until another task
    // finishes.
    synchronized(resourceUsage) {
      TResources.addTo(resourceUsage, estimatedResources);
    }

    // Note we ignore user here
    executor.submit(new TaskRunnable(
        taskId.requestId, taskId, message, estimatedResources));
    synchronized (client) {
      client.sendFrontendMessage(APP_ID, taskId, 1, ByteBuffer.wrap("Started".getBytes()));
    }
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
    BackendService.Processor<BackendService.Iface> processor =
        new BackendService.Processor<BackendService.Iface>(new ProtoBackend());

    int listenPort = conf.getInt("listen_port", DEFAULT_LISTEN_PORT);
    NM_PORT = conf.getInt("node_monitor_port", NodeMonitorThrift.DEFAULT_NM_THRIFT_PORT);
    TServers.launchThreadedThriftServer(listenPort, THRIFT_WORKER_THREADS, processor);

    // Register server
    client = TClients.createBlockingNmClient(NM_HOST, NM_PORT);

    try {
      client.registerBackend(APP_ID, "localhost:" + listenPort);
    } catch (TTransportException e) {
      LOG.debug("Error while registering backend: " + e.getMessage());
    }
  }
}
