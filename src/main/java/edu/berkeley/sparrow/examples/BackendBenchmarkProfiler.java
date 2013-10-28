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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;


/**
 * A standalone script for profiling benchmark capacity on a given machine.
 * 
 * This is useful for determining how much capacity a machine has for our benchmark. i.e.
 * what is the maximum rate of tasks-per-second the machine can sustain. We determine
 * this experimentally since it depends on several factors - in particular the speed of 
 * the CPU and the OS scheduling policy. See {@code main()} and {code runExperiment()} 
 * for more information on what is actually run here.
 * 
 * See {@link runExperiment()} for details.
 */
public class BackendBenchmarkProfiler {
  public static final int DEFAULT_BENCHMARK_ITERATIONS = 4;
  public static final int DEFAULT_BENCHMARK_ID = 1;
  public static final int DEFAULT_BUCKET_SIZE_S = 2;
  public static final int DEFAULT_TRAIL_LENGTH_S = 60;
  public static final double DEFAULT_START_RATE = 10;
  public static final double DEFAULT_END_RATE = 30;
  public static final double DEFAULT_RATE_STEP = 2;
  
  public static int benchmarkIterations = DEFAULT_BENCHMARK_ITERATIONS;
  public static int benchmarkId = DEFAULT_BENCHMARK_ID;
  public static int bucketSizeS = DEFAULT_BUCKET_SIZE_S;
  public static int trialLengthS = DEFAULT_TRAIL_LENGTH_S;
  public static double startRate = DEFAULT_START_RATE;
  public static double endRate = DEFAULT_END_RATE;
  public static double rateStep = DEFAULT_RATE_STEP;
  
  private static class BenchmarkRunnable implements Runnable {
    private HashMap<Long, List<Long>> runTimes;
    private HashMap<Long, List<Long>> waitTimes;
    private long timeCreated;
    private long bucketGranularity;

    /**
     * A runnable which runs a floating point benchmark and updates the global list
     * of runtimes with information about how long it took. Run time is assessed
     * as the difference between when the runnable is instantiated and when it finishes
     * the benchmark loop and exists. This might include queueing in the executor.  
     */
    private BenchmarkRunnable(HashMap<Long, List<Long>> runTimes, 
        HashMap<Long, List<Long>> waitTimes, long granularity) {
      this.runTimes = runTimes;
      this.waitTimes = waitTimes;
      timeCreated = System.currentTimeMillis();
      bucketGranularity = granularity;
    }
    
    @Override
    public void run() {
      long start = System.currentTimeMillis();
      Random r = new Random();
      ProtoBackend.runBenchmark(benchmarkId, benchmarkIterations, r);
      long end = System.currentTimeMillis();
      long timeIndex = timeCreated - (timeCreated % bucketGranularity);
      synchronized(runTimes) {
        if (!runTimes.containsKey((timeIndex))) {
          runTimes.put(timeIndex, new LinkedList<Long>());
        }
        runTimes.get(timeIndex).add(end - timeCreated);
      }      
      synchronized(waitTimes) {
        if (!waitTimes.containsKey((timeIndex))) {
          waitTimes.put(timeIndex, new LinkedList<Long>());
        }
        waitTimes.get(timeIndex).add(start - timeCreated);
      }      
    }
  }
  
  /**
   * This generates an arrival delay according to an exponential distribution with
   * average 1\{@code lambda}. Generating arrival delays from such a distribution creates
   * a Poission process with an average arrival rate of {@code lambda} events per second. 
   */
  public static double generateInterarrivalDelay(Random r, double lambda) {
    double u = r.nextDouble();
    return -Math.log(u)/lambda;
  }
  
  /**
   * Run an experiment which launches tasks at {@code arrivalRate} for {@code durationMs}
   * seconds and waits for all tasks to finish. Return a {@link DescriptiveStatistics}
   * object which contains stats about the distribution of task finish times. Tasks
   * are executed in a thread pool which contains at least {@code corePoolSize} threads
   * and grows up to {@code maxPoolSize} threads (growing whenever a new task arrives
   * and all existing threads are used). 
   * 
   * Setting {@code maxPoolSize} to a very large number enacts time sharing, while
   * setting it equal to {@code corePoolSize} creates a fixed size task pool.
   * 
   * The derivative of task finishes is tracked by bucketing tasks at the granularity
   * {@code bucketSize}. If it is detected that task finishes are increasing in an 
   * unbounded fashion (i.e. infinite queuing is occuring) a {@link RuntimeException} 
   * is thrown.
   */
  public static void runExperiment(double arrivalRate, int corePoolSize,
      int maxPoolSize, long bucketSize, long durationMs, DescriptiveStatistics runTimes,
      DescriptiveStatistics waitTimes) {
    long startTime = System.currentTimeMillis();
    long keepAliveTime = 10;
    Random r = new Random();
    BlockingQueue<Runnable> runQueue = new LinkedBlockingQueue<Runnable>();
    ExecutorService threadPool = new ThreadPoolExecutor(
        corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS, runQueue);
    if (maxPoolSize == Integer.MAX_VALUE) {
      threadPool = Executors.newCachedThreadPool();
    }
    
    // run times indexed by bucketing interval
    HashMap<Long, List<Long>> bucketedRunTimes = 
        new HashMap<Long, List<Long>>();
    // wait times indexed by bucketing interval
    HashMap<Long, List<Long>> bucketedWaitTimes = 
        new HashMap<Long, List<Long>>();
    
    /*
     * This is a little tricky. 
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
    long currTime = startTime;
    while (true) {
      long delay = (long) (generateInterarrivalDelay(r, arrivalRate) * 1000);
      
      // When should the next task launch, based on when the last task was scheduled
      // to launch.
      long nextTime = currTime + delay;
      
      // Diff gives how long we should wait for the next scheduled task. The difference 
      // may be negative if our last sleep() lasted too long relative to the inter-arrival
      // delay based on the last scheduled launch, so we round up to 0 in that case. 
      long diff = Math.max(0, nextTime - System.currentTimeMillis());
      currTime = nextTime;
      if (diff > 0) {
        try {
          Thread.sleep(diff);
        } catch (InterruptedException e) {
          System.err.println("Unexpected interruption!");
          System.exit(1);
        }
      }
      threadPool.submit((new BenchmarkRunnable(bucketedRunTimes, bucketedWaitTimes, bucketSize)));
      if (System.currentTimeMillis() > startTime + durationMs) {
        break;
      }
    }
    threadPool.shutdown();
    try {
      threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e1) {
      System.err.println("Unexpected interruption!");
      System.exit(1);
    }
    List<Long> times = new ArrayList<Long>(bucketedRunTimes.keySet());
    Collections.sort(times);
    HashMap<Long, DescriptiveStatistics> bucketStats = new HashMap<Long, 
        DescriptiveStatistics>();
    
    // Remove first and last buckets since they will not be completely full to do
    // discretization. 
    times.remove(0);
    times.remove(times.size() - 1);
    
    for(Long time : times) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      List<Long> list = bucketedRunTimes.get(time);
      for (Long l : list) {
        stats.addValue(l);
        runTimes.addValue(l);
      }
      bucketStats.put(time, stats);
      
      List<Long> waitList = bucketedWaitTimes.get(time);
      for (Long l : waitList) {
        waitTimes.addValue(l);
      }
    }
    int size = bucketStats.size();
    if (size >= 2) {
      DescriptiveStatistics first = bucketStats.get(times.get(0));
      DescriptiveStatistics last = bucketStats.get(times.get(times.size() - 1));
      double increase = last.getPercentile(50) / first.getPercentile(50);
      // A simple heuristic, if the median runtime went up by five from the first to 
      // last complete bucket, we assume we are seeing unbounded growth
      if (increase > 5.0) {
        throw new RuntimeException("Queue not in steady state: " + last.getMean() 
            + " vs " + first.getMean());
      }
    }
  }
  
  /**
   * Run the benchmark at increasingly high arrival rates until infinite queuing is
   * detected. This performs two trials, one where tasks are launched in a fixed-sized
   * pool and the other where tasks are not queued and always launched in parallel.
   * The statistics for runtime vs. arrival rate are printed out for each of the two
   * trials. 
   */
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    OptionParser parser = new OptionParser();
    parser.accepts("t", "size of core thread pool").
      withRequiredArg().ofType(Integer.class);
    parser.accepts("b", "benchmark ID").
    withRequiredArg().ofType(Integer.class);
    parser.accepts("i", "number of benchmark iterations to run").
      withRequiredArg().ofType(Integer.class);
    parser.accepts("s", "bucket size in seconds").
      withRequiredArg().ofType(Integer.class);
    parser.accepts("d", " experiment duration in seconds").
      withRequiredArg().ofType(Integer.class);
    parser.accepts("r", "initial rate of task launches").
      withRequiredArg().ofType(Double.class);
    parser.accepts("f", "final rate of task launches").
      withRequiredArg().ofType(Double.class);
    parser.accepts("k", "step size of increased rates").
      withRequiredArg().ofType(Double.class);
    
    OptionSet options = parser.parse(args);
    
    int coreThreadPoolSize = 4;
    if (options.has("t")) {
      coreThreadPoolSize = (Integer) options.valueOf("t");
    }
    if (options.has("b")) {
      benchmarkId = (Integer) options.valueOf("b");
    }
    if (options.has("i")) {
      benchmarkIterations = (Integer) options.valueOf("i");
    }
    if (options.has("s")) {
      bucketSizeS = (Integer) options.valueOf("s");
    }
    if (options.has("d")) {
      trialLengthS = (Integer) options.valueOf("d");
    }
    if (options.has("r")) {
      startRate = (Double) options.valueOf("r");
    }
    if (options.has("f")) {
      endRate = (Double) options.valueOf("f");
    }
    if (options.has("k")) {
      rateStep = (Double) options.valueOf("k");
    }
    
    // Run the benchmark a few times to let JIT kick in
    int bucketSizeMs = bucketSizeS * 1000;
    int trialLengthMs = trialLengthS * 1000;
    runExperiment(15.0, coreThreadPoolSize, coreThreadPoolSize, bucketSizeMs, 
       trialLengthMs, new DescriptiveStatistics(), new DescriptiveStatistics());
    runExperiment(15.0, coreThreadPoolSize, coreThreadPoolSize, bucketSizeMs, 
        trialLengthMs, new DescriptiveStatistics(), new DescriptiveStatistics());

    for (double i = startRate; i <= endRate; i = i + rateStep) {
      try {
        DescriptiveStatistics runTimes = new DescriptiveStatistics();
        DescriptiveStatistics waitTimes = new DescriptiveStatistics();
        runExperiment(i, coreThreadPoolSize, coreThreadPoolSize, bucketSizeMs, 
            trialLengthMs, runTimes, waitTimes);
        System.out.println(i + " run " + runTimes.getPercentile(50.0) + " " +
            runTimes.getPercentile(95) + " " + runTimes.getPercentile(99));
        System.out.println(i + " wait " + waitTimes.getPercentile(50.0) + " " +
            waitTimes.getPercentile(95) + " " + waitTimes.getPercentile(99));
      }
      catch (RuntimeException e) {
        System.out.println(e);
        break;
      }
    }

    for (double i = startRate; i <= endRate; i = i + rateStep) {
      try {
        DescriptiveStatistics runTimes = new DescriptiveStatistics();
        DescriptiveStatistics waitTimes = new DescriptiveStatistics();
        runExperiment(i, coreThreadPoolSize, Integer.MAX_VALUE, bucketSizeMs, 
            trialLengthMs, runTimes, waitTimes);
        System.out.println(i + " run " + runTimes.getPercentile(50.0) + " " +
            runTimes.getPercentile(95) + " " + runTimes.getPercentile(99));
        System.out.println(i + " wait " + waitTimes.getPercentile(50.0) + " " +
            waitTimes.getPercentile(95) + " " + waitTimes.getPercentile(99));
      }
      catch (RuntimeException e) {
        System.out.println(e);
        break;
      }
    }
  }
}
