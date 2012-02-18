package edu.berkeley.sparrow.prototype;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
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
  
  public static int benchmarkIterations = DEFAULT_BENCHMARK_ITERATIONS;
  public static int benchmarkId = DEFAULT_BENCHMARK_ID;
  
  private static class BenchmarkRunnable implements Runnable {
    private HashMap<Long, List<Long>> runTimes;
    private long start;
    private long bucketGranularity;

    /**
     * A runnable which runs a floating point benchmark and updates the global list
     * of runtimes with information about how long it took. Run time is assessed
     * as the difference between when the runnable is instantiated and when it finishes
     * the benchmark loop and exists. This might include queueing in the executor.  
     */
    private BenchmarkRunnable(HashMap<Long, List<Long>> runTimes, long granularity) {
      this.runTimes = runTimes;
      start = System.currentTimeMillis();
      bucketGranularity = granularity;
    }
    
    @Override
    public void run() {
      Random r = new Random();
      ProtoBackend.runBenchmark(benchmarkId, benchmarkIterations, r);
      long end = System.currentTimeMillis();
      long timeIndex = start - (start % bucketGranularity);
      synchronized(runTimes) {
        if (!runTimes.containsKey((timeIndex))) {
          runTimes.put(timeIndex, new LinkedList<Long>());
        }
        runTimes.get(timeIndex).add(end - start);
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
  public static DescriptiveStatistics runExperiment(double arrivalRate, int corePoolSize,
      int maxPoolSize, long bucketSize, long durationMs) {
    long startTime = System.currentTimeMillis();
    long keepAliveTime = 10;
    Random r = new Random();
    BlockingQueue<Runnable> runQueue = new LinkedBlockingDeque<Runnable>();
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
        corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS, runQueue);
    
    // run times indexed by bucketing interval
    HashMap<Long, List<Long>> runTimes = 
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
      threadPool.submit((new BenchmarkRunnable(runTimes, bucketSize)));
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
    List<Long> times = new ArrayList<Long>(runTimes.keySet());
    Collections.sort(times);
    HashMap<Long, DescriptiveStatistics> bucketStats = new HashMap<Long, 
        DescriptiveStatistics>();
    
    // Remove first and last buckets since they will not be completely full to do
    // discretization. 
    times.remove(0);
    times.remove(times.size() - 1);
    
    DescriptiveStatistics allStats = new DescriptiveStatistics();
    for(Long time : times) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      List<Long> list = runTimes.get(time);
      for (Long l : list) {
        stats.addValue(l);
        allStats.addValue(l);
      }
      bucketStats.put(time, stats);
    }
    int size = bucketStats.size();
    if (size >= 2) {
      DescriptiveStatistics first = bucketStats.get(times.get(0));
      DescriptiveStatistics last = bucketStats.get(times.get(times.size() - 1));
      double increase = last.getMean() / first.getMean();
      // A simple heuristic, if the average runtime went up by ten from the first to 
      // last complete bucket, we assume we are seeing unbounded growth
      if (increase > 10.0) {
        throw new RuntimeException("Queue not in steady state: " + last.getMean() 
            + " vs " + first.getMean());
      }
    }
    return allStats;
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
    
    // Run the benchmark a few times to let JIT kick in
    runExperiment(10.0, coreThreadPoolSize, coreThreadPoolSize, 1 * 1000, 30 * 1000);
    runExperiment(10.0, coreThreadPoolSize, coreThreadPoolSize, 1 * 1000, 30 * 1000);
    for (double i = 1; i < 100.0; i = i + 2) {
      try {
        DescriptiveStatistics result = runExperiment(i, coreThreadPoolSize, 
            coreThreadPoolSize, 1 * 1000, 30 * 1000);
        System.out.println(i + " " + result.getMean() + " " +
            (result.getMean() - result.getStandardDeviation()) + " " +
            (result.getMean() + result.getStandardDeviation()));
      }
      catch (RuntimeException e) {
        System.out.println(e);
        break;
      }
    }
    
    for (double i = 1; i < 100.0; i = i + 2) {
      try {
        DescriptiveStatistics result = runExperiment(i, coreThreadPoolSize, 
            Integer.MAX_VALUE, 1 * 1000, 30 * 1000);
        System.out.println(i + " " + result.getMean() + " " +
            (result.getMean() - result.getStandardDeviation()) + " " +
            (result.getMean() + result.getStandardDeviation()));
      }
      catch (RuntimeException e) {
        System.out.println(e);
        break;
      }
    }
  }
}
