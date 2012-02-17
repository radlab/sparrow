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
  public static final int BENCHMARK_ITERATIONS = 4;
  
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
      this.bucketGranularity = granularity;
    }
    
    @Override
    public void run() {
      Random r = new Random();
      int opsPerIteration = 1000 * 1000;
      // We keep a running result here and print it out so that the JVM doesn't
      // optimize all this computation away.
      float result = r.nextFloat();
      for (int i = 0; i < BENCHMARK_ITERATIONS * opsPerIteration; i++) {
        // On each iteration, perform a floating point mulitplication
        float x = r.nextFloat();
        float y = r.nextFloat();
        result += (x * y);
      }
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
   * setting it equal to {@code corePoolSize} creates a fixed size run queue.
   * 
   * The derivative of task finishes are tracked by bucketing tasks at the granularity
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
    
    long currTime = startTime;
    while (true) {
      long delay = (long) (generateInterarrivalDelay(r, arrivalRate) * 1000);
      long nextTime = currTime + delay;
      long diff = Math.max(0, nextTime - System.currentTimeMillis());
      currTime = nextTime;
      try {
        Thread.sleep(diff);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
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
    }
    List<Long> times = new ArrayList<Long>(runTimes.keySet());
    Collections.sort(times);
    HashMap<Long, DescriptiveStatistics> bucketStats = new HashMap<Long, 
        DescriptiveStatistics>();
    
    // Remove first and last buckets since they will not be completely full to do
    // discretization. 
    times.remove(times.get(0));
    times.remove(times.get(times.size() - 1));
    
    DescriptiveStatistics allStats = new DescriptiveStatistics();
    for(Long time : times) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      List<Long> list = runTimes.get(time);
      synchronized(list){
        for (Long l : list) {
          stats.addValue(l);
          allStats.addValue(l);
        }
      }
      bucketStats.put(time, stats);
    }
    int size = bucketStats.size();
    if (size >= 2) {
      DescriptiveStatistics first = bucketStats.get(times.get(0));
      DescriptiveStatistics last = bucketStats.get(times.get(times.size() - 1));
      double increase = last.getMean() / first.getMean();
      // A simple heuristic, if the average runtime went up by ten from the first to 
      // last complete bucket, we assume i
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
   * queue and the other where tasks are not queued and always launched in parallel.
   * The statistics for runtime vs. arrival rate are printed out for each of the two
   * trials. 
   */
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    OptionParser parser = new OptionParser();
    parser.accepts("t", "size of core thread pool").
      withRequiredArg().ofType(Integer.class);
    
    OptionSet options = parser.parse(args);
    
    int coreThreadPoolSize = 4;
    if (options.has("t")) {
      coreThreadPoolSize = (Integer) options.valueOf("t");
    }
    
    // Run the benchmark a few times to let JIT kick in
    runExperiment(10.0, coreThreadPoolSize, coreThreadPoolSize, 1 * 1000, 30 * 1000);
    runExperiment(10.0, coreThreadPoolSize, coreThreadPoolSize, 1 * 1000, 30 * 1000);
    for (double i = 1; i <= 100.0; i = i + 2) {
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
    
    for (double i = 1; i <= 100.0; i = i + 2) {
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
