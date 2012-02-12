package edu.berkeley.sparrow.prototype;

import java.net.InetSocketAddress;
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
import org.apache.thrift.TException;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TResourceVector;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;

/**
 * Frontend for the prototype implementation.
 */
public class ProtoFrontend {
  public static final double DEFAULT_JOB_ARRIVAL_RATE_S = 10; // Jobs/second
  public static final int DEFAULT_TASKS_PER_JOB = 1;          // Tasks/job
  
  // Type of benchmark to run, see ProtoBackend static constant for benchmark types
  public static final int DEFAULT_TASK_BENCHMARK = ProtoBackend.BENCHMARK_TYPE_FP_CPU;         
  public static final int DEFAULT_BENCHMARK_ITERATIONS = 10;  // # of benchmark iterations
  
  private static final Logger LOG = Logger.getLogger(ProtoFrontend.class);

  /** A runnable which Spawns a new thread to launch a scheduling request. */
  private static class JobLaunchRunnable implements Runnable {
    private List<TTaskSpec> request;
    private SparrowFrontendClient client;
    
    public JobLaunchRunnable(List<TTaskSpec> request, SparrowFrontendClient client) {
      this.request = request;
      this.client = client;
    }
    
    @Override
    public void run() {
      TUserGroupInfo user = new TUserGroupInfo();
      user.setUser("*");
      user.setGroup("*");
      try {
        client.submitJob("testApp", request, user);
        LOG.debug("Submitted job");
      } catch (TException e) {
        LOG.error("Scheduling request failed!", e);
      }
    }
  }
  
  public static List<TTaskSpec> generateJob(int numTasks, int benchmarkId, 
      int benchmarkIterations) {
    TResourceVector resources = TResources.createResourceVector(300, 1);
    
    // Pack task parameters
    ByteBuffer message = ByteBuffer.allocate(8);
    message.putInt(benchmarkId);
    message.putInt(benchmarkIterations);
    
    List<TTaskSpec> out = new ArrayList<TTaskSpec>();
    for (int taskId = 0; taskId < numTasks; taskId++) {
      TTaskSpec spec = new TTaskSpec();
      spec.setTaskID(Integer.toString(taskId));
      spec.setMessage(message.array());
      spec.setEstimatedResources(resources);
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
      parser.accepts("c", "configuration file (required)").
        withRequiredArg().ofType(String.class);
      parser.accepts("help", "print help statement");
      OptionSet options = parser.parse(args);
      
      if (options.has("help") || !options.has("c")) {
        parser.printHelpOn(System.out);
        System.exit(-1);
      }
      
      // Logger configuration: log to the console
      BasicConfigurator.configure();
      LOG.setLevel(Level.DEBUG);
          
      String configFile = (String) options.valueOf("c");
      Configuration conf = new PropertiesConfiguration(configFile);
      
      Random r = new Random();
      double lambda = conf.getDouble("job.arrival.rate.s", DEFAULT_JOB_ARRIVAL_RATE_S);
      int tasksPerJob = conf.getInt("tasks.per.job", DEFAULT_TASKS_PER_JOB);
      int benchmarkIterations = conf.getInt("tasks.bemnchmark.iterations", 
          DEFAULT_BENCHMARK_ITERATIONS);
      int benchmarkId = conf.getInt("tasks.benchmark.id", DEFAULT_TASK_BENCHMARK);
      
      SparrowFrontendClient client = new SparrowFrontendClient();
      client.initialize(new InetSocketAddress("localhost", 
          SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT), "testApp");
      
      // Loop and generate tasks launches
      while (true) {
        // Lambda is the arrival rate in S, so we need to multiply the result here by
        // 1000 to convert to ms.
        Thread.sleep((long) (generateInterarrivalDelay(r, lambda) * 1000));
        Runnable runnable =  new JobLaunchRunnable(
            generateJob(tasksPerJob, benchmarkId, benchmarkIterations), client);
        new Thread(runnable).start();
      }
    }
    catch (Exception e) {
      LOG.error("Fatal exception", e);
    }
  }
}
