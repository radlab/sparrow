package edu.berkeley.sparrow.daemon.nodemonitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.nodemonitor.TaskScheduler.TaskDescription;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.thrift.BackendService;

/**
 * Creates a thread pool which is responsible for launching tasks on backends.
 * 
 * This class pulls runnable tasks from a {@link TaskScheduler} and executes them. 
 */
public class TaskLauncherService {
  private final static Logger LOG = Logger.getLogger(TaskLauncherService.class);
  /* The number of threads we use to launch tasks on backends. We also use this
   * to determine how many thrift connections to keep open to each backend, so that
   * in the limit case where all threads are talking to the same backend, we don't run
   * out of connections.*/
  public final static int CLIENT_POOL_SIZE = 10;
  
  /** A runnable which spins in a loop asking for tasks to launch and launching them. */
  private class TaskLaunchRunnable implements Runnable {
    @Override
    public void run() {
      while (true) {
        TaskDescription task = scheduler.getNextTask(); // blocks until task is ready
        BackendService.Client client = null;
        if (!backendClients.containsKey(task.backendSocket)) {
          createThriftClients(task.backendSocket);
        }
        try {
          client = backendClients.get(task.backendSocket).take(); // blocks until a client
                                                                  // becomes available.
        } catch (InterruptedException e) {
          LOG.fatal(e);
        }
        
        try {
          client.launchTask(task.message, task.taskId, task.user);
        } catch (TException e) {
          LOG.error("Error launching task: " + task.taskId, e);
          return; // Don't put this client back if there was an error
        }
        
        try {
          backendClients.get(task.backendSocket).put(client);
        } catch (InterruptedException e) {
          LOG.fatal(e);
        }
        
        LOG.debug("Launched task " + task.taskId);
      }
    }
  }
  
  private TaskScheduler scheduler;

  /** Cache of thrift clients pools for each backends. Clients are removed from the pool
   *  when in use. */
  private HashMap<InetSocketAddress, BlockingQueue<BackendService.Client>> backendClients =
      new HashMap<InetSocketAddress, BlockingQueue<BackendService.Client>>();
  
  public void initialize(Configuration conf, TaskScheduler scheduler) {
    this.scheduler = scheduler;
    ExecutorService service = Executors.newFixedThreadPool(CLIENT_POOL_SIZE);
    for (int i = 0; i < CLIENT_POOL_SIZE; i++) {
      service.submit(new TaskLaunchRunnable());
    }
  }
  
  /** Creates a set of thrift clients and adds them to the client pool. */
  public void createThriftClients(InetSocketAddress backendAddr) {
    BlockingQueue<BackendService.Client> clients = new 
        LinkedBlockingDeque<BackendService.Client>();
    for (int i = 0; i < CLIENT_POOL_SIZE; i++) {
      try {
        clients.put(TClients.createBlockingBackendClient(
            backendAddr.getHostName(), backendAddr.getPort()));
      } catch (InterruptedException e) {
        LOG.error("Interrupted creating thrift clients", e);
      } catch (IOException e) {
        LOG.error("Error creating thrift client", e);
      }
    }
    backendClients.put(backendAddr, clients);
  }
  
  
}