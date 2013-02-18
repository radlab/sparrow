package edu.berkeley.sparrow.daemon;

/**
 * Configuration parameters for sparrow.
 */
public class SparrowConf {
  // Values: "debug", "info", "warn", "error", "fatal"
  public final static String LOG_LEVEL = "log_level";

  public final static String ZK_SERVERS = "zk.server.string";
  public final static String ZK_TIMEOUT = "zk.timeout";
  public final static String SCHEDULER_THRIFT_PORT = "scheduler.thrift.port";
  public final static String SCHEDULER_THRIFT_THREADS =
      "scheduler.thrift.threads";
  // Listen port for the state store --> scheduler interface
  public final static String SCHEDULER_STATE_THRIFT_PORT = "scheduler.state.thrift.port";
  public final static String SCHEDULER_STATE_THRIFT_THREADS =
      "scheduler.state.thrift.threads";

  /* List of ports corresponding to node monitors (backend interface) this daemon is
   * supposed to run. In most deployment scenarios this will consist of a single port,
   * or will be left unspecified in favor of the default port. */
  public final static String NM_THRIFT_PORTS = "agent.thrift.ports";

  /* List of ports corresponding to node monitors (internal interface) this daemon is
   * supposed to run. In most deployment scenarios this will consist of a single port,
   * or will be left unspecified in favor of the default port. */
  public final static String INTERNAL_THRIFT_PORTS = "internal_agent.thrift.ports";

  public final static String NM_THRIFT_THREADS = "agent.thrift.threads";
  public final static String INTERNAL_THRIFT_THREADS =
      "internal_agent.thrift.threads";
  /** Type of task scheduler to use on node monitor. Values: "fifo", "round_robin". */
  public final static String NM_TASK_SCHEDULER_TYPE = "node_monitor.task_scheduler";

  public final static String SYSTEM_MEMORY = "system.memory";
  public final static int DEFAULT_SYSTEM_MEMORY = 1024;

  public final static String SYSTEM_CPUS = "system.cpus";
  public final static int DEFAULT_SYSTEM_CPUS = 4;

  // Values: "production", "standalone", "configbased"
  public final static String DEPLYOMENT_MODE = "deployment.mode";
  public final static String DEFAULT_DEPLOYMENT_MODE = "production";

  /** Hostname of the state store. */
  public final static String STATE_STORE_HOST = "state_store.host";
  public final static String DEFAULT_STATE_STORE_HOST = "localhost";

  /** Port of the state store. */
  public final static String STATE_STORE_PORT = "state_store.port";
  public final static int DEFAULT_STATE_STORE_PORT = 20506;

  /** The ratio of probes used in a scheduling decision to tasks. */
  // For requests w/o constraints...
  public final static String SAMPLE_RATIO = "sample.ratio";
  public final static double DEFAULT_SAMPLE_RATIO = 1.05;
  // For requests w/ constraints...
  public final static String SAMPLE_RATIO_CONSTRAINED = "sample.ratio.constrained";
  public final static int DEFAULT_SAMPLE_RATIO_CONSTRAINED = 2;

  /** The hostname of this machine. */
  public final static String HOSTNAME = "hostname";

  /** The size of the task set to consider a signal from spark about allocation */
  public final static String SPECIAL_TASK_SET_SIZE = "special.task.set.size";
  public final static int DEFAULT_SPECIAL_TASK_SET_SIZE = 2;

  // Parameters for static operation (least usable system tests).
  // Expects a comma-separated list of host:port pairs describing the address of the
  // internal interface of the node monitors.
  public final static String STATIC_NODE_MONITORS = "static.node_monitors";
  public final static String STATIC_APP_NAME = "static.app.name";
  public final static String STATIC_SCHEDULERS = "static.frontends";

  public static final String GET_TASK_PORT = "get_task.port";
}
