package edu.berkeley.sparrow.daemon.util;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Utilities to aid the configuration file-based scheduler and node monitor.
 */
public class ConfigUtil {
  private final static Logger LOG = Logger.getLogger(ConfigUtil.class);

  /**
   * Parses the list of backends from a {@link Configuration}.
   *
   * Returns a map of address of backends to a {@link TResourceVector} describing the
   * total resource capacity for that backend.
   */
  public static Set<InetSocketAddress> parseBackends(
      Configuration conf) {
    if (!conf.containsKey(SparrowConf.STATIC_NODE_MONITORS)) {
      throw new RuntimeException("Missing configuration node monitor list");
    }

    Set<InetSocketAddress> backends = new HashSet<InetSocketAddress>();

    for (String node: conf.getStringArray(SparrowConf.STATIC_NODE_MONITORS)) {
      Optional<InetSocketAddress> addr = Serialization.strToSocket(node);
      if (!addr.isPresent()) {
        LOG.warn("Bad backend address: " + node);
        continue;
      }
      backends.add(addr.get());
    }

    return backends;
  }

  /**
   * Parses a list of schedulers from a {@code Configuration}
   * @return
   */
  public static List<InetSocketAddress> parseSchedulers(Configuration conf) {
    if (!conf.containsKey(SparrowConf.STATIC_SCHEDULERS)) {
      throw new RuntimeException("Missing configuration frontend list.");
    }
    List<InetSocketAddress> frontends = new ArrayList<InetSocketAddress>();
    for (String node: conf.getStringArray(SparrowConf.STATIC_SCHEDULERS)) {
      Optional<InetSocketAddress> addr = Serialization.strToSocket(node);
      if (!addr.isPresent()) {
        LOG.warn("Bad scheduler address: " + node);
        continue;
      }
      frontends.add(addr.get());
    }
    return frontends;
  }
}
