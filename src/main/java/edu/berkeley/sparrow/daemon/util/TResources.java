package edu.berkeley.sparrow.daemon.util;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.Map.Entry;

import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Helper class for working with {@link TResourceVector} objects. We would
 * normally include these functions in the {@code TResourceVector} class itself
 * but we can't since it is auto-compiled.
 */
public class TResources {
  
  /** Constructor for resources */
  public static TResourceVector createResourceVector(long memory, int cores) {
    TResourceVector out = new TResourceVector();
    out.setMemory(memory);
    out.setCores(cores);
    return out;
  }
  
  /** Constructor for resource usage. */
  public static TResourceUsage createResourceUsage(TResourceVector res, int queueLength) {
    TResourceUsage out = new TResourceUsage();
    out.queueLength = queueLength;
    out.resources = res;
    return out;
  }
  
  /** Return a copy of a resource */
  public static TResourceVector clone(TResourceVector in) {
    return createResourceVector(in.getMemory(), in.getCores()); 
  }
  
  /** Return a new empty resource object. */
  public static TResourceVector none() {
    return createResourceVector(0, 0);
  }
  
  /** Add the resource {@code b} to resource {@code a} */
  public static TResourceVector addTo(TResourceVector a, TResourceVector b) {
    a.setMemory(a.getMemory() + b.getMemory());
    a.setCores(a.getCores() + b.getCores());
    return a;
  }
  
  /** Subtract the resource {@code b} from the resource {@code a} */
  public static TResourceVector subtractFrom(TResourceVector a, TResourceVector b) {
    a.setMemory(a.getMemory() - b.getMemory());
    a.setCores(a.getCores() - b.getCores());
    return a;
  }
  
  /** Add two resources, returning a new resource. */
  public static TResourceVector add(TResourceVector a, TResourceVector b) {
    return addTo(clone(a), b);
  }
  
  /** Subtract resource {@code b} from resource {@code a} and return the result */
  public static TResourceVector subtract(TResourceVector a, TResourceVector b) {
    return (subtractFrom(clone(a), b));
  }
  
  /** Return whether this resource is valid. */
  public static boolean isValid(TResourceVector r) {
    return (r.memory >= 0 && r.cores >= 0);
  }
  
  /** Return whether two resources are equal. */
  public static boolean equal(TResourceVector a, TResourceVector b) {
    return ((a.getMemory() == b.getMemory()) && (a.getCores() == b.getCores()));
  }
  
  /** Return whether resource {@code a} is less than or equal resource {@code b}, 
   *  meaning all pairwise comparisons fulfill {@code a <= b}. 
   */
  public static boolean isLessThanOrEqualTo(TResourceVector a, TResourceVector b) {
    return (a.getMemory() <= b.getMemory()) && (a.getCores() <= b.getCores());
  }
  
  /**
   * First compares nodes based on free CPU's. If two nodes both have the same number of
   * CPU's in use, breaks ties using the queue length. Since the queue length is only
   * expected to be greater than 1 when all CPU's are in use, this effectively ranks 
   * all fully loaded nodes in order of queue length.
   */
  public static class CPUThenQueueComparator implements Comparator<TResourceUsage> {
    private Comparator<TResourceUsage> queueComp = new MinQueueComparator();
    private Comparator<TResourceUsage> cpuComp = new MinCPUComparator();
    @Override
    public int compare(TResourceUsage u1, TResourceUsage u2) {
      int res = cpuComp.compare(u1, u2);
      if (res == 0) {
        return queueComp.compare(u1, u2);
      } else {
        return res;
      }
    }
  }
  
  public static class MinQueueComparator implements Comparator<TResourceUsage> {
    @Override
    public int compare(TResourceUsage u1, TResourceUsage u2) {
      int q1 = u1.queueLength;
      int q2 = u2.queueLength;
      if (q1 > q2) { return 1; }
      if (q1 < q2) { return -1; }
      return 0;
    }
  }
  public static class MinCPUComparator implements Comparator<TResourceUsage> {
    @Override
    public int compare(TResourceUsage u1, TResourceUsage u2) {
      int c1 = u1.getResources().cores;
      int c2 = u2.getResources().cores;
      if (c1 > c2) { return 1; }
      if (c1 < c2) { return -1; }
      return 0;
    }
  }
  
  /**
   * Function for making an entry comparator out of a resource comparator. Useful
   * when sorting entries of nodes with associated resources.
   */
  public static Comparator<Entry<InetSocketAddress, TResourceUsage>> makeEntryComparator(
      final Comparator<TResourceUsage> comparator) {
    return new Comparator<Entry<InetSocketAddress, TResourceUsage>>() {
      public int compare(Entry<InetSocketAddress, TResourceUsage> e1, 
          Entry<InetSocketAddress, TResourceUsage> e2) {
        return comparator.compare(e1.getValue(), e2.getValue());
      }
    };
  }
  
}
