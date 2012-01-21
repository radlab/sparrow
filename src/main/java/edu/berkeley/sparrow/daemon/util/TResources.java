package edu.berkeley.sparrow.daemon.util;

import edu.berkeley.sparrow.thrift.TResourceVector;

/**
 * Helper class for working with {@link TResourceVector} objects. We would
 * normally include these functions in the {@code TResourceVector} class itself
 * but we can't since it is auto-compiled.
 */
public class TResources {
  
  /** Constructor for resources */
  public static TResourceVector createResourceVector(long memory) {
    TResourceVector out = new TResourceVector();
    out.setMemory(memory);
    return out;
  }
  
  /** Return a copy of a resoure */
  public static TResourceVector clone(TResourceVector in) {
    return createResourceVector(in.getMemory()); 
  }
  
  /** Return a new empty resource object. */
  public static TResourceVector none() {
    return createResourceVector(0);
  }
  
  /** Add the resource {@code b} to resource {@code a} */
  public static TResourceVector addTo(TResourceVector a, TResourceVector b) {
    a.setMemory(a.getMemory() + b.getMemory());
    return a;
  }
  
  /** Subtract the resource {@code b} from the resource {@code a} */
  public static TResourceVector subtractFrom(TResourceVector a, TResourceVector b) {
    a.setMemory(a.getMemory() - b.getMemory());
    return a;
  }
  
  /** Add two resources, returning a new resource. */
  public static TResourceVector add(TResourceVector a, TResourceVector b) {
    return addTo(clone(a), b);
  }
  
  /** Return whether this resource is valid. */
  public static boolean isValid(TResourceVector r) {
    return (r.memory > 0);
  }
  
  /** Return whether two resources are equal. */
  public static boolean equal(TResourceVector a, TResourceVector b) {
    return a.getMemory() == b.getMemory();
  }
  
  /** Return a comparison between two resources. */
  public static int compareTo(TResourceVector a, TResourceVector b) {
    if (a.getMemory() > b.getMemory()) {
      return 1;
    } else if (a.getMemory() == b.getMemory()) {
      return 0;
    }
    return -1;
  }
}
