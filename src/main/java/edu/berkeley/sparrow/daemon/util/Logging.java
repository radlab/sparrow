package edu.berkeley.sparrow.daemon.util;

import com.google.common.base.Joiner;

public class Logging {
  private static Joiner paramJoiner = Joiner.on(",");
  
  /**
   * Return a function name (determined via reflection) and all its parameters (passed)
   * in a consistent stringformat. Very helpful in logging function calls throughout
   * our program.
   */
  public static String functionCall(Object ... params) {
    String name = Thread.currentThread().getStackTrace()[2].getMethodName();
    return name + ": [" + paramJoiner.join(params) + "]";
  }
}
