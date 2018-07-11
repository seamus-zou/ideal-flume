package com.ideal.flume.tools;

import java.lang.management.ManagementFactory;

public class PidUtils {
  public static final String PID;

  static {
    String name = ManagementFactory.getRuntimeMXBean().getName();
    PID = name.split("@")[0];
  }
}
