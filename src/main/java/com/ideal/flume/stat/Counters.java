package com.ideal.flume.stat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Counters {
  private static final Logger logger = LoggerFactory.getLogger("statlog");

  public static final int OPT_TOTAL_ONLY = 0x01;
  public static final int OPT_NO_PRINT = 0x02;
  public static final int OPT_NO_RESET = 0x04;

  private static Map<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
  private static Queue<Counter> counterList = new LinkedBlockingQueue<Counter>();

  public static Counter create(String group, String name, int opt) {
    String nm;
    if (null != group) {
      nm = group + "." + name;
    } else {
      nm = name;
    }
    if (counterMap.containsKey(nm)) {
      throw new IllegalArgumentException("counter with name " + nm + " already exists.");
    }
    Counter counter = new Counter(group, name, opt);
    counterMap.put(counter.getFullName(), counter);
    counterList.add(counter);
    return counter;
  }

  public static void destory(String group, String name) {
    String nm;
    if (null != group) {
      nm = group + "." + name;
    } else {
      nm = name;
    }
    Counter counter = counterMap.remove(nm);
    if (null != counter)
      counterList.remove(counter);
  }

  public static Counter get(String group, String name) {
    String nm;
    if (null != group) {
      nm = group + "." + name;
    } else {
      nm = name;
    }
    return counterMap.get(nm);
  }

  public static void reset() {
    for (Counter counter : counterList) {
      counter.reset();
    }
  }

  public static void snapshot() {
    for (Counter counter : counterList) {
      counter.snapshot();
    }
  }

  public static void enable(String group, int opt) {
    for (Counter counter : counterList) {
      if (StringUtils.equals(counter.getGroup(), group)) {
        counter.enable(opt);
      }
    }
  }

  public static void disable(String group, int opt) {
    for (Counter counter : counterList) {
      if (StringUtils.equals(counter.getGroup(), group)) {
        counter.disable(opt);
      }
    }
  }

  public static void print(PrintStream out) {
    for (Counter counter : counterList) {
      counter.print(out);
    }
  }

  public static String asString() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    for (Counter counter : counterList) {
      counter.print(stream);
    }
    return out.toString();
  }

  private static final AtomicBoolean INTERRUPTED = new AtomicBoolean(false);
  private static final AtomicBoolean PRINT_STARTED = new AtomicBoolean(false);

  public static void startPrint(final String startTime) {
    if (PRINT_STARTED.get()) {
      logger.info("print task is started.");
      return;
    }

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!INTERRUPTED.get()) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
            logger.error("", e);
          }

          Counters.snapshot();
          logger.info("start time: " + startTime + "\n" + Counters.asString());
        }
      }
    }, "print-counters");
    t.start();
    PRINT_STARTED.set(true);
  }

  public static void stopPrint() {
    INTERRUPTED.set(true);
  }

  public static class Counter {
    private long lastTimestamp;
    private final long initTimestamp;
    private final String group;
    private final String name;
    private final String fullName;
    private int opt;
    private final AtomicLong value = new AtomicLong(0);
    private long snapshot;
    private long last;

    private Counter(String group, String name, int opt) {
      super();
      this.group = group;
      this.name = name;
      if (null != group) {
        fullName = group + "." + name;
      } else {
        fullName = name;
      }
      this.opt = opt;
      this.initTimestamp = this.lastTimestamp = System.currentTimeMillis();
    }

    public long getSnapshot() {
      return snapshot;
    }

    public long getLast() {
      return last;
    }

    public String getName() {
      return name;
    }

    public long getValue() {
      return value.get();
    }

    public long add(long delta) {
      return value.getAndAdd(delta);
    }

    public long incr() {
      return value.getAndIncrement();
    }

    public long decr() {
      return value.getAndDecrement();
    }

    public void set(long newValue) {
      value.set(newValue);
    }

    public void reset() {
      if ((opt & OPT_NO_RESET) == 0) {
        set(0);
        last = snapshot = 0;
      }
    }

    public void snapshot() {
      last = snapshot;
      snapshot = value.get();
    }

    public void print(PrintStream out) {
      long now = System.currentTimeMillis();
      long diffTotal = (now - this.initTimestamp) / 1000;
      long diff = (now - this.lastTimestamp) / 1000;
      if (diffTotal == 0) {
        diffTotal = 1;
      }
      if (diff == 0) {
        diff = 1;
      }
      // if ((opt & OPT_NO_PRINT) == 0 && snapshot > 0) {
      if ((opt & OPT_NO_PRINT) == 0) {
        if ((opt & OPT_TOTAL_ONLY) != 0) {
          out.format("%s:\t%s\n", fullName, snapshot);
        } else {
          out.format("%s:\t%s\t%s\t%s\n", fullName, snapshot, snapshot / diffTotal,
              (snapshot - last) / diff);
        }
      }
      this.lastTimestamp = now;
    }

    @Override
    public String toString() {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      print(new PrintStream(out));
      return out.toString();
    }

    public int getOpt() {
      return opt;
    }

    public void enable(int opt) {
      this.opt |= opt;
    }

    public void disable(int opt) {
      this.opt &= ~opt;
    }

    public String getGroup() {
      return group;
    }

    public String getFullName() {
      return fullName;
    }

  }

}
