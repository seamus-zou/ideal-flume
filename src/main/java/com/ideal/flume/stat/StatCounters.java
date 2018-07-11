package com.ideal.flume.stat;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ideal.flume.stat.Counters.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatCounters {
  private static final Logger logger = LoggerFactory.getLogger(StatCounters.class);

  private static final Map<String, StatCounter> counterMap = Maps.newConcurrentMap();
  private static final List<StatCounter> counterList = Lists.newArrayList();

  private static final LinkedBlockingQueue<StatWriter> statWriters =
      new LinkedBlockingQueue<StatWriter>(10);

  public static StatCounter create(StatType type, String sideKey) {
    String key = type.name() + "." + sideKey;
    if (counterMap.containsKey(key)) {
      throw new IllegalArgumentException("counter with name " + key + " already exists.");
    }

    StatCounter counter = new StatCounter(key, type, sideKey);
    counterList.add(counter);
    counterMap.put(key, counter);
    return counter;
  }

  public static boolean addStatWriter(StatWriter writer) {
    return statWriters.offer(writer);
  }

  public static void writeStat() {
    Date now = new Date();
    List<Stat> stats = Lists.newArrayList();
    for (StatCounter counter : counterList) {
      stats.add(counter.toStat(now));
    }

    for (StatWriter writer : statWriters) {
      writer.write(stats);
    }
  }

  public static class StatCounter {
    private long lastTimestamp;
    private final String name;
    private final StatType type;
    private final String sideKey;
    private final AtomicLong events = new AtomicLong(0);
    private final AtomicLong bytes = new AtomicLong(0);
    private long snapshotEvents;
    private long lastEvents;
    private long snapshotBytes;
    private long lastBytes;

    private final Counter eventCounter;
    private final Counter byteCounter;

    public StatCounter(String name, StatType type, String sideKey) {
      this.name = name;
      this.lastTimestamp = System.currentTimeMillis();
      this.type = type;
      this.sideKey = sideKey;

      this.eventCounter = Counters.create(name, "events", 0);
      this.byteCounter = Counters.create(name, "bytes", 0);
    }

    public Stat toStat(Date nowDate) {
      long now = nowDate.getTime();
      lastBytes = snapshotBytes;
      snapshotBytes = bytes.get();
      lastEvents = snapshotEvents;
      snapshotEvents = events.get();

      long diff = (now - this.lastTimestamp) / 1000;
      if (diff == 0) {
        diff = 1;
      }

      Stat stat = new Stat();
      stat.setName(name);
      stat.setType(type.ordinal());
      stat.setSideKey(sideKey);
      stat.setBytes(snapshotBytes);
      stat.setEvents(snapshotEvents);
      stat.setByteSpeed((int) ((snapshotBytes - lastBytes) / diff));
      stat.setEventSpeed((int) ((snapshotEvents - lastEvents) / diff));
      stat.setTimestamp(nowDate);

      this.lastTimestamp = now;
      return stat;
    }

    public long addEvent(long delta) {
      eventCounter.add(delta);
      return events.getAndAdd(delta);
    }

    public long addByte(long delta) {
      byteCounter.add(delta);
      return bytes.getAndAdd(delta);
    }

    public long incrEvent() {
      eventCounter.incr();
      return events.getAndIncrement();
    }

    public long incrByte() {
      byteCounter.incr();
      return bytes.getAndIncrement();
    }

    public long decrEvent() {
      eventCounter.decr();
      return events.getAndDecrement();
    }

    public long decrByte() {
      byteCounter.decr();
      return bytes.getAndDecrement();
    }

    public void setEvent(long newValue) {
      eventCounter.set(newValue);
      events.set(newValue);
    }

    public void setByte(long newValue) {
      byteCounter.set(newValue);
      bytes.set(newValue);
    }
  }

  public static enum StatType {
    SOURCE, SINK
  }
}
