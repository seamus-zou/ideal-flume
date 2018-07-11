package com.ideal.flume.stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ideal.flume.node.Application;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.HostnameUtils;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StatWriter {
  private static final Logger logger = LoggerFactory.getLogger(StatWriter.class);

  private final ObjectMapper mapper = new ObjectMapper();

  public abstract void write(List<Stat> stats);

  public abstract void write(String stat);

  public abstract void write(Stat stat);

  protected String stat2Json(Stat stat) {
    try {
      return mapper.writeValueAsString(stat);
    } catch (IOException e) {
      logger.error("", e);
    }
    return StringUtils.EMPTY;
  }

  protected String stat2Json4Js(Stat stat) {
    StringBuffer sb = new StringBuffer();

    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("timestamp", System.currentTimeMillis() / 1000);
    jsonMap.put("host", HostnameUtils.getHostname());
    jsonMap.put("type", "monitor");

    Map<String, Integer> data = new HashMap<>();

    if (stat.getType() == StatType.SOURCE.ordinal()) {
      jsonMap.put("source", Application.AgentName.getName() + "_flume_in");
    } else if (stat.getType() == StatType.SINK.ordinal()) {
      jsonMap.put("source", Application.AgentName.getName() + "_flume_out");
    } else {
      return StringUtils.EMPTY;
    }

    data.put("events", stat.getEventSpeed());
    data.put("bytes", stat.getByteSpeed());
    jsonMap.put("data", data);

    try {
      sb.append(mapper.writeValueAsString(jsonMap));
    } catch (IOException e) {
      logger.error("", e);
      return StringUtils.EMPTY;
    }

    return sb.toString();
  }
}
