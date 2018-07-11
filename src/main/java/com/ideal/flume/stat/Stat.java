package com.ideal.flume.stat;

import java.io.IOException;
import java.util.Date;

import com.ideal.flume.node.Application.AgentName;
import com.ideal.flume.tools.HostnameUtils;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class Stat {
  /** 监控名称 */
  private String name;
  @JsonProperty(value = "agent_id")
  private final int agentId = AgentName.getId();
  @JsonProperty(value = "agent_name")
  private final String agentName = AgentName.getName();
  /** 采集机hostname */
  @JsonProperty(value = "host")
  private final String hostName = HostnameUtils.getHostname();
  /** 监控类型 0: source; 1: sink */
  private int type;
  /** 数据源或者输出的标识字符串 */
  @JsonProperty(value = "side")
  private String sideKey;
  /** 行数 */
  private long events;
  /** 字节数 */
  private long bytes;
  /** 行的瞬时速度 */
  @JsonProperty(value = "event_speed")
  private int eventSpeed;
  /** 字节的瞬时速度 */
  @JsonProperty(value = "byte_speed")
  private int byteSpeed;
  /** 监控时间戳 */
  private Date timestamp;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getEvents() {
    return events;
  }

  public void setEvents(long events) {
    this.events = events;
  }

  public long getBytes() {
    return bytes;
  }

  public void setBytes(long bytes) {
    this.bytes = bytes;
  }

  public int getEventSpeed() {
    return eventSpeed;
  }

  public void setEventSpeed(int eventSpeed) {
    this.eventSpeed = eventSpeed;
  }

  public int getByteSpeed() {
    return byteSpeed;
  }

  public void setByteSpeed(int byteSpeed) {
    this.byteSpeed = byteSpeed;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public String getSideKey() {
    return sideKey;
  }

  public void setSideKey(String sideKey) {
    this.sideKey = sideKey;
  }

  public String getHostName() {
    return hostName;
  }

  public int getAgentId() {
    return agentId;
  }

  public String getAgentName() {
    return agentName;
  }

  public static void main(String[] args)
      throws JsonGenerationException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    Stat s = new Stat();
    s.setName("name1");
    s.setBytes(1000000000L);
    s.setByteSpeed(20000);
    s.setEvents(3000000000L);
    s.setEventSpeed(400000);
    s.setTimestamp(new Date());
    System.out.println(mapper.writeValueAsString(s));
  }
}
