package com.ideal.flume.source;

import static com.ideal.flume.source.KafkaSourceConstants.BATCH_DURATION_MS;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_BATCH_DURATION;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.BATCH_SIZE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_BATCH_SIZE;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseSource extends AbstractSource implements Configurable, PollableSource {

  private static final Logger logger = LoggerFactory.getLogger(HbaseSource.class);

  private String hbaseSiteXml;
  private String coreSiteXml;
  private String hdfsSiteXml;
  private String tableName;
  private HTable table;
  private byte[] startRowKey;
  private List<String> columns;

  private int batchSize;
  private int timeUpperLimit;

  private Iterator<Result> resultIterator;

  private final List<Event> eventList = Lists.newArrayList();

  private final ObjectMapper mapper = new ObjectMapper();

  private static final String KEY_ROW = "row";
  private static final String KEY_TIMESTAMP = "timestamp";

  private StatCounter statCounter;

  private Map<String, Object> convert(Result result) {
    Map<String, Object> ret = Maps.newHashMap();
    ret.put(KEY_ROW, Bytes.toString(result.getRow()));

    // all versions map
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : map
        .entrySet()) {
      Map<String, Object> qualifierMap = Maps.newHashMap();
      for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : familyEntry.getValue()
          .entrySet()) {
        Long timestamp = qualifierEntry.getValue().firstKey();
        byte[] value = qualifierEntry.getValue().get(timestamp);
        qualifierMap.put(Bytes.toString(qualifierEntry.getKey()), Bytes.toString(value));
        qualifierMap.put(KEY_TIMESTAMP, timestamp);
      }
      ret.put(Bytes.toString(familyEntry.getKey()), qualifierMap);
    }

    // NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
    // for (Entry<byte[], NavigableMap<byte[], byte[]>> entry : map.entrySet()) {
    // Map<String, String> subMap = Maps.newHashMap();
    // ret.put(Bytes.toString(entry.getKey()), subMap);
    //
    // for (Entry<byte[], byte[]> subEntry : entry.getValue().entrySet()) {
    // subMap.put(Bytes.toString(subEntry.getKey()), Bytes.toString(subEntry.getValue()));
    // }
    // }

    // for (KeyValue kv : result.raw()) {
    // // logger.info(Bytes.toString(kv.getFamily()) + "," + Bytes.toString(kv.getQualifier()) + ","
    // // + Bytes.toString(kv.getValue()) + "," + kv.getTimestamp());
    //
    // String family = Bytes.toString(kv.getFamily());
    // Map<String, Object> subMap = Maps.newHashMap();
    // ret.put(family, subMap);
    // subMap.put(Bytes.toString(kv.getQualifier()), Bytes.toString(kv.getValue()));
    // subMap.put(KEY_TIMESTAMP, kv.getTimestamp());
    // }
    return ret;
  }

  @Override
  public Status process() throws EventDeliveryException {
    long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
    try {
      while (eventList.size() < batchSize && System.currentTimeMillis() < batchEndTime) {
        if (!resultIterator.hasNext()) {
          break;
        }

        Result result = resultIterator.next();
        if (null == result) {
          continue;
        }

        String line = mapper.writeValueAsString(convert(result));
        logger.info(line);
        Event event = EventBuilder.withBody(Bytes.toBytes(line), null);
        eventList.add(event);

        statCounter.incrEvent();
        statCounter.addByte(event.getBody().length);
      }

      if (eventList.size() > 0) {
        getChannelProcessor().processEventBatch(eventList);
        eventList.clear();

        // TODO 緩存rowkey
      }

      if (!resultIterator.hasNext()) {
        return Status.BACKOFF;
      }
      return Status.READY;
    } catch (Exception ex) {
      logger.error("HbaseSource EXCEPTION, {}", ex);
      return Status.BACKOFF;
    }
  }

  @Override
  public void configure(Context context) {
    hbaseSiteXml = context.getString("hbasesite");
    Preconditions.checkState(StringUtils.isNotBlank(hbaseSiteXml),
        "Configuration must specify hbase-site.xml.");

    coreSiteXml = context.getString("coresite");
    hdfsSiteXml = context.getString("hdfssite");

    tableName = context.getString("table");
    Preconditions.checkState(StringUtils.isNotBlank(hbaseSiteXml),
        "Configuration must specify table.");

    String startRow = context.getString("startRow");
    if (StringUtils.isNotBlank(startRow)) {
      startRowKey = Bytes.toBytes(startRow);
    } else {
      startRowKey = HConstants.EMPTY_START_ROW;
    }

    String columnNames = context.getString("columns");
    if (StringUtils.isNotBlank(columnNames)) {
      columns = Lists.newArrayList(columnNames.split(","));
    } else {
      columns = Lists.newArrayList();
    }

    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    timeUpperLimit = context.getInteger(BATCH_DURATION_MS, DEFAULT_BATCH_DURATION); // ms
  }

  @Override
  public synchronized void start() {
    final Configuration hbaseConfig = HBaseConfiguration.create();
    try {
      hbaseConfig.addResource(new FileInputStream(hbaseSiteXml));
      hbaseConfig.addResource(new FileInputStream(coreSiteXml));
      hbaseConfig.addResource(new FileInputStream(hdfsSiteXml));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException("Hbase config xml file does not exist.", e);
    }

    try {
      this.table = new HTable(hbaseConfig, tableName);
    } catch (IOException e) {
      throw new IllegalArgumentException("can not get the hbase table: " + tableName + ".", e);
    }

    Scan scan = new Scan();
    // scan.setBatch(batchSize);
    scan.setStartRow(startRowKey);

    for (String column : columns) {
      scan.addFamily(Bytes.toBytes(column));
    }

    try {
      ResultScanner rs = table.getScanner(scan);
      this.resultIterator = rs.iterator();
    } catch (IOException e) {
      throw new IllegalArgumentException("can not get the scan iterator.", e);
    }

    statCounter = StatCounters.create(StatType.SOURCE, tableName);

    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }



}
