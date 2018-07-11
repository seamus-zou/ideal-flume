package com.ideal.flume.sink.database;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.google.common.base.Throwables;
import com.ideal.flume.source.database.DataBaseSourceUtil;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库sink Created by jred on 2016/12/23.
 */
public class DataBaseSink extends AbstractSink implements Configurable {

  private Logger logger = LoggerFactory.getLogger(DataBaseSink.class);

  /**
   * 取数据批量大小
   */
  private int batchSize;
    /**
     * 提交等待时间
     */
  private int timeUpperLimit;
  /**
   * 数据库字符编码
   */
  private String dbCharset;
  /**
   * 数据库表名称
   */
  private String dbTableName;
  /**
   * 内容分割字符串
   */
  private String splitStr;
  /**
   * 数据源
   */
  private DataSource dataSource;

  /**
   * 是否停止该sink
   */
  private boolean isStop = false;

  /**
   * 数据队列。用于存放待写入的数据
   */
  private LinkedBlockingQueue<String> dataQuene;

  private StatCounter statCounter;

  class DataBaseTask implements Runnable {
    private List<String> dataList = new LinkedList<String>();


    @Override
    public void run() {
      logger.info(this.getClass().getName() + "start run...");
      while (!isStop) {
        for (int i = 0; i < batchSize; i++) {
          if (dataQuene.size() == 0) {
            continue;
          } else {
            try {
              dataList.add(dataQuene.poll(1, TimeUnit.MINUTES));
            } catch (InterruptedException e) {
              logger.error(e.getMessage(), e);
            }
          }
        }
        if (dataList.size() > 0) {
          batchUpdate(dataList);
        }
      }
      logger.info(this.getClass().getName() + "run end.");

    }

    /**
     * 批量更新数据
     * 
     * @param dataList
     */
    public void batchUpdate(List<String> dataList) {
      String tmp = dataList.get(0);
      String[] rows = tmp.split(splitStr);
      logger.debug(
          "splitStr:" + splitStr + " rowsNum:" + rows.length + " dataSize:" + dataList.size());
      // 获得插入的sql语句
      String insertSql = DataBaseSinkUtil.getInsertSql(dbTableName, rows.length);
      // 获得批量插入的数据参数
      String[][] params = new String[dataList.size()][rows.length];
      for (int i = 0; i < dataList.size(); i++) {
        String row = dataList.get(i);
        String[] cols = row.split(splitStr);
        for (int j = 0; j < cols.length; j++) {
          params[i][j] = cols[j];
        }
      }
      DataBaseSinkUtil.batchInsert(dataSource, insertSql, params);
      dataList.clear();
    }
  }



  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = null;
    Event event = null;
    Long charactersSize = 0l;
    try {
      long processedEvents = 0;
      long batchEndTime = System.currentTimeMillis() + timeUpperLimit;

      transaction = channel.getTransaction();
      transaction.begin();
      for (; ((processedEvents < batchSize)||System.currentTimeMillis()<batchEndTime); processedEvents += 1) {
        event = channel.take();
        if (event == null) {
          // no events available in channel
          break;
        }
        byte[] eventBody = event.getBody();
        String data = new String(eventBody, dbCharset);

        statCounter.incrEvent();
        statCounter.addByte(eventBody.length);

        if (logger.isDebugEnabled()) {
          logger.debug("{Event} " + "msg : " + data);
          logger.debug("event #{}", processedEvents);
        }
        dataQuene.put(data);
        charactersSize += eventBody.length;
      }

      transaction.commit();
    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error("Failed to publish events", ex);
      result = Status.BACKOFF;
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (Exception e) {
          logger.error("Transaction rollback failed", e);
          throw Throwables.propagate(e);
        }
      }
      throw new EventDeliveryException(errorMsg, ex);
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }

    return result;
  }

  @Override
  public void configure(Context context) {
    logger.info("configure start...");
    batchSize = context.getInteger(DataBaseSinkConstants.BATCH_SIZE, 5000);
    timeUpperLimit = context.getInteger(DataBaseSinkConstants.TIMEUPPERLIMIT, 2000);
    dbCharset = context.getString(DataBaseSinkConstants.DB_CHARSET, "UTF-8");
    dbTableName = context.getString(DataBaseSinkConstants.DB_TABLE_NAME);
    if (StringUtils.isEmpty(dbTableName)) {
      logger.error("dbTableName must be specific.");
      throw new IllegalArgumentException("dbTableName must be specific.");
    }
    splitStr = context.getString(DataBaseSinkConstants.SPLIT_STR, "\t");
//    if (StringUtils.isEmpty(splitStr)) {
//      logger.error("splitStr must be specific.");
//      throw new IllegalArgumentException("splitStr must be specific.");
//    }
    splitStr = DataBaseSinkUtil.formartSpace(splitStr);


    statCounter = StatCounters.create(StatType.SINK, dbTableName);

    Properties dbProps = new Properties();
    DataBaseSourceUtil.setDbProperties(context, dbProps);
    if (dataSource == null) {
      dataSource = DataBaseSourceUtil.getDataSource(dbProps);
      logger.info("数据库连接成功。datasource = " + dataSource);
    }
    logger.info("configure end.");

  }

  @Override
  public synchronized void start() {
    logger.info("Starting {}...", this);
    dataQuene = new LinkedBlockingQueue<String>(5000000);
    new Thread(new DataBaseTask()).start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    this.isStop = true;
    super.stop();
    logger.info("database sink {} stopped.", getName());
  }
}
