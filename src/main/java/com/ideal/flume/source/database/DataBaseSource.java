package com.ideal.flume.source.database;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.ideal.flume.cache.CacheClient;
import com.ideal.flume.cache.CacheUtils;
import com.ideal.flume.sink.database.DataBaseSinkConstants;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.EncoderHandler;
import com.ideal.flume.tools.HostnameUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * 数据库数据源 Created by jred on 2016/11/14.
 */
public class DataBaseSource extends AbstractSource implements Configurable, PollableSource {


  private static final Logger LOGGER = LoggerFactory.getLogger(DataBaseSource.class);

  /**
   * 批量提交数量
   */
  private int batchUpperLimit;
  /**
   * 提交时间间隔
   */
  private int timeUpperLimit;
  /**
   * 是否需要分页，默认为否
   */
  private boolean isPageable = false;
  /**
   * 分页数据大小
   */
  private int pageSize;
  /**
   * 查询语句
   */
  private String orgQuerySql;
  private String newQuerySql;
  /**
   * 表示数据是否更新的字段
   */
  private String updateFieldName;
  /**
   * 数据库的配置属性
   */
  private Properties dbProps;
  /**
   * 数据库类型
   */
  private String dbType;
  /**
   * 数据字符编码
   */
  private String dbCharset;
  /**
   * 主机名称
   */
  private String hostname;
  /**
   * 内容分割字符串
   */
  private String splitStr;



  private final List<Event> eventList = new ArrayList<Event>();

  private DataSource dataSource;

  /**
   * 上次更新时间
   */
  private long lastUpdateTime = -1;

  private int sendThreadCount;
  private boolean batchMerge;

  // 增量列名和类型
  private String incrementColumnName;
  private String incrementType;

  private CacheClient cacheClient;
  private String cacheField;
  private String cacheKey;

  private StatCounter statCounter;

  private final SimpleDateFormat SDF = new SimpleDateFormat("yyyyMMddHHmmssSSS");

  public Status process() throws EventDeliveryException {
    // 当初始化时或者1天以后才能再次读取数据
    if ((StringUtils.isNotBlank(incrementColumnName) && System.currentTimeMillis() - lastUpdateTime > 3000) || lastUpdateTime == -1
        || (System.currentTimeMillis() > (lastUpdateTime + 1000 * 60 * 60 * 24))) {
      try {
        newQuerySql = processSql(orgQuerySql, incrementColumnName, incrementType);
        LOGGER.debug("query sql: " + newQuerySql);
        
        // 判断是否需要分页
        if (isPageable) {
          long totalSize = DataBaseSourceUtil.getSize(dataSource, newQuerySql);
          int pageCount = DataBaseSourceUtil.computePage(totalSize, pageSize);
          for (int i = 1; i <= pageCount; i++) {
            try {
              List<Map<String, Object>> list =
                  DataBaseSourceUtil.getListByPage(dataSource, dbType, newQuerySql, i, pageSize);
              processEvent(list);
            } catch (Exception e) {
              LOGGER.error("处理数据出错。第" + i + "页数据.sql = " + newQuerySql + "", e);
            }
          }

        } else {
          processEvent(DataBaseSourceUtil.getList(dataSource, newQuerySql));
        }
        lastUpdateTime = System.currentTimeMillis();
        return Status.READY;
      } catch (Exception e) {
        return Status.BACKOFF;

      }
    }
    return Status.READY;
  }


  private void processEvent(List<Map<String, Object>> list) {
    if (null == list || list.size() == 0) {
      return;
    }

    Event event;
    long batchStartTime = System.currentTimeMillis();
    long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
    Map<String, String> headers = null;

    for (int n = 0; n < list.size(); n++) {
      Map<String, Object> data = list.get(n);
      StringBuilder stringBuilder = new StringBuilder();
      for (Map.Entry<String, Object> map : data.entrySet()) {
        stringBuilder.append(map.getValue() + splitStr);
      }
      try {
        headers = new HashMap<String, String>();
        headers.put("%{thread}", "1");
        headers.put("%{hostname}", hostname);
        event = EventBuilder.withBody(stringBuilder.toString().getBytes(dbCharset), headers);
        eventList.add(event);

        statCounter.incrEvent();
        statCounter.addByte(event.getBody().length);

        if (eventList.size() == batchUpperLimit && System.currentTimeMillis() > batchEndTime) {
          getChannelProcessor().processEventBatch(eventList);
          eventList.clear();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Wrote {} events to channel", eventList.size());
          }
        }

      } catch (UnsupportedEncodingException e) {
        LOGGER.error("", e);
      }

      if (n == list.size() - 1 && StringUtils.isNotBlank(incrementColumnName)) {
        // 最后一个，缓存index
        Object indexValue = data.get(incrementColumnName);
        if (null == indexValue) {
          LOGGER.error("data don't contains key: {}", incrementColumnName);
        }

        cacheIndex(indexValue.toString());
      }

    }
    // 提交为提交的数据
    if (eventList.size() > 0) {
      getChannelProcessor().processEventBatch(eventList);
      eventList.clear();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Wrote {} events to channel", eventList.size());
      }
    }

  }


  public void configure(Context context) {
    dbProps = new Properties();
    DataBaseSourceUtil.setDbProperties(context, dbProps);
//    cacheKey = dbProps.getProperty("jdbc.url") + "," + dbProps.getProperty("jdbc.userName") + ","
//        + dbProps.getProperty("jdbc.dataSource");
    cacheKey = dbProps.getProperty("jdbc.userName") + "_" + dbProps.getProperty("jdbc.dataSource");
    LOGGER.info("cache key: {}", cacheKey);
    
    cacheClient = CacheUtils.getCacheClient(cacheKey);

    batchUpperLimit = context.getInteger(DataBaseSourceConstants.BATCH_UPPER_LIMIT, 100);
    timeUpperLimit = context.getInteger(DataBaseSourceConstants.TIME_UPPER_LIMIT, 2000);
    isPageable = context.getBoolean(DataBaseSourceConstants.IS_PAGEABLE);
    pageSize = context.getInteger(DataBaseSourceConstants.PAGE_SIZE, 0);
    orgQuerySql = context.getString(DataBaseSourceConstants.QUERY_SQL);
    LOGGER.info("query sql: " + orgQuerySql);
    cacheField = EncoderHandler.encodeByMD5(orgQuerySql);
    LOGGER.info("cache field: " + cacheField);

    incrementColumnName = context.getString(DataBaseSourceConstants.INCREMENT_COLUMN);
    incrementType = context.getString(DataBaseSourceConstants.INCREMENT_TYPE);
    newQuerySql = processSql(orgQuerySql, incrementColumnName, incrementType);
    LOGGER.info("query sql: " + newQuerySql);

    updateFieldName = context.getString(DataBaseSourceConstants.UPDATE_FIELD_NAME);
    dbType = context.getString(DataBaseSourceConstants.DB_TYPE);
    dbCharset = context.getString(DataBaseSourceConstants.DB_CHARSET, "UTF-8");
    splitStr = context.getString(DataBaseSinkConstants.SPLIT_STR, "\t");
    if (dataSource == null) {
      dataSource = DataBaseSourceUtil.getDataSource(dbProps);
      LOGGER.info("数据库连接成功。datasource = " + dataSource);
    }
    hostname = HostnameUtils.getHostname();
  }

  private String processSql(String querySql, String incrementColumnName, String incrementType) {
    if (StringUtils.isBlank(querySql)) {
      throw new IllegalArgumentException("query sql must be specified.");
    }

    if (StringUtils.isBlank(incrementColumnName)) {
      return querySql;
    }

    if (StringUtils.isBlank(incrementType)) {
      throw new IllegalArgumentException("increment column type must be specified.");
    }

    incrementType = incrementType.toLowerCase();
    if ("int".equals(incrementType) || "long".equals(incrementType)
        || "timestamp".equals(incrementType)) {

    } else {
      throw new IllegalArgumentException("increment column type must be 'int', 'long' or 'date'.");
    }

    Statement s;
    try {
      s = CCJSqlParserUtil.parse(querySql);
    } catch (JSQLParserException e) {
      throw new IllegalArgumentException("can not parse the query sql", e);
    }

    Select select = (Select) s;
    PlainSelect body = (PlainSelect) select.getSelectBody();

    Expression exp = body.getWhere();

    String indexValue = getIndex();
    if (null != indexValue) {
      GreaterThan gt = new GreaterThan();
      gt.setLeftExpression(new Column(incrementColumnName));
      if ("int".equals(incrementType)) {
        gt.setRightExpression(new LongValue(Long.parseLong(indexValue)));
      } else if ("long".equals(incrementType)) {
        gt.setRightExpression(new LongValue(Long.parseLong(indexValue)));
      } else if ("timestamp".equals(incrementType)) {
        gt.setRightExpression(new TimestampValue(indexValue));
      }

      if (null == exp) {
        body.setWhere(gt);
      } else {
        AndExpression de = new AndExpression(exp, gt);
        body.setWhere(de);
      }
      
    }

    List<OrderByElement> orderByElements = new ArrayList<OrderByElement>();
    OrderByElement dd = new OrderByElement();
    dd.setExpression(new Column(incrementColumnName));
    dd.setAscDescPresent(true);
    orderByElements.add(dd);

    body.setOrderByElements(orderByElements);
    return body.toString();
  }


  @Override
  public synchronized void start() {
    super.start();

    statCounter = StatCounters.create(StatType.SOURCE, orgQuerySql);
  }

  private void cacheIndex(String v) {
    cacheClient.cache(cacheKey, cacheField, v);
  }

  private String getIndex() {
    return cacheClient.get(cacheKey, cacheField);
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }
}
