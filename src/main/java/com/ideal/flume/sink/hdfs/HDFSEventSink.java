/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.ideal.flume.sink.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ideal.flume.cache.RedisClientUtils;
import com.ideal.flume.cache.RedisLock;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.Counters.Counter;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.DateTimeUtils;
import com.ideal.flume.tools.SizedLinkedHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.hdfs.BucketClosedException;
import org.apache.flume.sink.hdfs.HDFSWriter;
import org.apache.flume.sink.hdfs.KerberosUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;

public class HDFSEventSink extends AbstractSink implements Configurable {
  private final static Logger logger = LoggerFactory.getLogger(HDFSEventSink.class);

  public interface WriterCallback {
    public void run(String filePath);
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDFSEventSink.class);

  private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");

  private static final long defaultRollInterval = 30;
  private static final long defaultRollSize = 1024;
  private static final long defaultRollCount = 10;
  private static final String defaultFileName = "FlumeData";
  private static final String defaultSuffix = "";
  private static final String defaultInUsePrefix = "";
  private static final String defaultInUseSuffix = ".tmp";
  private static final int defaultBatchSize = 100;
  private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  private static final int defaultMaxOpenFiles = 5000;
  // Time between close retries, in seconds
  private static final long defaultRetryInterval = 180;
  // Retry forever.
  private static final int defaultTryCount = Integer.MAX_VALUE;

  /**
   * Default length of time we wait for blocking BucketWriter calls before timing out the operation.
   * Intended to prevent server hangs.
   */
  private static final long defaultCallTimeout = 10000;
  /**
   * Default number of threads available for tasks such as append/open/close/flush with hdfs. These
   * tasks are done in a separate thread in the case that they take too long. In which case we
   * create a new file and move on.
   */
  private static final int defaultThreadPoolSize = 10;
  private static final int defaultRollTimerPoolSize = 1;

  /**
   * Singleton credential manager that manages static credentials for the entire JVM
   */
  private static final AtomicReference<KerberosUser> staticLogin =
      new AtomicReference<KerberosUser>();

  private final HDFSWriterFactory writerFactory;

  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private int batchSize;
  private int threadsPoolSize;
  private int rollTimerPoolSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String fileType;
  private String filePath;
  private String fileName;
  private String suffix;
  private String inUsePrefix;
  private String inUseSuffix;
  private TimeZone timeZone;
  private int maxOpenFiles;
  private ExecutorService callTimeoutPool;
  private ScheduledExecutorService timedRollerPool;

  private String kerbConfPrincipal;
  private String kerbKeytab;
  private String proxyUserName;
  private UserGroupInformation proxyTicket;

  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;

  private long callTimeout;
  private Context context;

  private volatile int idleTimeout;
  private FileSystem mockFs;
  private HDFSWriter mockWriter;
  private long retryInterval;
  private int tryCount;
  private int threadCount;

  private SimpleBucketPath simpleBucketPath;

  private SendRunnable[] sendThreads;
  private Map<String, SendRunnable> pathThreadMap;
  private Counter sendQueueCounter;

  private String exterShellCmd = ""; // 外部shell命令字符串

  private String currentDay = ""; // 当前日期

  private String currentHour = ""; // 当前小时

  private String dayFormat = ""; // 日期格式

  private String lastDay = ""; // 上次日期

  private String lastHour = ""; // 上次小时

  private HdfsSinkTimeGroupCounter hdfsSinkTimeGroupCounter;
  /**
   * redis客户端
   */
  private JedisCommands jedisCommands;
  /**
   * redis地址
   */
  private String redisUrl;
  /**
   * redis认证密码
   */
  private String redisPwd;
  /**
   * hadoop用户名
   */
  private String schemaName;
  /**
   * hive的表名称
   */
  private String tableName;
  /**
   * 是否开启数据按时间分组统计
   */
  private boolean isCountTimeGroup = false;

  private RedisLock redisLock;
  /**
   * 临界时间
   */
  private int criticalTime;

  private StatCounter statCounter;

  public HDFSEventSink() {
    this(new HDFSWriterFactory());
  }

  public HDFSEventSink(HDFSWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  // read configuration and setup thresholds
  @Override
  public void configure(Context context) {
    this.simpleBucketPath = new SimpleBucketPath();

    this.context = context;

    filePath = Preconditions.checkNotNull(context.getString("hdfs.path"), "hdfs.path is required");
    fileName = context.getString("hdfs.filePrefix", defaultFileName);

    statCounter = StatCounters.create(StatType.SINK, getName());

    this.suffix = context.getString("hdfs.fileSuffix", defaultSuffix);
    inUsePrefix = context.getString("hdfs.inUsePrefix", defaultInUsePrefix);
    inUseSuffix = context.getString("hdfs.inUseSuffix", defaultInUseSuffix);
    String tzName = context.getString("hdfs.timeZone");
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    rollInterval = context.getLong("hdfs.rollInterval", defaultRollInterval);
    rollSize = context.getLong("hdfs.rollSize", defaultRollSize);
    rollCount = context.getLong("hdfs.rollCount", defaultRollCount);
    batchSize = context.getInteger("hdfs.batchSize", defaultBatchSize);
    idleTimeout = context.getInteger("hdfs.idleTimeout", 0);
    String codecName = context.getString("hdfs.codeC");
    fileType = context.getString("hdfs.fileType", defaultFileType);
    maxOpenFiles = context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles);
    callTimeout = context.getLong("hdfs.callTimeout", defaultCallTimeout);
    threadsPoolSize = context.getInteger("hdfs.threadsPoolSize", defaultThreadPoolSize);
    rollTimerPoolSize = context.getInteger("hdfs.rollTimerPoolSize", defaultRollTimerPoolSize);
    kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal", "");
    kerbKeytab = context.getString("hdfs.kerberosKeytab", "");
    proxyUserName = context.getString("hdfs.proxyUser", "");
    tryCount = context.getInteger("hdfs.closeTries", defaultTryCount);
    if (tryCount <= 0) {
      LOG.warn("Retry count value : " + tryCount + " is not "
          + "valid. The sink will try to close the file until the file " + "is eventually closed.");
      tryCount = defaultTryCount;
    }
    retryInterval = context.getLong("hdfs.retryInterval", defaultRetryInterval);
    if (retryInterval <= 0) {
      LOG.warn("Retry Interval value: " + retryInterval + " is not "
          + "valid. If the first close of a file fails, "
          + "it may remain open and will not be renamed.");
      tryCount = 1;
    }

    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");
    if (codecName == null) {
      codeC = null;
      compType = CompressionType.NONE;
    } else {
      codeC = getCodec(codecName);
      // TODO : set proper compression type
      compType = CompressionType.BLOCK;
    }

    // Do not allow user to set fileType DataStream with codeC together
    // To prevent output file with compress extension (like .snappy)
    if (fileType.equalsIgnoreCase(HDFSWriterFactory.DataStreamType) && codecName != null) {
      throw new IllegalArgumentException("fileType: " + fileType
          + " which does NOT support compressed output. Please don't set codeC"
          + " or change the fileType if compressed output is desired.");
    }

    if (fileType.equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
      Preconditions.checkNotNull(codeC,
          "It's essential to set compress codec" + " when fileType is: " + fileType);
    }

    if (!authenticate()) {
      LOG.error("Failed to authenticate!");
    }
    needRounding = context.getBoolean("hdfs.round", false);

    if (needRounding) {
      String unit = context.getString("hdfs.roundUnit", "second");
      if (unit.equalsIgnoreCase("hour")) {
        this.roundUnit = Calendar.HOUR_OF_DAY;
      } else if (unit.equalsIgnoreCase("minute")) {
        this.roundUnit = Calendar.MINUTE;
      } else if (unit.equalsIgnoreCase("second")) {
        this.roundUnit = Calendar.SECOND;
      } else {
        LOG.warn("Rounding unit is not valid, please set one of"
            + "minute, hour, or second. Rounding will be disabled");
        needRounding = false;
      }
      this.roundValue = context.getInteger("hdfs.roundValue", 1);
      if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
            "Round value" + "must be > 0 and <= 60");
      } else if (roundUnit == Calendar.HOUR_OF_DAY) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
            "Round value" + "must be > 0 and <= 24");
      }
    }

    this.useLocalTime = context.getBoolean("hdfs.useLocalTimeStamp", false);

    this.threadCount = context.getInteger("threadCount", 4);
    this.threadsPoolSize = (int) Math.floor(threadCount * 1.5);
    formatDateFormat(filePath);
    exterShellCmd = context.getString("exterShellCmd");
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(exterShellCmd)
        && exterShellCmd.indexOf("${day}") < 0) {
      throw new IllegalArgumentException(
          "configrue filed: exterShellCmd must contain '${day}' or '${day}' and '${hour}'. ");
    }


    isCountTimeGroup = context.getBoolean("isCountTimeGroup", false);
    if (isCountTimeGroup) {
      redisUrl = context.getString("redisUrl");
      redisPwd = context.getString("redisPwd");
      schemaName = context.getString("schemaName");
      tableName = context.getString("tableName");
      if (org.apache.commons.lang3.StringUtils.isEmpty(redisUrl)
          || org.apache.commons.lang3.StringUtils.isEmpty(schemaName)
          || org.apache.commons.lang3.StringUtils.isEmpty(tableName)) {
        throw new IllegalArgumentException(
            "configrue filed: counter time group must specific redis url,schemaName, tableName. ");
      }
      if (jedisCommands == null) {
        if (StringUtils.isEmpty(redisPwd)) {
          jedisCommands = RedisClientUtils.getJedisCmd(redisUrl);
        } else {
          jedisCommands = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
        }
      }
      if (hdfsSinkTimeGroupCounter == null) {
        hdfsSinkTimeGroupCounter = new HdfsSinkTimeGroupCounter();
        hdfsSinkTimeGroupCounter.setHadoopUserName(schemaName);
        hdfsSinkTimeGroupCounter.setHiveTableName(tableName);
        // 初始化统计数据到redis
        Map<String, String> dataMap = new HashMap<String, String>();
        dataMap.put("schemaName", schemaName);
        dataMap.put("tableName", tableName);
        redisLock = new RedisLock(tableName, jedisCommands);
        try {
          redisLock.lock();
          jedisCommands.hmset(tableName, dataMap);
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        } finally {
          redisLock.unLock();
        }
      }
    }
    criticalTime = context.getInteger("criticalTime", -1);

  }


  private void formatDateFormat(String filePath) {
    if (filePath.indexOf("%Y") > 0) {
      dayFormat = filePath.substring(filePath.indexOf("%Y"), filePath.indexOf("%d") + 2);
      dayFormat = dayFormat.replace("%Y", "yyyy").replace("%m", "MM").replace("%d", "dd");
    }
  }

  private static boolean codecMatches(Class<? extends CompressionCodec> cls, String codecName) {
    String simpleName = cls.getSimpleName();
    if (cls.getName().equals(codecName) || simpleName.equalsIgnoreCase(codecName)) {
      return true;
    }
    if (simpleName.endsWith("Codec")) {
      String prefix = simpleName.substring(0, simpleName.length() - "Codec".length());
      if (prefix.equalsIgnoreCase(codecName)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static CompressionCodec getCodec(String codecName) {
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    CompressionCodec codec = null;
    ArrayList<String> codecStrs = new ArrayList<String>();
    codecStrs.add("None");
    for (Class<? extends CompressionCodec> cls : codecs) {
      codecStrs.add(cls.getSimpleName());
      if (codecMatches(cls, codecName)) {
        try {
          codec = cls.newInstance();
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate " + cls + " class");
        } catch (IllegalAccessException e) {
          LOG.error("Unable to access " + cls + " class");
        }
      }
    }

    if (codec == null) {
      if (!codecName.equalsIgnoreCase("None")) {
        throw new IllegalArgumentException(
            "Unsupported compression codec " + codecName + ".  Please choose from: " + codecStrs);
      }
    } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
      // Must check instanceof codec as BZip2Codec doesn't inherit
      // Configurable
      // Must set the configuration for Configurable objects that may or
      // do use
      // native libs
      ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
    }
    return codec;
  }

  class SendRunnable extends Thread {
    WriterLinkedHashMap sfWriters;
    final Object sfWritersLock = new Object();

    private final Map<String, LinkedBlockingQueue<Event>> msgQueueMap =
        new ConcurrentHashMap<String, LinkedBlockingQueue<Event>>();
    private boolean interrupted = false;

    public SendRunnable(String name) {
      super.setName(name);
    }

    @Override
    public void interrupt() {
      this.interrupted = true;
    }

    @Override
    public boolean isInterrupted() {
      return this.interrupted;
    }

    public void addEvent(Event event) {
      try {
        String lookupPath = event.getHeaders().get("realPath") + DIRECTORY_DELIMITER
            + event.getHeaders().get("realName");
        LinkedBlockingQueue<Event> que = msgQueueMap.get(lookupPath);
        if (null == que) {
          que = new LinkedBlockingQueue<Event>(5000000);
          msgQueueMap.put(lookupPath, que);
        }
        que.put(event);
        sendQueueCounter.incr();
      } catch (InterruptedException e) {
        logger.info("", e);
      }
    }

    private void send(Event event, long size, String lookupPath, String realPath, String realName)
        throws Exception {
      BucketWriter bucketWriter;
      HDFSWriter hdfsWriter = null;
      // Callback to remove the reference to the bucket writer from the
      // sfWriters map so that all buffers used by the HDFS file
      // handles are garbage collected.
      WriterCallback closeCallback = new WriterCallback() {
        @Override
        public void run(String bucketPath) {
          LOG.info("Writer callback called.");
          synchronized (sfWritersLock) {
            sfWriters.remove(bucketPath);
          }
        }
      };
      synchronized (sfWritersLock) {
        bucketWriter = sfWriters.get(lookupPath);
        // we haven't seen this file yet, so open it and cache the
        // handle
        if (bucketWriter == null) {
          hdfsWriter = writerFactory.getWriter(fileType);
          bucketWriter =
              initializeBucketWriter(realPath, realName, lookupPath, hdfsWriter, closeCallback);
          sfWriters.put(lookupPath, bucketWriter);
        }
      }

      // Write the data to HDFS
      try {
        bucketWriter.append(event);
      } catch (BucketClosedException ex) {
        LOG.info("Bucket was closed while trying to append, "
            + "reinitializing bucket and writing event.");
        hdfsWriter = writerFactory.getWriter(fileType);
        bucketWriter =
            initializeBucketWriter(realPath, realName, lookupPath, hdfsWriter, closeCallback);
        synchronized (sfWritersLock) {
          sfWriters.put(lookupPath, bucketWriter);
        }
        bucketWriter.append(event);
      }
      bucketWriter.addEventCounter(size);
    }

    @Override
    public void run() {
      while (!interrupted || msgQueueMap.size() > 0) {
        if (msgQueueMap.size() == 0) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
          }
          continue;
        }

        Set<String> paths = msgQueueMap.keySet();
        for (String lookupPath : paths) {
          LinkedBlockingQueue<Event> msgQueue = msgQueueMap.remove(lookupPath);
          if (null == msgQueue || msgQueue.size() == 0) {
            continue;
          }

          try {
            Event tmpEvent = msgQueue.peek();
            String realPath = tmpEvent.getHeaders().get("realPath");
            String realName = tmpEvent.getHeaders().get("realName");

            List<Event> toSends = new ArrayList<Event>(batchSize);
            int drainLen = msgQueue.drainTo(toSends, batchSize);

            while (drainLen > 0) {
              Event event = new SimpleEvent();
              ByteArrayOutputStream out = new ByteArrayOutputStream(drainLen * 256);
              long bodyLen = 0;

              Iterator<Event> it = toSends.iterator();
              Event e = it.next();
              while (null != e) {
                out.write(e.getBody());
                bodyLen += e.getBody().length;

                if (it.hasNext()) {
                  e = it.next();
                  out.write('\n');
                } else {
                  e = null;
                }
              }
              event.setBody(out.toByteArray());
              out.close();

              try {
                send(event, drainLen, lookupPath, realPath, realName);
              } catch (Throwable e1) {
                logger.error("", e1);
              } finally {
                sendQueueCounter.add(-drainLen);
              }

              statCounter.addEvent(drainLen);
              statCounter.addByte(bodyLen);

              if (isCountTimeGroup) {
                hdfsSinkTimeGroupCounter.add(drainLen);
              }

              toSends.clear();
              drainLen = msgQueue.drainTo(toSends, batchSize);
            }
          } catch (Throwable e) {
            logger.error("", e);
          }
        }
      }
    }
  }

  /**
   * Pull events out of channel and send it to HDFS. Take at most batchSize events per Transaction.
   * Find the corresponding bucket for the event. Ensure the file is open. Serialize the data and
   * write it to the file on HDFS. <br/>
   * This method is not thread safe.
   */
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    try {
      int txnEventCount = 0;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();
        if (event == null) {
          break;
        }

        String realPath = simpleBucketPath.simpleEscapeString(filePath, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime, criticalTime);
        String realName = simpleBucketPath.simpleEscapeString(fileName, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime, criticalTime);

        // String realPath = BucketPath.escapeString(filePath, event.getHeaders(), timeZone,
        // needRounding, roundUnit,
        // roundValue, useLocalTime);
        // String realName = BucketPath.escapeString(fileName, event.getHeaders(), timeZone,
        // needRounding, roundUnit,
        // roundValue, useLocalTime);

        String lookupPath = realPath + DIRECTORY_DELIMITER + realName;

        event.getHeaders().put("realPath", realPath);
        event.getHeaders().put("realName", realName);

        SendRunnable sendThread = pathThreadMap.get(lookupPath);
        if (null == sendThread) {
          sendThread = sendThreads[Math.abs(lookupPath.hashCode() % threadCount)];
          pathThreadMap.put(lookupPath, sendThread);
        }
        sendThread.addEvent(event);


        String timeStampStr = "";
        if (useLocalTime) {
          timeStampStr = System.currentTimeMillis() + "";
        } else {
          timeStampStr = event.getHeaders().get("timestamp");

        }
        // 如果可以切换时间，则执行以下逻辑
        if (isChangeTime(timeStampStr)) {
          // 如果需要按时间统计数据，则同步数据到redis
          if (isCountTimeGroup) {
            syncCounterToReids();
          }
          // 切换时间
          changeTime(timeStampStr);

          if (StringUtils.isNotEmpty(exterShellCmd)) {
            // 执行外部shell命令
            executeExterShellCmd();
          }
        }

      }

      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        return Status.READY;
      }
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("process failed", th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      transaction.close();
    }
  }


  /**
   * 执行外部shell命令
   */
  private void executeExterShellCmd() {
    HdfsEventSinkUtil.executeExterShellCmd(exterShellCmd, currentDay, currentHour);
  }


  /**
   * 同步按时间分组统计数据到redis中
   */
  private void syncCounterToReids() {
    String keyField;
    if (dayFormat.toLowerCase().indexOf("hh") > 0) {
      keyField = lastDay + lastHour;
    } else {
      keyField = lastDay;
    }
    if (StringUtils.isNotEmpty(keyField)) {
      try {
        redisLock.lock();
        String fileValue = jedisCommands.hget(tableName, keyField);
        long value = fileValue == null ? hdfsSinkTimeGroupCounter.getCountValue()
            : (Long.valueOf(fileValue) + hdfsSinkTimeGroupCounter.getCountValue());
        jedisCommands.hset(tableName, keyField, value + "");
        logger.info("======| sync data to redis. keyField=" + keyField + ";value=" + value);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      } finally {
        // 解锁
        redisLock.unLock();
      }
      // 统计数清零
      hdfsSinkTimeGroupCounter.resetVal();
    }

  }


  /**
   * 是否切换时间
   * 
   * @param timeStampStr 毫秒数
   * @return
   */
  private boolean isChangeTime(String timeStampStr) {
    Calendar ca = Calendar.getInstance();
    ca.setTimeInMillis(Long.valueOf(timeStampStr));
    currentDay = DateTimeUtils.customDateTime(ca, dayFormat);
    int hour = ca.get(ca.HOUR_OF_DAY);
    currentHour = hour < 10 ? "0" + hour : hour + "";
    processCriticalTime(hour);
    boolean isChange = false;
    if (filePath.indexOf("%H") < 0) {
      isChange = !currentDay.equals(lastDay);
    } else {
      isChange = !currentHour.equals(lastHour);
    }
    if (isChange) {
      return true;
    }
    return false;
  }

  /**
   * 切换时间
   * 
   * @param timeStampStr
   */
  private void changeTime(String timeStampStr) {
    Calendar ca = Calendar.getInstance();
    ca.setTimeInMillis(Long.valueOf(timeStampStr));
    currentDay = DateTimeUtils.customDateTime(ca, dayFormat);
    int hour = ca.get(ca.HOUR_OF_DAY);
    currentHour = hour < 10 ? "0" + hour : hour + "";
    processCriticalTime(hour);
    lastDay = currentDay;
    lastHour = currentHour;
  }


  /**
   * 处理时间边界业务
   * 
   * @param hour
   */
  private void processCriticalTime(int hour) {
    // 如果存在时间边界业务
    if (criticalTime > -1) {
      if (hour < criticalTime) {
        String tmpDay = currentDay;
        long times = DateTimeUtils.parserDateTime(currentDay).getTime();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(times);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        currentDay = DateTimeUtils.customDateTime(calendar, dayFormat);
        currentHour = tmpDay + currentHour;
      }
    }

  }



  private BucketWriter initializeBucketWriter(String realPath, String realName, String lookupPath,
      HDFSWriter hdfsWriter, WriterCallback closeCallback) {
    BucketWriter bucketWriter = new BucketWriter(rollInterval, rollSize, rollCount, batchSize,
        context, realPath, realName, inUsePrefix, inUseSuffix, suffix, codeC, compType, hdfsWriter,
        timedRollerPool, proxyTicket, idleTimeout, closeCallback, lookupPath, callTimeout,
        callTimeoutPool, retryInterval, tryCount);
    if (mockFs != null) {
      bucketWriter.setFileSystem(mockFs);
      bucketWriter.setMockStream(mockWriter);
    }
    return bucketWriter;
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    for (SendRunnable sendThread : sendThreads) {
      synchronized (sendThread.sfWritersLock) {
        for (Entry<String, BucketWriter> entry : sendThread.sfWriters.entrySet()) {
          LOG.info("Closing {}", entry.getKey());

          try {
            entry.getValue().close();
          } catch (Exception ex) {
            LOG.warn("Exception while closing " + entry.getKey() + ". " + "Exception follows.", ex);
            if (ex instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    }

    // shut down all our thread pools
    ExecutorService toShutdown[] = {callTimeoutPool, timedRollerPool};
    for (ExecutorService execService : toShutdown) {
      execService.shutdown();
      try {
        while (execService.isTerminated() == false) {
          execService.awaitTermination(Math.max(defaultCallTimeout, callTimeout),
              TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ex) {
        LOG.warn("shutdown interrupted on " + execService, ex);
      }
    }

    callTimeoutPool = null;
    timedRollerPool = null;

    for (SendRunnable sendThread : sendThreads) {
      sendThread.sfWriters.clear();
      sendThread.sfWriters = null;
      sendThread.interrupt();
    }
    // 同步统计数据到Redis数据库中
    syncCounterToReids();
    super.stop();

  }

  @Override
  public void start() {
    String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
    callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
        new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

    String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
    timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
        new ThreadFactoryBuilder().setNameFormat(rollerName).build());

    sendThreads = new SendRunnable[threadCount];
    for (int i = 0; i < threadCount; i++) {
      SendRunnable s = new SendRunnable(getName() + "-send-" + i);
      s.sfWriters = new WriterLinkedHashMap(maxOpenFiles);
      sendThreads[i] = s;
      s.start();
    }
    pathThreadMap = new SizedLinkedHashMap<String, SendRunnable>(64);

    sendQueueCounter = Counters.create("send-queue", getName(), 1);

    super.start();
  }



  private boolean authenticate() {

    // logic for kerberos login
    boolean useSecurity = UserGroupInformation.isSecurityEnabled();

    LOG.info("Hadoop Security enabled: " + useSecurity);

    if (useSecurity) {

      // sanity checking
      if (kerbConfPrincipal.isEmpty()) {
        LOG.error("Hadoop running in secure mode, but Flume config doesn't "
            + "specify a principal to use for Kerberos auth.");
        return false;
      }
      if (kerbKeytab.isEmpty()) {
        LOG.error("Hadoop running in secure mode, but Flume config doesn't "
            + "specify a keytab to use for Kerberos auth.");
        return false;
      } else {
        // If keytab is specified, user should want it take effect.
        // HDFSEventSink will halt when keytab file is non-exist or
        // unreadable
        File kfile = new File(kerbKeytab);
        if (!(kfile.isFile() && kfile.canRead())) {
          throw new IllegalArgumentException(
              "The keyTab file: " + kerbKeytab + " is nonexistent or can't read. "
                  + "Please specify a readable keytab file for Kerberos auth.");
        }
      }

      String principal;
      try {
        // resolves _HOST pattern using standard Hadoop search/replace
        // via DNS lookup when 2nd argument is empty
        principal = SecurityUtil.getServerPrincipal(kerbConfPrincipal, "");
      } catch (IOException e) {
        LOG.error("Host lookup error resolving kerberos principal (" + kerbConfPrincipal
            + "). Exception follows.", e);
        return false;
      }

      Preconditions.checkNotNull(principal, "Principal must not be null");
      KerberosUser prevUser = staticLogin.get();
      KerberosUser newUser = new KerberosUser(principal, kerbKeytab);

      // be cruel and unusual when user tries to login as multiple
      // principals
      // this isn't really valid with a reconfigure but this should be
      // rare
      // enough to warrant a restart of the agent JVM
      // TODO: find a way to interrogate the entire current config state,
      // since we don't have to be unnecessarily protective if they switch
      // all
      // HDFS sinks to use a different principal all at once.
      Preconditions.checkState(prevUser == null || prevUser.equals(newUser),
          "Cannot use multiple kerberos principals in the same agent. "
              + " Must restart agent to use new principal or keytab. " + "Previous = %s, New = %s",
          prevUser, newUser);

      // attempt to use cached credential if the user is the same
      // this is polite and should avoid flooding the KDC with auth
      // requests
      UserGroupInformation curUser = null;
      if (prevUser != null && prevUser.equals(newUser)) {
        try {
          curUser = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
          LOG.warn("User unexpectedly had no active login. Continuing with " + "authentication", e);
        }
      }

      if (curUser == null || !curUser.getUserName().equals(principal)) {
        try {
          // static login
          kerberosLogin(this, principal, kerbKeytab);
        } catch (IOException e) {
          LOG.error("Authentication or file read error while attempting to "
              + "login as kerberos principal (" + principal + ") using " + "keytab (" + kerbKeytab
              + "). Exception follows.", e);
          return false;
        }
      } else {
        LOG.debug("{}: Using existing principal login: {}", this, curUser);
      }

      // we supposedly got through this unscathed... so store the static
      // user
      staticLogin.set(newUser);
    }

    // hadoop impersonation works with or without kerberos security
    proxyTicket = null;
    if (!proxyUserName.isEmpty()) {
      try {
        proxyTicket = UserGroupInformation.createProxyUser(proxyUserName,
            UserGroupInformation.getLoginUser());
      } catch (IOException e) {
        LOG.error("Unable to login as proxy user. Exception follows.", e);
        return false;
      }
    }

    UserGroupInformation ugi = null;
    if (proxyTicket != null) {
      ugi = proxyTicket;
    } else if (useSecurity) {
      try {
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        LOG.error("Unexpected error: Unable to get authenticated user after "
            + "apparent successful login! Exception follows.", e);
        return false;
      }
    }

    if (ugi != null) {
      // dump login information
      AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
      LOG.info("Auth method: {}", authMethod);
      LOG.info(" User name: {}", ugi.getUserName());
      LOG.info(" Using keytab: {}", ugi.isFromKeytab());
      if (authMethod == AuthenticationMethod.PROXY) {
        UserGroupInformation superUser;
        try {
          superUser = UserGroupInformation.getLoginUser();
          LOG.info(" Superuser auth: {}", superUser.getAuthenticationMethod());
          LOG.info(" Superuser name: {}", superUser.getUserName());
          LOG.info(" Superuser using keytab: {}", superUser.isFromKeytab());
        } catch (IOException e) {
          LOG.error("Unexpected error: unknown superuser impersonating proxy.", e);
          return false;
        }
      }

      LOG.info("Logged in as user {}", ugi.getUserName());

      return true;
    }

    return true;
  }

  /**
   * Static synchronized method for static Kerberos login. <br/>
   * Static synchronized due to a thundering herd problem when multiple Sinks attempt to log in
   * using the same principal at the same time with the intention of impersonating different users
   * (or even the same user). If this is not controlled, MIT Kerberos v5 believes it is seeing a
   * replay attach and it returns: <blockquote>Request is a replay (34) - PROCESS_TGS</blockquote>
   * In addition, since the underlying Hadoop APIs we are using for impersonation are static, we
   * define this method as static as well.
   *
   * @param principal Fully-qualified principal to use for authentication.
   * @param keytab Location of keytab file containing credentials for principal.
   * @return Logged-in user
   * @throws IOException if login fails.
   */
  private static synchronized UserGroupInformation kerberosLogin(HDFSEventSink sink,
      String principal, String keytab) throws IOException {

    // if we are the 2nd user thru the lock, the login should already be
    // available statically if login was successful
    UserGroupInformation curUser = null;
    try {
      curUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      // not a big deal but this shouldn't typically happen because it
      // will
      // generally fall back to the UNIX user
      LOG.debug("Unable to get login user before Kerberos auth attempt.", e);
    }

    // we already have logged in successfully
    if (curUser != null && curUser.getUserName().equals(principal)) {
      LOG.debug("{}: Using existing principal ({}): {}", new Object[] {sink, principal, curUser});

      // no principal found
    } else {

      LOG.info("{}: Attempting kerberos login as principal ({}) from keytab " + "file ({})",
          new Object[] {sink, principal, keytab});

      // attempt static kerberos login
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      curUser = UserGroupInformation.getLoginUser();
    }

    return curUser;
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() + " }";
  }

  @VisibleForTesting
  void setBucketClock(Clock clock) {
    BucketPath.setClock(clock);
  }

  @VisibleForTesting
  void setMockFs(FileSystem mockFs) {
    this.mockFs = mockFs;
  }

  @VisibleForTesting
  void setMockWriter(HDFSWriter writer) {
    this.mockWriter = writer;
  }

  @VisibleForTesting
  int getTryCount() {
    return tryCount;
  }

  /*
   * Extended Java LinkedHashMap for open file handle LRU queue. We want to clear the oldest file
   * handle if there are too many open ones.
   */
  private static class WriterLinkedHashMap extends LinkedHashMap<String, BucketWriter> {
    private static final long serialVersionUID = 4456535248932876110L;
    private final int maxOpenFiles;

    public WriterLinkedHashMap(int maxOpenFiles) {
      super(16, 0.75f, true); // stock initial capacity/load, access
      // ordering
      this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
      if (size() > maxOpenFiles) {
        // If we have more that max open files, then close the last one
        // and
        // return true
        try {
          eldest.getValue().close();
        } catch (IOException e) {
          LOG.warn(eldest.getKey().toString(), e);
        } catch (InterruptedException e) {
          LOG.warn(eldest.getKey().toString(), e);
          Thread.currentThread().interrupt();
        }
        return true;
      } else {
        return false;
      }
    }
  }
}
