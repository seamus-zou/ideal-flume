package com.ideal.flume.client.avro;

import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_DELETE_POLICY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SPOOLED_FILE_SUFFIX;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.ideal.flume.cache.CacheClient;
import com.ideal.flume.clients.ClientProps;
import com.ideal.flume.clients.CollectClient;
import com.ideal.flume.clients.CollectClientFactory;
import com.ideal.flume.enums.ConsumeOrder;
import com.ideal.flume.enums.DeletePolicy;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.serialization.BufferedBytesDeserializer;
import com.ideal.flume.serialization.MockResettableBufferedInputStream;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.tools.DateTimeUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YeeFileCarryReader {
  private final static Logger logger = LoggerFactory.getLogger(YeeFileCarryReader.class);

  private final CollectClient readClient;
  private final CollectClient listClient;
  private final CollectClient checkClient;

  private final String remoteUrl;
  private CacheClient cacheClient;

  private final String baseDir;
  private String currentDayDir; // 每天一个目录
  private int currentHourDir; // 天目录下，每小时一个目录
  private boolean needSwitchHour;

  private String spoolDirectory;
  private final String completedSuffix;
  private final String deletePolicy;
  private final ConsumeOrder consumeOrder;
  private final int bufSize = BufferedBytesDeserializer.BUFFER_SIZE;
  private final String prefix;
  private final String suffix;
  private final int channelCount;
  private final String hostname;
  // 文件日期字符串
  private String fileDateStr;
  // 动态目录命名规则，同DateFormat
  private String dynamicDirReg;
  // 文件日期格式
  private String fileDateFormat;
  private final boolean isRepair;

  private Optional<FileInfo> currentFile = Optional.absent();
  /**
   * Always contains the last file from which lines have been read. *
   */
  private Optional<FileInfo> lastFileRead = Optional.absent();
  private boolean committed = true;

  private int eventsReads; // 当前文件已读的次数，每次固定读8192字节
  private final static Map<String, AtomicInteger> failFileRetryMap =
      new ConcurrentHashMap<String, AtomicInteger>(10); // 失败文件重试次数记录
  private static AtomicInteger readerIndexTmp = new AtomicInteger(); // reader的序号生成器
  private final int readerIndexInt; // 当前reader的序号
  private final String readerIndex;

  private final static Map<String, LinkedBlockingQueue<CollectFile>> collectFilesMap =
      new ConcurrentHashMap<String, LinkedBlockingQueue<CollectFile>>();
  private final static Map<String, Boolean> listingFiles = new ConcurrentHashMap<String, Boolean>();
  private final AtomicBoolean doend = new AtomicBoolean(false);
  private long doendTmp;

  private final static Map<String, Counters.Counter> unreadFilesCounterMap =
      new ConcurrentHashMap<String, Counters.Counter>(10);
  private final static Map<String, Counters.Counter> unreadFileSizeCounterMap =
      new ConcurrentHashMap<String, Counters.Counter>(10);

  private final static Map<String, Integer> chkFileLines = Maps.newConcurrentMap();

  private boolean interrupted = false;

  private YeeFileCarryReader(ClientProps clientProps, CacheClient cacheClient,
      String spoolDirectory, String completedSuffix, String deletePolicy, ConsumeOrder consumeOrder,
      String prefix, String suffix, int channelCount, String hostname, String baseDir,
      String currentDayDir, int currentHourDir, boolean needSwitchHour, String fileDateStr,
      String dynamicDirReg, String fileDateFormat, boolean isRepair

  ) throws Exception {
    this.fileDateStr = fileDateStr;
    this.dynamicDirReg = dynamicDirReg;
    this.fileDateFormat = fileDateFormat;
    // Sanity checks
    Preconditions.checkNotNull(clientProps);
    Preconditions.checkNotNull(cacheClient);
    Preconditions.checkNotNull(spoolDirectory);
    Preconditions.checkNotNull(completedSuffix);
    Preconditions.checkNotNull(deletePolicy);
    Preconditions.checkNotNull(prefix);
    Preconditions.checkNotNull(suffix);

    // validate delete policy
    if (!deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())
        && !deletePolicy.equalsIgnoreCase(DeletePolicy.CACHE.name())
        && !deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
      throw new IllegalArgumentException(
          "Delete policies other than NEVER and IMMEDIATE are not yet supported");
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}",
          new Object[] {YeeFileCarryReader.class.getSimpleName(), spoolDirectory});
    }

    this.cacheClient = cacheClient;
    this.spoolDirectory = spoolDirectory.endsWith("/") ? spoolDirectory : spoolDirectory + "/";
    this.completedSuffix = completedSuffix;
    this.deletePolicy = deletePolicy;
    this.consumeOrder = Preconditions.checkNotNull(consumeOrder);
    this.prefix = prefix;
    this.suffix = suffix;
    this.channelCount = channelCount;
    this.hostname = hostname;
    this.isRepair = isRepair;

    if (isRepair) {
      // 补数据或重采，清除在读缓存
      cacheClient.cleanReadingCache(-1);
    }

    this.baseDir = baseDir.endsWith("/") ? baseDir : baseDir + "/";
    this.currentDayDir = currentDayDir;
    this.currentHourDir = currentHourDir;
    this.needSwitchHour = needSwitchHour;

    this.readerIndexInt = readerIndexTmp.getAndIncrement();
    this.readerIndex = "" + readerIndexInt;
    this.remoteUrl = StringUtils.trimToEmpty(clientProps.getIp())
        + StringUtils.trimToEmpty(clientProps.getWorkingDirectory());
    this.readClient = CollectClientFactory.createClient(clientProps);

    if (needSwitchHour) {
      this.checkClient = CollectClientFactory.createClient(clientProps);

      while (this.currentHourDir < 23) {
        if (!checkClient.exists(this.spoolDirectory)) {
          switchNextHour();
        } else {
          break;
        }
      }
    } else {
      this.checkClient = null;
    }

    synchronized (collectFilesMap) {
      if (!collectFilesMap.containsKey(remoteUrl)) {
        collectFilesMap.put(remoteUrl, new LinkedBlockingQueue<CollectFile>());
      }
    }
    synchronized (unreadFilesCounterMap) {
      if (!unreadFilesCounterMap.containsKey(remoteUrl)) {
        unreadFilesCounterMap.put(remoteUrl,
            Counters.create("reader", remoteUrl + "-unreadFiles", 1));
      }
    }
    synchronized (unreadFileSizeCounterMap) {
      if (!unreadFileSizeCounterMap.containsKey(remoteUrl)) {
        unreadFileSizeCounterMap.put(remoteUrl,
            Counters.create("reader", remoteUrl + "-unreadFileSize", 1));
      }
    }

    synchronized (listingFiles) {
      if (null == listingFiles.get(remoteUrl) || !listingFiles.get(remoteUrl)) {
        listingFiles.put(remoteUrl, true);
        this.listClient = CollectClientFactory.createClient(clientProps);
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            while (!interrupted) {
              try {
                listFiles();

                Thread.sleep(60000);
              } catch (Throwable t) {
                logger.error("", t);
              }
            }
          }
        }, "list file-" + remoteUrl);
        t.start();
      } else {
        this.listClient = null;
      }
    }
  }

  private void listFiles() {
    LinkedBlockingQueue<CollectFile> remoteFiles = collectFilesMap.get(remoteUrl);
    if (remoteFiles.size() == 0) {
      if (isRepair && null != getLastFileRead()) {
        // 补数据或者重采，目录为空后停止
        stop();
        return;
      }

      try {
        logger.info("list files...{}{}", spoolDirectory, needSwitchHour ? currentHourDir + "" : "");
        // 如果有check文件，获取所有的check文件
        List<CollectFile> checkFiles =
            listClient.listFiles(spoolDirectory, new CollectFileFilter() {
              @Override
              public boolean accept(CollectFile file) {
                return file.getName().endsWith(".chk");
              }
            });
        if (CollectionUtils.isNotEmpty(checkFiles)) {
          getCheckLines(checkFiles);
        }

        /* Filter to exclude finished or hidden files */
        CollectFileFilter filter = new CollectFileFilter() {
          public boolean accept(CollectFile candidate) {
            logger.debug("start accept file.");
            long start = System.currentTimeMillis();
            String fileName = candidate.getName();
            String absFileName = spoolDirectory + fileName;
            if (candidate.isDirectory() || fileName.endsWith(completedSuffix)
                || fileName.startsWith(".") || !fileName.endsWith(suffix)
                || !fileName.startsWith(prefix) ||
            // 修改1分钟之外才会被处理
            (System.currentTimeMillis() - candidate.getTimeInMillis() < 60000) ||
            // 已经被读过的文件
            (isReaded(absFileName)) ||
            // 正在被处理的files
            (isReading(absFileName)) ||
            // 已确认失败的文件
            isFailed(absFileName) ||
            // 文件名日期字符串不对的文件
            isFileNotHaveDateStr(fileName)) {
              long end = System.currentTimeMillis();
              logger.debug("end accept file. use time = " + (end - start) + "ms");
              return false;
            }

            return true;
          }
        };

        logger.info("listClient..." + listClient + ":" + listClient.isConnected());

        List<CollectFile> files = listClient.listFiles(spoolDirectory, filter);
        if (null == files || files.size() == 0) {
          unreadFilesCounterMap.get(remoteUrl).reset();
          unreadFileSizeCounterMap.get(remoteUrl).reset();

          logger.info("list remote files done. EMPTY.");
          return;
        }

        if (consumeOrder == ConsumeOrder.RANDOM) {
          // do nothing
        } else if (consumeOrder == ConsumeOrder.YOUNGEST) {
          Collections.sort(files, new Comparator<CollectFile>() {
            @Override
            public int compare(CollectFile f1, CollectFile f2) {
              try {
                return (int) (f2.getTimeInMillis() - f1.getTimeInMillis());
              } catch (Exception e) {
                return 0;
              }
            }
          });
        } else {
          Collections.sort(files, new Comparator<CollectFile>() {
            @Override
            public int compare(CollectFile f1, CollectFile f2) {
              try {
                return (int) (f1.getTimeInMillis() - f2.getTimeInMillis());
              } catch (Exception e) {
                return 0;
              }
            }
          });
        }

        doend.set(false);
        unreadFilesCounterMap.get(remoteUrl).reset();
        unreadFileSizeCounterMap.get(remoteUrl).reset();
        Set<String> listNames = new HashSet<String>();
        for (CollectFile f : files) {
          listNames.add(f.getName());
          remoteFiles.put(f);

          unreadFilesCounterMap.get(remoteUrl).incr();
          unreadFileSizeCounterMap.get(remoteUrl).add(f.getSize());
        }

        // 有些文件失败可能是服务器上已经把文件删除了，把这些文件从map里清掉
        if (failFileRetryMap.size() > 0) {
          Set<String> failedNames = failFileRetryMap.keySet();
          for (String s : failedNames) {
            if (!listNames.contains(s)) {
              failFileRetryMap.remove(s);
            }
          }
        }

        listNames.clear();
        listNames = null;

        logger.info("list remote files done. 1");
      } catch (Throwable e) {
        logger.error("", e);
      }
    }
  }

  /**
   * 获取校验文件里的行数
   *
   * @param checkFiles 校验文件列表
   */
  private void getCheckLines(List<CollectFile> checkFiles) {
    for (CollectFile file : checkFiles) {
      String dataFileName = file.getName().substring(0, file.getName().length() - 4);
      if (isReaded(dataFileName) || isFailed(dataFileName)) {
        continue;
      }

      BufferedReader reader = null;
      try {
        reader = new BufferedReader(
            new InputStreamReader(listClient.retrieveFileStream(spoolDirectory + file.getName())));

        chkFileLines.put(dataFileName, Integer.parseInt(reader.readLine()));
      } catch (Exception e) {
        logger.error("", e);
      } finally {
        if (null != reader) {
          try {
            reader.close();
          } catch (IOException e) {
            logger.error("", e);
          }
        }

        try {
          listClient.completePendingCommand();
        } catch (Exception e) {
          logger.error("", e);
        }
      }
    }
  }

  private boolean isReaded(String fileName) {
    return !isRepair && cacheClient.isReaded(fileName);
  }

  private boolean isReading(String fileName) {
    return !isRepair && cacheClient.isReading(fileName);
  }

  private boolean isFileNotHaveDateStr(String fileName) {
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(fileDateStr)) {
      if (fileName.indexOf(fileDateStr) < 0) {
        return true;
      }
    }
    return false;
  }

  private boolean isFailed(String fileName) {
    return !isRepair && cacheClient.isFailed(fileName);
  }

  private void cacheReaded(String fileName) throws Exception {
    cacheClient.cacheReaded(fileName);
  }

  private boolean cacheReading(String fileName) throws Exception {
    return cacheClient.cacheReading(fileName);
  }

  private void cacheReadedLine(String fileName, int lines) {
    cacheClient.cacheReadedLines(fileName, lines);
  }

  private int getReadLine(String fileName) {
    return cacheClient.getFileReadedLines(fileName);
  }

  private void removeReading(String fileName) {
    cacheClient.removeReading(fileName);
  }

  private void cacheFailFile(String fileName) throws Exception {
    cacheClient.cacheFailed(fileName);
  }

  public boolean fileend() throws IOException {
    if (doend.get()) {
      // return (System.currentTimeMillis() - doendTmp > 300000);
      return (System.currentTimeMillis() - doendTmp > 120000);
    } else {
      if (collectFilesMap.get(remoteUrl).size() == 0) {
        doendTmp = System.currentTimeMillis();
        doend.set(true);
      }
      return false;
    }
  }

  public void stop() {
    interrupted = true;
  }

  public void setSpoolDirectory(String dayDir) {
    doend.set(false);
    this.currentDayDir = dayDir;
    this.currentHourDir = 0;
    this.spoolDirectory = baseDir + dayDir + (needSwitchHour ? "/00/" : "/");
  }

  public void switchNextHour() throws Exception {
    int tmpHour = currentHourDir;
    while (tmpHour < 23) {
      tmpHour++;
      String path = baseDir + currentDayDir + "/" + (tmpHour < 10 ? "0" + tmpHour : tmpHour) + "/";
      if (checkClient.exists(path)) {
        doend.set(false);
        this.spoolDirectory = path;
        currentHourDir = tmpHour;
        logger.info("switch: {} {}", tmpHour, readerIndex);
        return;
      }
    }
  }

  public void setFileDateStr(String fileDateStr1) {
    this.fileDateStr = fileDateStr1;
  }


  public String getSpoolDirectory() {
    return spoolDirectory;
  }

  public String getCurrentDay() {
    return currentDayDir;
  }

  public int getCurrentHour() {
    return currentHourDir;
  }

  private static class FileInfo {
    private final CollectFile file;
    private final long length;
    private final long lastModified;
    private final EventDeserializer deserializer;

    public FileInfo(CollectFile file, EventDeserializer deserializer) {
      this.file = file;
      this.length = file.getSize();
      this.lastModified = file.getTimeInMillis();
      this.deserializer = deserializer;
    }

    public long getLength() {
      return length;
    }

    public long getLastModified() {
      return lastModified;
    }

    public EventDeserializer getDeserializer() {
      return deserializer;
    }

    public CollectFile getFile() {
      return file;
    }
  }

  public static class Builder {
    private ClientProps clientProps;
    private CacheClient cacheClient;
    private String spoolDirectory;
    private String completedSuffix = SPOOLED_FILE_SUFFIX;
    private String deletePolicy = DEFAULT_DELETE_POLICY;
    private ConsumeOrder consumeOrder = DEFAULT_CONSUME_ORDER;
    private int channelCount;
    private String hostname;

    private String prefix;
    private String suffix;

    private String baseDir;
    private String currentDayDir;
    private int currentHourDir;
    private boolean needSwitchHour;
    private String fileDateStr;
    // 动态目录命名规则，同DateFormat
    private String dynamicDirReg;
    // 文件日期格式
    private String fileDateFormat;
    private boolean isRepair;

    public Builder spoolDirectory(String directory) {
      this.spoolDirectory = directory;
      return this;
    }

    public Builder clientProps(ClientProps clientProps) {
      this.clientProps = clientProps;
      return this;
    }

    public Builder cacheClient(CacheClient cacheClient) {
      this.cacheClient = cacheClient;
      return this;
    }

    public Builder completedSuffix(String completedSuffix) {
      this.completedSuffix = completedSuffix;
      return this;
    }

    public Builder deletePolicy(String deletePolicy) {
      this.deletePolicy = deletePolicy;
      return this;
    }

    public Builder consumeOrder(ConsumeOrder consumeOrder) {
      this.consumeOrder = consumeOrder;
      return this;
    }

    public Builder prefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder suffix(String suffix) {
      this.suffix = suffix;
      return this;
    }

    public Builder channelCount(int channelCount) {
      this.channelCount = channelCount;
      return this;
    }

    public Builder hostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder baseDir(String baseDir) {
      this.baseDir = baseDir;
      return this;
    }

    public Builder currentDayDir(String currentDayDir) {
      this.currentDayDir = currentDayDir;
      return this;
    }

    public Builder currentHourDir(int currentHourDir) {
      this.currentHourDir = currentHourDir;
      return this;
    }

    public Builder needSwitchHour(boolean needSwitchHour) {
      this.needSwitchHour = needSwitchHour;
      return this;
    }

    public Builder fileDateStr(String fileDateStr) {
      this.fileDateStr = fileDateStr;
      return this;
    }

    public Builder dynamicDirReg(String dynamicDirReg) {
      this.dynamicDirReg = dynamicDirReg;
      return this;
    }

    public Builder fileDateFormat(String fileDateFormat) {
      this.fileDateFormat = fileDateFormat;
      return this;
    }

    public Builder isRepair(boolean isRepair) {
      this.isRepair = isRepair;
      return this;
    }

    public YeeFileCarryReader build() throws Exception {
      return new YeeFileCarryReader(clientProps, cacheClient, spoolDirectory, completedSuffix,
          deletePolicy, consumeOrder, prefix, suffix, channelCount, hostname, baseDir,
          currentDayDir, currentHourDir, needSwitchHour, fileDateStr, dynamicDirReg, fileDateFormat,
          isRepair);
    }
  }

  public String getLastFileRead() {
    if (!lastFileRead.isPresent()) {
      return null;
    }
    return lastFileRead.get().getFile().getName();
  }

  public Event readEvent() throws Exception {
    List<Event> events = readEvents(1);
    if (!events.isEmpty()) {
      return events.get(0);
    } else {
      return null;
    }
  }

  public List<Event> readEvents(int numEvents) throws Exception {
    if (!committed) {
      if (!currentFile.isPresent()) {
        throw new IllegalStateException("File should not roll when commit is outstanding.");
      }
      logger.info("Last read was never committed - resetting mark position.");
      currentFile.get().getDeserializer().reset();

      // 读取过文件但没完成，跳过已读的行
      int redisReadLine = getReadLine(spoolDirectory + currentFile.get().getFile().getName());
      if (redisReadLine > 0) {
        eventsReads = redisReadLine;
        EventDeserializer des = currentFile.get().getDeserializer();
        des.readEvents(redisReadLine);
      }
    } else {
      // Check if new files have arrived since last call
      if (!currentFile.isPresent()) {
        currentFile = getNextFile();

        if (currentFile.isPresent()) {
          // 读取过文件但没完成，跳过已读的行
          int redisReadLine = getReadLine(spoolDirectory + currentFile.get().getFile().getName());
          if (redisReadLine > 0) {
            eventsReads = redisReadLine;
            EventDeserializer des = currentFile.get().getDeserializer();
            des.readEvents(redisReadLine);
          }
        }
      }

      // Return empty list if no new files
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
    }

    EventDeserializer des = currentFile.get().getDeserializer();
    List<Event> events = Collections.emptyList();
    try {
      events = des.readEvents(numEvents);
    } catch (Throwable t) {
      retireCurrentFile();
      currentFile = getNextFile();

      logger.info("error file: " + currentFile.get().getFile().getName());
      return Collections.emptyList();
    }

    /*
     * It's possible that the last read took us just up to a file boundary. If so, try to roll to
     * the next file, if there is one.
     */
    if (events.isEmpty()) {
      retireCurrentFile();
      currentFile = getNextFile();
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
      events = currentFile.get().getDeserializer().readEvents(numEvents);
    }

    committed = false;
    lastFileRead = currentFile;
    eventsReads += events.size();

    List<Event> ret = new ArrayList<Event>();
    Map<String, String> headers = new HashMap<String, String>(4);
    headers.put("%{hostname}", hostname);
    headers.put("way", "c" + ((int) (Math.random() * channelCount)));
    headers.put(DEFAULT_FILENAME_HEADER_KEY, getCurrentReadingFile());
    headers.put(DEFAULT_BASENAME_HEADER_KEY, currentFile.get().getFile().getAbsolutePath());
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(currentDayDir)) {
      if (needSwitchHour) {
        headers.put("timestamp", DateTimeUtils
            .parseDate(currentDayDir + " " + currentHourDir, dynamicDirReg + " HH").getTime() + "");
      } else {
        headers.put("timestamp",
            DateTimeUtils.parseDate(currentDayDir, dynamicDirReg).getTime() + "");
      }
    }
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(fileDateStr)) {
      headers.put("timestamp",
          (DateTimeUtils.parseDate(fileDateStr, fileDateFormat)).getTime() + "");
    }

    return ret;
  }

  public void close() throws Exception {
    if (currentFile.isPresent()) {
      closeDeserializer(currentFile.get().getDeserializer());
      currentFile = Optional.absent();
    }
  }

  private void closeDeserializer(EventDeserializer deserializer) throws Exception {
    deserializer.close();
    readClient.completePendingCommand();
  }

  public void commit() throws Exception {
    if (!committed && currentFile.isPresent()) {
      currentFile.get().getDeserializer().mark();
      committed = true;

      cacheReadedLine(spoolDirectory + currentFile.get().getFile().getName(), eventsReads);
    }
  }

  private Optional<FileInfo> getNextFile() throws Exception {
    LinkedBlockingQueue<CollectFile> collectFiles = collectFilesMap.get(remoteUrl);
    if (null == collectFiles || collectFiles.size() == 0) {
      // No matching file in spooling directory.
      return Optional.absent();
    }

    CollectFile f = collectFiles.poll();
    if (null == f) {
      return Optional.absent();
    }
    String absFileName = spoolDirectory + f.getName();

    while (isReaded(absFileName) || isReading(absFileName) || isFailed(absFileName)) {
      unreadFilesCounterMap.get(remoteUrl).decr();
      unreadFileSizeCounterMap.get(remoteUrl).add(-f.getSize());

      f = collectFiles.poll();

      if (null == f) {
        break;
      }
    }

    if (null == f) {
      return Optional.absent();
    }

    while (!cacheReading(absFileName)) {
      unreadFilesCounterMap.get(remoteUrl).decr();
      unreadFileSizeCounterMap.get(remoteUrl).add(-f.getSize());

      f = collectFiles.poll();

      if (null == f) {
        break;
      }
    }

    if (null == f) {
      return Optional.absent();
    }

    return openFile(f);
  }

  private Optional<FileInfo> openFile(CollectFile file) {
    eventsReads = 0;

    if (logger.isDebugEnabled()) {
      logger.debug("reader {} open file {}", readerIndex, file.getName());
    }

    long fileSize = 0L;
    try {
      fileSize = file.getSize();
      unreadFilesCounterMap.get(remoteUrl).decr();
      unreadFileSizeCounterMap.get(remoteUrl).add(-fileSize);

      InputStream fileIn = new BufferedInputStream(
          readClient.retrieveFileStream(spoolDirectory + file.getName()), bufSize);
      ResettableInputStream in = new MockResettableBufferedInputStream(fileIn, bufSize);

      EventDeserializer deserializer = new BufferedBytesDeserializer.Builder().build(null, in);

      failFileRetryMap.remove(file.getName());
      return Optional.of(new FileInfo(file, deserializer));
    } catch (Exception e) {
      try {
        readClient.completePendingCommand();
      } catch (Exception e2) {
        logger.error("", e2);
      }

      if (e instanceof NullPointerException) {
        logger.error("error get inputstream: " + file.getName());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          logger.error("", e);
        }
      } else {
        logger.error("Exception opening file: " + file, e);
      }

      AtomicInteger fc = failFileRetryMap.get(file.getName());
      if (null == fc) {
        fc = new AtomicInteger(1);
        failFileRetryMap.put(file.getName(), fc);
      } else {
        fc.incrementAndGet();
      }

      if (fc.get() >= 5) {
        // 重试超过5次，认定失败
        try {
          cacheFailFile(file.getAbsolutePath());
        } catch (Exception e1) {
          logger.error("", e1);
        }
        failFileRetryMap.remove(file.getName());
      }

      removeReading(spoolDirectory + file.getName());
      return Optional.absent();
    }
  }

  private String getCurrentReadingFile() {
    if (!currentFile.isPresent()) {
      return "";
    }
    return currentFile.get().getFile().getName();
  }

  private void retireCurrentFile() throws Exception {
    Preconditions.checkState(currentFile.isPresent());

    CollectFile fileToRoll = currentFile.get().getFile();

    checkLines(fileToRoll);

    closeDeserializer(currentFile.get().getDeserializer());

    // Verify that spooling assumptions hold
    if (fileToRoll.getTimeInMillis() != currentFile.get().getLastModified()) {
      String message = "File has been modified since being read: " + fileToRoll;
      throw new IllegalStateException(message);
    }
    if (fileToRoll.getSize() != currentFile.get().getLength()) {
      String message = "File has changed size since being read: " + fileToRoll;
      throw new IllegalStateException(message);
    }

    if (deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())) {
      rollCurrentFile(fileToRoll);
    } else if (deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
      deleteCurrentFile(fileToRoll);
    } else if (deletePolicy.equalsIgnoreCase(DeletePolicy.CACHE.name())) {
      writeCurrentFile(fileToRoll);
    } else {
      // TODO: implement delay in the future
      throw new IllegalArgumentException("Unsupported delete policy: " + deletePolicy);
    }
  }

  private void checkLines(CollectFile fileToRoll) {
    Integer lines = chkFileLines.get(fileToRoll.getName());
    if (null != lines && lines != eventsReads) {
      // TODO 往外发出告警，读到的行数和校验文件内的行数不一致
    }
  }

  private void rollCurrentFile(CollectFile fileToRoll) throws Exception {
    String from = spoolDirectory + fileToRoll.getName();
    String dest = from + completedSuffix;
    logger.info("Preparing to move file {} to {}", from, dest);
    if (readClient.rename(from, dest)) {
      logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);
    } else {
      /*
       * If we are here then the file cannot be renamed for a reason other than that the destination
       * file exists (actually, that remains possible w/ small probability due to TOC-TOU
       * conditions).
       */
      String message = "Unable to move " + fileToRoll + " to " + dest
          + ". This will likely cause duplicate events. Please verify that "
          + "flume has sufficient permissions to perform these operations.";
      throw new FlumeException(message);
    }
    readClient.completePendingCommand();
  }

  private void deleteCurrentFile(CollectFile fileToDelete) throws Exception {
    logger.info("Preparing to delete file {}", fileToDelete);
    readClient.deleteFile(spoolDirectory + fileToDelete.getName());
  }

  private void writeCurrentFile(CollectFile file) throws Exception {
    String path = file.getName();
    String absPath = spoolDirectory + path;
    cacheReaded(absPath);
    removeReading(absPath);

    if (logger.isDebugEnabled()) {
      logger.debug("reader {} finish file {}", readerIndex, path);
    }
  }

}
