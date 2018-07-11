package com.ideal.flume.client.avro;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ideal.flume.cache.CacheClient;
import com.ideal.flume.clients.ClientProps;
import com.ideal.flume.clients.CollectClient;
import com.ideal.flume.clients.CollectClientFactory;
import com.ideal.flume.enums.ConsumeOrder;
import com.ideal.flume.enums.DataFileType;
import com.ideal.flume.enums.DeletePolicy;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.serialization.MockResettableInputStream;
import com.ideal.flume.stat.Counters;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.*;

public class YeeFileEventReader4 {
    private final static Logger logger = LoggerFactory.getLogger(YeeFileEventReader4.class);

    private final CollectClient readClient;
    private final CollectClient listClient;

    private final String remoteUrl;
    private CacheClient cacheClient;
    private DataFileType dataFileType;

    private String spoolDirectory;
    private final String completedSuffix;
    private final String deserializerType;
    private final Context deserializerContext;
    private final String deletePolicy;
    private final Charset inputCharset;
    private final ConsumeOrder consumeOrder;
    private final int bufSize;
    private final String prefix;
    private final String suffix;
    private final int channelCount;
    private final int sendThreadCount;
    private final String hostname;
    private final int listDays;
    private final boolean isRepair;

    private final boolean dynamicYear;
    private final boolean dynamicMonth;
    private final boolean dynamicDay;
    private final boolean dynamicHour;

    private final static String DYNAMIC_YEAR_STR = "%Y";
    private final static String DYNAMIC_MONTH_STR = "%m";
    private final static String DYNAMIC_DAY_STR = "%d";
    private final static String DYNAMIC_HOUR_STR = "%H";

    private final static String[] HOURS =
            new String[] {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
                    "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"};

    private Optional<FileInfo> currentFile = Optional.absent();
    /**
     * Always contains the last file from which lines have been read. *
     */
    private Optional<FileInfo> lastFileRead = Optional.absent();
    private boolean committed = true;

    private int eventsReads; // 当前文件已读的记录数
    private final static Map<String, AtomicInteger> failFileRetryMap =
            new ConcurrentHashMap<String, AtomicInteger>(10); // 失败文件重试次数记录
    private static AtomicInteger readerIndexTmp = new AtomicInteger(); // reader的序号生成器
    private final int readerIndexInt; // 当前reader的序号
    private final String readerIndex;

    private final static Map<String, LinkedBlockingQueue<CollectFile>> collectFilesMap =
            new ConcurrentHashMap<String, LinkedBlockingQueue<CollectFile>>();
    private final static Map<String, Boolean> listingFiles =
            new ConcurrentHashMap<String, Boolean>();

    private final static Map<String, Counters.Counter> unreadFilesCounterMap =
            new ConcurrentHashMap<String, Counters.Counter>(10);
    private final static Map<String, Counters.Counter> unreadFileSizeCounterMap =
            new ConcurrentHashMap<String, Counters.Counter>(10);

    private final static Map<String, Integer> chkFileLines = Maps.newConcurrentMap();

    private boolean interrupted = false;

    private YeeFileEventReader4(ClientProps clientProps, CacheClient cacheClient,
            String spoolDirectory, String completedSuffix, String deserializerType,
            Context deserializerContext, String deletePolicy, String inputCharset,
            ConsumeOrder consumeOrder, int bufSize, String prefix, String suffix, int channelCount,
            int sendThreadCount, DataFileType dataFileType, String hostname, boolean isRepair,
            final long listInterval, int listDays) throws Exception {
        // Sanity checks
        Preconditions.checkNotNull(clientProps);
        Preconditions.checkNotNull(cacheClient);
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(deletePolicy);
        Preconditions.checkNotNull(inputCharset);
        Preconditions.checkNotNull(prefix);
        Preconditions.checkNotNull(suffix);
        Preconditions.checkNotNull(dataFileType);

        // validate delete policy
        if (!deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())
                && !deletePolicy.equalsIgnoreCase(DeletePolicy.CACHE.name())
                && !deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
            throw new IllegalArgumentException(
                    "Delete policies other than NEVER and IMMEDIATE are not yet supported");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, deserializer={}", new Object[] {
                    YeeFileEventReader4.class.getSimpleName(), spoolDirectory, deserializerType});
        }

        this.cacheClient = cacheClient;
        this.spoolDirectory = spoolDirectory.endsWith("/") ? spoolDirectory : spoolDirectory + "/";
        this.dynamicYear = spoolDirectory.indexOf(DYNAMIC_YEAR_STR) >= 0;
        this.dynamicMonth = spoolDirectory.indexOf(DYNAMIC_MONTH_STR) >= 0;
        this.dynamicDay = spoolDirectory.indexOf(DYNAMIC_DAY_STR) >= 0;
        this.dynamicHour = spoolDirectory.indexOf(DYNAMIC_HOUR_STR) >= 0;
        this.completedSuffix = completedSuffix;
        this.deserializerType = deserializerType;
        this.deserializerContext = deserializerContext;
        this.deletePolicy = deletePolicy;
        this.inputCharset = Charset.forName(inputCharset);
        this.consumeOrder = Preconditions.checkNotNull(consumeOrder);
        this.bufSize = bufSize;
        this.prefix = prefix;
        this.suffix = suffix;
        this.channelCount = channelCount;
        this.sendThreadCount = sendThreadCount;
        this.dataFileType = dataFileType;
        this.hostname = hostname;
        this.isRepair = isRepair;
        this.listDays = listDays;

        if (isRepair) {
            // 补数据或重采，清除在读缓存
            cacheClient.cleanReadingCache(-1);
        }

        this.readerIndexInt = readerIndexTmp.getAndIncrement();
        this.readerIndex = "" + readerIndexInt;
        this.remoteUrl = StringUtils.trimToEmpty(clientProps.getIp())
                + StringUtils.trimToEmpty(clientProps.getWorkingDirectory());
        this.readClient = CollectClientFactory.createClient(clientProps);

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

                                Thread.sleep(listInterval);
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

    private void listCheckFiles(String path) throws Exception {
        // 如果有check文件，获取所有的check文件
        List<CollectFile> checkFiles = listClient.listFiles(path, new CollectFileFilter() {
            @Override
            public boolean accept(CollectFile file) {
                return file.getName().endsWith(".chk");
            }
        });
        if (CollectionUtils.isNotEmpty(checkFiles)) {
            getCheckLines(checkFiles);
        }
    }

    private void listFiles() {
        LinkedBlockingQueue<CollectFile> remoteFiles = collectFilesMap.get(remoteUrl);
        if (remoteFiles.size() == 0) {
            if (isRepair && null != getLastFileRead()) {
                // 补数据或者重采，目录为空后停止
                stop();
                logger.info("repair data finished.");
                return;
            }

            try {
                logger.info("list files...{}", spoolDirectory);

                /* Filter to exclude finished or hidden files */
                CollectFileFilter filter = new CollectFileFilter() {
                    public boolean accept(CollectFile candidate) {
                        logger.debug("start accept file.");
                        long start = System.currentTimeMillis();
                        String fileName = candidate.getName();
                        if (candidate.isDirectory() || fileName.endsWith(completedSuffix)
                                || fileName.startsWith(".") || !fileName.endsWith(suffix)
                                || !fileName.startsWith(prefix) ||
                        // 修改1分钟之外才会被处理
                        (System.currentTimeMillis() - candidate.getTimeInMillis() < 60000)) {
                            if (logger.isDebugEnabled()) {
                                long end = System.currentTimeMillis();
                                logger.debug("end accept file. use time = " + (end - start) + "ms");
                            }
                            return false;
                        }

                        return true;
                    }
                };

                logger.info("listClient..." + listClient + ":" + listClient.isConnected());

                List<CollectFile> files = Lists.newArrayList();
                if (dynamicYear || dynamicMonth || dynamicDay || dynamicHour) {
                    for (int i = listDays; i <= 0; i++) {
                        String path = spoolDirectory;
                        try {
                            Calendar ca = Calendar.getInstance();
                            ca.add(Calendar.DAY_OF_MONTH, i);

                            if (dynamicYear) {
                                path = StringUtils.replace(path, DYNAMIC_YEAR_STR,
                                        String.valueOf(ca.get(Calendar.YEAR)));
                            }
                            if (dynamicMonth) {
                                int month = ca.get(Calendar.MONTH) + 1;
                                path = StringUtils.replace(path, DYNAMIC_MONTH_STR,
                                        month < 10 ? "0" + month : "" + month);
                            }
                            if (dynamicDay) {
                                int day = ca.get(Calendar.DAY_OF_MONTH);
                                path = StringUtils.replace(path, DYNAMIC_DAY_STR,
                                        day < 10 ? "0" + day : "" + day);
                            }

                            if (dynamicHour) {
                                for (String hour : HOURS) {
                                    String tmpPath =
                                            StringUtils.replace(path, DYNAMIC_HOUR_STR, hour);
                                    listCheckFiles(tmpPath);
                                    files.addAll(listClient.listFiles(tmpPath, filter));
                                }
                            } else {
                                listCheckFiles(path);

                                files.addAll(listClient.listFiles(path, filter));
                            }
                        } catch (Exception e) {
                            logger.error("list file error: " + path, e);
                        }
                    }
                } else {
                    files.addAll(listClient.listFiles(spoolDirectory, filter));
                }

                if (null == files || files.size() == 0) {
                    unreadFilesCounterMap.get(remoteUrl).reset();
                    unreadFileSizeCounterMap.get(remoteUrl).reset();

                    logger.info("list remote files done. EMPTY.");
                    return;
                }

                Iterator<CollectFile> it = files.iterator();
                while (it.hasNext()) {
                    CollectFile file = it.next();
                    String absFileName = file.getAbsolutePath();
                    if (// 已经被读过的文件
                    isReaded(absFileName) ||
                    // 正在被处理的files
                            isReading(absFileName) ||
                            // 已确认失败的文件
                            isFailed(absFileName)) {
                        it.remove();
                    }
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

                unreadFilesCounterMap.get(remoteUrl).reset();
                unreadFileSizeCounterMap.get(remoteUrl).reset();
                Set<String> listNames = new HashSet<String>();
                for (CollectFile f : files) {
                    listNames.add(f.getAbsolutePath());
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
            String dataFileName =
                    file.getAbsolutePath().substring(0, file.getAbsolutePath().length() - 4);
            if (isReaded(dataFileName) || isFailed(dataFileName)) {
                continue;
            }

            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(
                        listClient.retrieveFileStream(file.getAbsolutePath())));

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


    public void stop() {
        interrupted = true;
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
        private String deserializerType = DEFAULT_DESERIALIZER;
        private Context deserializerContext = new Context();
        private String deletePolicy = DEFAULT_DELETE_POLICY;
        private String inputCharset = DEFAULT_INPUT_CHARSET;
        private ConsumeOrder consumeOrder = DEFAULT_CONSUME_ORDER;
        private int bufSize;
        private int channelCount;
        private int sendThreadCount = 4;
        private String hostname;

        private String prefix;
        private String suffix;
        private DataFileType dataFileType;

        private boolean isRepair;
        private long listInterval;
        private int listDays;

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

        public Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public Builder deletePolicy(String deletePolicy) {
            this.deletePolicy = deletePolicy;
            return this;
        }

        public Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public Builder consumeOrder(ConsumeOrder consumeOrder) {
            this.consumeOrder = consumeOrder;
            return this;
        }

        public Builder bufSize(int bufSize) {
            this.bufSize = bufSize;
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

        public Builder sendThreadCount(int sendThreadCount) {
            this.sendThreadCount = sendThreadCount;
            return this;
        }

        public Builder dataFileType(DataFileType dataFileType) {
            this.dataFileType = dataFileType;
            return this;
        }

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }


        public Builder isRepair(boolean isRepair) {
            this.isRepair = isRepair;
            return this;
        }

        public Builder listInterval(long listInterval) {
            this.listInterval = listInterval;
            return this;
        }

        public Builder listDays(int listDays) {
            this.listDays = listDays;
            return this;
        }

        public YeeFileEventReader4 build() throws Exception {
            return new YeeFileEventReader4(clientProps, cacheClient, spoolDirectory, completedSuffix,
                    deserializerType, deserializerContext, deletePolicy, inputCharset, consumeOrder,
                    bufSize, prefix, suffix, channelCount, sendThreadCount, dataFileType, hostname,
                    isRepair, listInterval, listDays);
        }
    }

    public String getLastFileRead() {
        if (!lastFileRead.isPresent()) {
            return null;
        }
        return lastFileRead.get().getFile().getAbsolutePath();
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
            int redisReadLine = getReadLine(currentFile.get().getFile().getAbsolutePath());
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
                    int redisReadLine = getReadLine(currentFile.get().getFile().getAbsolutePath());
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

            logger.info("error file: " + currentFile.get().getFile().getAbsolutePath());
            return Collections.emptyList();
        }

        /*
         * It's possible that the last read took us just up to a file boundary. If so, try to roll
         * to the next file, if there is one.
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
        headers.put("%{thread}", "" + ((int) (Math.random() * sendThreadCount)));
        headers.put("%{hostname}", hostname);
        headers.put("way", "c" + ((int) (Math.random() * channelCount)));
        headers.put(DEFAULT_FILENAME_HEADER_KEY, currentFile.get().getFile().getName());
        headers.put(DEFAULT_BASENAME_HEADER_KEY, currentFile.get().getFile().getAbsolutePath());

        for (Event event : events) {
            event.setHeaders(headers);
            ret.add(event);
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

            cacheReadedLine(currentFile.get().getFile().getAbsolutePath(), eventsReads);
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
        String absFileName = f.getAbsolutePath();

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
            logger.debug("reader {} open file {}", readerIndex, file.getAbsolutePath());
        }

        long fileSize = 0L;
        try {
            fileSize = file.getSize();
            unreadFilesCounterMap.get(remoteUrl).decr();
            unreadFileSizeCounterMap.get(remoteUrl).add(-fileSize);

            InputStream fileIn;
            if (DataFileType.GZ == dataFileType) {
                fileIn = new GZIPInputStream(readClient.retrieveFileStream(file.getAbsolutePath()),
                        bufSize);
            } else {
                fileIn = new BufferedInputStream(
                        readClient.retrieveFileStream(file.getAbsolutePath()), bufSize);
            }
            ResettableInputStream in = new MockResettableInputStream(fileIn, inputCharset.name());

            EventDeserializer deserializer =
                    EventDeserializerFactory.getInstance(deserializerType, deserializerContext, in);

            failFileRetryMap.remove(file.getAbsolutePath());
            return Optional.of(new FileInfo(file, deserializer));
        } catch (Exception e) {
            try {
                readClient.completePendingCommand();
            } catch (Exception e2) {
                logger.error("", e2);
            }

            if (e instanceof NullPointerException) {
                logger.error("error get inputstream: " + file.getAbsolutePath());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    logger.error("", e);
                }
            } else {
                logger.error("Exception opening file: " + file, e);
            }

            AtomicInteger fc = failFileRetryMap.get(file.getAbsolutePath());
            if (null == fc) {
                fc = new AtomicInteger(1);
                failFileRetryMap.put(file.getAbsolutePath(), fc);
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
                failFileRetryMap.remove(file.getAbsolutePath());
            }

            removeReading(file.getAbsolutePath());
            return Optional.absent();
        }
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
        Integer lines = chkFileLines.get(fileToRoll.getAbsolutePath());
        if (null != lines && lines != eventsReads) {
            // TODO 往外发出告警，读到的行数和校验文件内的行数不一致
        }
    }

    private void rollCurrentFile(CollectFile fileToRoll) throws Exception {
        String from = fileToRoll.getAbsolutePath();
        String dest = from + completedSuffix;
        logger.info("Preparing to move file {} to {}", from, dest);
        if (readClient.rename(from, dest)) {
            logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);
        } else {
            /*
             * If we are here then the file cannot be renamed for a reason other than that the
             * destination file exists (actually, that remains possible w/ small probability due to
             * TOC-TOU conditions).
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
        readClient.deleteFile(fileToDelete.getAbsolutePath());
    }

    private void writeCurrentFile(CollectFile file) throws Exception {
        String absPath = file.getAbsolutePath();
        cacheReaded(absPath);
        removeReading(absPath);

        if (logger.isDebugEnabled()) {
            logger.debug("reader {} finish file {}", readerIndex, absPath);
        }
    }

}
