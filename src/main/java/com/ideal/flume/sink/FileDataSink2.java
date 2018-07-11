package com.ideal.flume.sink;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ideal.flume.Constants;
import com.ideal.flume.clients.ClientProps;
import com.ideal.flume.clients.CollectClient;
import com.ideal.flume.clients.CollectClientFactory;
import com.ideal.flume.enums.ClientType;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.io.BucketWriter;
import com.ideal.flume.io.YeeFileWriter;
import com.ideal.flume.io.WriterCallback;
import com.ideal.flume.sink.hdfs.SimpleBucketPath;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.Counters.Counter;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.CompressionCodecUtils;
import com.ideal.flume.tools.HostnameUtils;
import com.ideal.flume.tools.PidUtils;
import com.ideal.flume.tools.SizedLinkedHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.CLIENT_TYPE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_REMOTE_USER_NAME;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_REMOTE_USER_PWD;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_HOSTS;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_PASSIVE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_USER_NAME;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_USER_PWD;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 文件类型的sink，目前支持hdfs、ftp、sftp和本地文件
 *
 * @author yukai
 * @version 1.0 2017年10月11日
 * @since JDK 1.6
 * 
 */
public class FileDataSink2 extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(FileDataSink2.class);

    private CollectClient fileClient;
    private CollectClient tmpFileClient;

    private static final long defaultRollInterval = 300;
    private static final long defaultRollSize = 629145600; // org-data 600M
    private static final int defaultRollCount = 1000000;
    private static final String defaultSuffix = "";
    private static final String defaultInUseSuffix = ".tmp";
    private static final int defaultBatchSize = 100000;
    private static final String defaultCodecName = "gzip";
    private static final int defaultMaxOpenFiles = 5000;
    private static final int defaultRollTimerPoolSize = 2;
    private static final String READ_NAME_KEY = "realName";

    /**
     * 文件回滚时间间隔（秒）.
     */
    private long rollInterval;
    private long rollIntervalMs;
    /**
     * 文件回滚字节.
     */
    private long rollSize;
    /**
     * 文件回滚行数.
     */
    private int rollLine;
    private int batchSize;
    private int rollTimerPoolSize;
    private CompressionCodec codeC;
    /**
     * 临时目录（固定部分）.
     */
    private String tempPath;
    /**
     * 最终目录（固定部分）.
     */
    private String filePath;
    /**
     * 文件名前缀（包含目录的动态部分）.
     */
    private String filePrefix;
    /**
     * 指定文件后缀名.
     */
    private String suffix;
    /**
     * 文件未写完时的临时后缀名.
     */
    private String inUseSuffix;
    private int maxOpenFiles;
    private ScheduledExecutorService timedRollerPool;

    private boolean useLocalTime = false;

    private Context context;

    private SendRunnable[] sendThreads;
    private Map<String, SendRunnable> pathThreadMap;

    private static final AtomicInteger INDEX = new AtomicInteger(0);
    /**
     * 文件名修饰，包含本机的hostname、pid、sink序号，用以区分文件来源.
     */
    private String myFileNameSign;

    private Counter queueSizeCounter;
    // private Counter sentLinesCounter;
    // private Counter sentBytesCounter;

    private ClientType clientType;
    private String remoteHosts;
    private String remoteUserName;
    private String remoteUserPwd;
    private boolean passiveMode = false; // ftp数据源有用

    private String coreSiteXml;
    private String hdfsSiteXml;
    private boolean isConfAuth;
    private String ticketCache;
    private String keytab;
    private String principal;
    private boolean isTicketAuth;

    private StatCounter statCounter;

    private boolean carryFile;

    private int threadCount;

    private boolean useTimestamp;

    /**
     * channel里有数据时，不能stop
     */
    private AtomicBoolean canStop = new AtomicBoolean(false);

    @Override
    public void configure(Context context) {
        String typeStr = context.getString(CLIENT_TYPE);
        if (ClientType.LOCAL.name().equalsIgnoreCase(typeStr)) {
            clientType = ClientType.LOCAL;
        } else if (ClientType.FTP.name().equalsIgnoreCase(typeStr)) {
            clientType = ClientType.FTP;
        } else if (ClientType.SFTP.name().equalsIgnoreCase(typeStr)) {
            clientType = ClientType.SFTP;
        } else if (ClientType.HDFS.name().equalsIgnoreCase(typeStr)) {
            clientType = ClientType.HDFS;
        } else {
            Preconditions.checkState(false, "Unsupported client type.");
        }

        if (clientType != ClientType.LOCAL) {
            remoteHosts = context.getString(REMOTE_HOSTS);
            if (clientType != ClientType.HDFS) {
                Preconditions.checkState(StringUtils.isNotBlank(remoteHosts),
                        "Configuration must specify remote hosts.");
            }

            remoteUserName = context.getString(REMOTE_USER_NAME, DEFAULT_REMOTE_USER_NAME);
            remoteUserPwd = context.getString(REMOTE_USER_PWD, DEFAULT_REMOTE_USER_PWD);

            if (clientType == ClientType.FTP) {
                passiveMode = context.getBoolean(REMOTE_PASSIVE, false);
            } else if (clientType == ClientType.HDFS) {
                coreSiteXml = context.getString("coresite");
                hdfsSiteXml = context.getString("hdfssite");

                isConfAuth =
                        StringUtils.isNotBlank(coreSiteXml) && StringUtils.isNotBlank(hdfsSiteXml);

                ticketCache = context.getString("ticketCache");
                keytab = context.getString("keytab");
                principal = context.getString("principal");
                isTicketAuth = StringUtils.isNotBlank(principal)
                        && (StringUtils.isNotBlank(ticketCache) || StringUtils.isNotBlank(keytab));

                if (!isConfAuth && !isTicketAuth) {
                    throw new IllegalArgumentException("hdfs file needs a authentication method");
                }
            }
        }

        filePath = Preconditions.checkNotNull(context.getString(Constants.FILE_CFG_PATH),
                "path is required");
        filePath = filePath.endsWith(Constants.DIRECTORY_DELIMITER) ? filePath
                : filePath + Constants.DIRECTORY_DELIMITER;
        tempPath = Preconditions.checkNotNull(context.getString(Constants.FILE_CFG_TMP_PATH),
                "tempPath is required");
        tempPath = tempPath.endsWith(Constants.DIRECTORY_DELIMITER) ? tempPath
                : tempPath + Constants.DIRECTORY_DELIMITER;
        filePrefix = Preconditions.checkNotNull(context.getString(Constants.FILE_CFG_PREFIX),
                "prefix is required");

        this.context = context;
        myFileNameSign="";
//        myFileNameSign = "." + HostnameUtils.getHostname() + "." + PidUtils.PID + "."+ INDEX.getAndIncrement();

        suffix = context.getString(Constants.FILE_CFG_SUFFIX, defaultSuffix);
        inUseSuffix = context.getString(Constants.FILE_CFG_INUSE_SUFFIX, defaultInUseSuffix);
        rollInterval = context.getLong(Constants.FILE_CFG_ROLL_INTERVAL, defaultRollInterval);
        rollIntervalMs = rollInterval * 1000L;
        rollSize = context.getLong(Constants.FILE_CFG_ROLL_SIZE, defaultRollSize);
        rollLine = context.getInteger(Constants.FILE_CFG_ROLL_LINE, defaultRollCount);
        batchSize = context.getInteger(Constants.BATCH_SIZE, defaultBatchSize);
        maxOpenFiles = context.getInteger(Constants.FILE_CFG_MAX_OPEN_FILES, defaultMaxOpenFiles);
        rollTimerPoolSize = context.getInteger(Constants.FILE_CFG_ROLLTIMER_POOL_SIZE,
                defaultRollTimerPoolSize);

        CompressionCodecUtils compressionCodecUtils = new CompressionCodecUtils(fileClient);
        // String codecName = context.getString(Constants.FILE_CFG_CODEC, defaultCodecName);
        String codecName = context.getString(Constants.FILE_CFG_CODEC);
        codeC = compressionCodecUtils.getCodec(codecName);

        useLocalTime = context.getBoolean(Constants.FILE_CFG_USE_LOCAL_TIME, false);
        useTimestamp = context.getBoolean("useTimestamp", true);

        this.threadCount = context.getInteger("threadCount", 4);

        carryFile = context.getBoolean("carryFile", false);
        if (carryFile) {
            // 非结构化的文件传输，不需要分行、自动隔断文件、压缩之类的配置，强制使配置无效
            this.context.put(EventSerializer.CTX_PREFIX + "appendNewline", "false");
            rollSize = -1;
            rollLine = -1;
            rollInterval = 1000000 + rollInterval;
            codeC = null;
        }
    }

    class SendRunnable extends Thread {
        WriterLinkedHashMap sfWriters;
        final Object sfWritersLock = new Object();

        private final Map<String, LinkedBlockingQueue<EventBo>> msgQueueMap =
                new ConcurrentHashMap<String, LinkedBlockingQueue<EventBo>>();
        private final Map<String, Long> msgLastAddTimeMap = Maps.newConcurrentMap();
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

        public void addEvent(EventBo event) {
            try {
//            	logger.info(" filePath : "+ filePath);
                String lookupPath = filePath + event.getRealName();
//                logger.info(" lookupPath : "+ lookupPath);
                LinkedBlockingQueue<EventBo> que = msgQueueMap.get(lookupPath);
                if (null == que) {
                    que = new LinkedBlockingQueue<EventBo>(10000);
                    msgQueueMap.put(lookupPath, que);
                    msgLastAddTimeMap.put(lookupPath, System.currentTimeMillis());
                }
                que.put(event);
                queueSizeCounter.incr();
            } catch (InterruptedException ex) {
                logger.info("", ex);
            }
        }

        private void send(Event event, int size, String lookupPath, String realName)
                throws Exception {
            BucketWriter bucketWriter;
            YeeFileWriter redcamWriter = null;
            // Callback to remove the reference to the bucket writer from the
            // sfWriters map so that all buffers used by the HDFS file
            // handles are garbage collected.
            WriterCallback closeCallback = new WriterCallback() {
                @Override
                public void run(String bucketPath) {
                    logger.info("Writer callback called.");
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
                    redcamWriter = new YeeFileWriter(context, fileClient);
                    bucketWriter = new BucketWriter(fileClient, redcamWriter, rollInterval,
                            rollSize, rollLine, batchSize, tempPath, filePath, realName,
                            inUseSuffix, suffix, codeC, timedRollerPool, closeCallback, lookupPath,
                            useTimestamp);
                    sfWriters.put(lookupPath, bucketWriter);
                }
            }

            // Write the data to HDFS
            try {
                bucketWriter.append(event, size);
            } catch (FlumeException ex) {
                logger.info("Bucket was closed while trying to append, "
                        + "reinitializing bucket and writing event.");
                redcamWriter = new YeeFileWriter(context, fileClient);
                bucketWriter = new BucketWriter(fileClient, redcamWriter, rollInterval, rollSize,
                        rollLine, batchSize, tempPath, filePath, realName, inUseSuffix, suffix,
                        codeC, timedRollerPool, closeCallback, lookupPath, useTimestamp);
                synchronized (sfWritersLock) {
                    sfWriters.put(lookupPath, bucketWriter);
                }
                bucketWriter.append(event, size);
            }
        }

        @Override
        public void run() {
            while (!interrupted || msgQueueMap.size() > 0) {
                if (msgQueueMap.size() == 0) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ex) {
                        logger.error("", ex);
                    }
                    continue;
                }

                Set<String> paths = msgQueueMap.keySet();
                for (String lookupPath : paths) {
                    LinkedBlockingQueue<EventBo> msgQueue = msgQueueMap.get(lookupPath);
                    Long lastTime = msgLastAddTimeMap.get(lookupPath);
                    if (null == lastTime) {
                        lastTime = System.currentTimeMillis();
                        msgLastAddTimeMap.put(lookupPath, lastTime);
                    }

                    // if (null == msgQueue || (msgQueue.size() < batchSize
                    // && System.currentTimeMillis() - lastTime < (rollInterval * 1000L) &&
                    // !interrupted)) {
                    // continue;
                    // }

                    if (null == msgQueue || msgQueue.size() == 0) {
                        continue;
                    }

                    msgQueueMap.remove(lookupPath);
                    msgLastAddTimeMap.remove(lookupPath);

                    try {
                        EventBo tmpEvent = msgQueue.peek();
                        String realName = tmpEvent.getRealName();

                        List<EventBo> toSends = new ArrayList<EventBo>(batchSize);
                        int drainLen = msgQueue.drainTo(toSends, batchSize);

                        while (drainLen > 0) {
                            Event event = new SimpleEvent();
                            ByteArrayOutputStream out = new ByteArrayOutputStream(drainLen * 256);
                            long bodyLen = 0;

                            Iterator<EventBo> it = toSends.iterator();
                            EventBo bigEvent = it.next();
                            while (null != bigEvent) {
                                out.write(bigEvent.getEvent().getBody());
                                bodyLen += bigEvent.getEvent().getBody().length;

                                if (it.hasNext()) {
                                    bigEvent = it.next();

                                    if (!carryFile) {
                                        out.write(Constants.LINE_SEPARATOR);
                                    }
                                } else {
                                    bigEvent = null;
                                }
                            }
                            event.setBody(out.toByteArray());
                            out.close();

                            try {
                                send(event, drainLen, lookupPath, realName);
                            } catch (Throwable e1) {
                                logger.error("", e1);
                            } finally {
                                queueSizeCounter.add(-drainLen);
                            }

                            // sentLinesCounter.add(drainLen);
                            // sentBytesCounter.add(bodyLen);
                            statCounter.addEvent(drainLen);
                            statCounter.addByte(bodyLen);

                            toSends.clear();
                            drainLen = msgQueue.drainTo(toSends, batchSize);
                        }
                    } catch (Throwable ex) {
                        logger.error("", ex);
                    }
                }
            }
        }
    }

    @Override
    public Status process() throws FlumeException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            int txnEventCount = 0;
            
            for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
                Event event = channel.take();
                if (null == event) {
                    canStop.set(true);
                    break;
                }

                canStop.set(false);
               String realName = SimpleBucketPath.simpleEscapeString(filePrefix,
                        event.getHeaders(), useLocalTime) + myFileNameSign;

                SendRunnable sendThread = pathThreadMap.get(realName);
                if (null == sendThread) {
                    sendThread =
                            sendThreads[(realName.hashCode() & Integer.MAX_VALUE) % threadCount];
                    pathThreadMap.put(realName, sendThread);
                }

                sendThread.addEvent(new EventBo(event, realName));
            }

            transaction.commit();

            if (txnEventCount == 0) {
                return Status.BACKOFF;
            } else {
                return Status.READY;
            }
        } catch (Throwable th) {
            transaction.rollback();
            logger.error("process failed", th);
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new FlumeException(th);
            }
        } finally {
            transaction.close();
        }
    }

    @Override
    public void stop() {
        while (!canStop.get()) {
            try {
                logger.info("channel is not empty, sleep 5 seconds.");
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                logger.error("", ex);
            }
        }

        for (SendRunnable sendThread : sendThreads) {
            if (sendThread.msgQueueMap.size() > 0) {
                try {
                    logger.info("sendThread {} is not done, sleep 5 seconds.",
                            sendThread.getName());
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    logger.error("", ex);
                }
            }
        }

        // do not constrain close() calls with a timeout
        for (SendRunnable sendThread : sendThreads) {
            synchronized (sendThread.sfWritersLock) {
                for (Entry<String, BucketWriter> entry : sendThread.sfWriters.entrySet()) {
                    logger.info("Closing {}", entry.getKey());

                    try {
                        entry.getValue().close();
                    } catch (Exception ex) {
                        logger.warn("Exception while closing " + entry.getKey() + ". "
                                + "Exception follows.", ex);
                        if (ex instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        }

        // shut down all our thread pools
        timedRollerPool.shutdown();
        try {
            while (!timedRollerPool.isTerminated()) {
                timedRollerPool.awaitTermination(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            logger.warn("shutdown interrupted on " + timedRollerPool, ex);
        }

        timedRollerPool = null;

        for (SendRunnable sendThread : sendThreads) {
            sendThread.sfWriters.clear();
            sendThread.sfWriters = null;
            sendThread.interrupt();
        }

        super.stop();
    }

    @Override
    public void start() {
        ClientProps clientProps;
        if (clientType == ClientType.LOCAL) {
            String localIp = "127.0.0.1";
            try {
                localIp = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                logger.error("", e);
            }
            clientProps = new ClientProps(localIp, clientType);
            clientProps.setIp(localIp);

            statCounter = StatCounters.create(StatType.SINK, localIp);
        } else if (clientType == ClientType.HDFS) {
            clientProps = new ClientProps(filePath, clientType);
            clientProps.setWorkingDirectory(filePath);

            if (isConfAuth) {
                Configuration hdfsConfig = new Configuration();
                try {
                    hdfsConfig.addResource(new FileInputStream(coreSiteXml));
                    hdfsConfig.addResource(new FileInputStream(hdfsSiteXml));
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException("HDFS config xml file does not exist.", e);
                }

                UserGroupInformation.setConfiguration(hdfsConfig);
                clientProps.setHdfsConfig(hdfsConfig);
            } else {
                clientProps.setPrincipal(principal);
                clientProps.setTicketCache(ticketCache);
                clientProps.setKeytab(keytab);
            }

            // statCounter = StatCounters.create(StatType.SINK, filePath);
            statCounter = StatCounters.create(StatType.SINK, getName());
        } else {
            int it = remoteHosts.indexOf(":");
            String ip = remoteHosts.substring(0, it);
            String portStr = remoteHosts.substring(it + 1);

            clientProps = new ClientProps(ip, clientType);
            clientProps.setIp(ip);
            clientProps.setPort(Integer.parseInt(portStr));
            clientProps.setUserName(remoteUserName);
            clientProps.setPassword(remoteUserPwd);
            clientProps.setPassiveMode(passiveMode);

            statCounter = StatCounters.create(StatType.SINK, ip);
        }

        try {
            fileClient = CollectClientFactory.createClient(clientProps);
            tmpFileClient = CollectClientFactory.createClient(clientProps);
        } catch (Exception e) {
            logger.error("", e);
            throw new FlumeException("can not create file client.");
        }

        String rollerName = getName() + "-roll-timer-%d";
        timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
                new ThreadFactoryBuilder().setNameFormat(rollerName).build());

        // timedRollerPool.scheduleWithFixedDelay(new TmpFileKiller(), rollIntervalMs,
        // rollIntervalMs,
        // TimeUnit.MILLISECONDS);

        sendThreads = new SendRunnable[threadCount];
        for (int i = 0; i < threadCount; i++) {
            SendRunnable s = new SendRunnable(getName() + "-send-" + i);
            s.sfWriters = new WriterLinkedHashMap(maxOpenFiles / threadCount);
            sendThreads[i] = s;
            s.start();
        }
        pathThreadMap = new SizedLinkedHashMap<String, SendRunnable>(64);

        queueSizeCounter = Counters.create(getName(), "queue", 1);
        // sentLinesCounter = Counters.create(getName(), "lines", 0);
        // sentBytesCounter = Counters.create(getName(), "bytes", 0);

        super.start();
    }

   /* class TmpFileKiller implements Runnable {

        @Override
        public void run() {
            try {
                List<CollectFile> files =
                        tmpFileClient.listFiles(tempPath, new CollectFileFilter() {
                            @Override
                            public boolean accept(CollectFile file) {
                                return file.getName().endsWith(inUseSuffix)
                                        && System.currentTimeMillis()
                                                - file.getTimeInMillis() > rollIntervalMs;
                            }
                        }, true);

                for (CollectFile file : files) {
                    String absolutePath = file.getAbsolutePath();

                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(tmpFileClient.retrieveFileStream(absolutePath)));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Event event = new SimpleEvent();
                        event.setBody(line.getBytes(Charsets.UTF_8));

                        String realName = absolutePath.replace(tempPath, "");
                        realName = realName.substring(0, realName.length() - inUseSuffix.length());
                        realName = realName + myFileNameSign;

                        SendRunnable sendThread = pathThreadMap.get(realName);
                        if (null == sendThread) {
                            sendThread = sendThreads[(realName.hashCode() & Integer.MAX_VALUE)
                                    % threadCount];
                            pathThreadMap.put(realName, sendThread);
                        }
                        sendThread.addEvent(new EventBo(event, realName));
                    }
                    reader.close();
                    tmpFileClient.deleteFile(absolutePath);
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }

    }
*/
    @Override
    public String toString() {
        return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() + " }";
    }

    /*
     * Extended Java LinkedHashMap for open file handle LRU queue. We want to clear the oldest file
     * handle if there are too many open ones.
     */
    private static class WriterLinkedHashMap extends LinkedHashMap<String, BucketWriter> {
        private static final long serialVersionUID = 4456535248932876110L;
        private final int maxOpenFiles;

        public WriterLinkedHashMap(int maxOpenFiles) {
            super(16, 0.75f, true); // stock initial capacity/load, access ordering
            this.maxOpenFiles = maxOpenFiles;
        }

        @Override
        protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
            if (size() > maxOpenFiles) {
                // If we have more that max open files, then close the last one and return true
                try {
                    eldest.getValue().close();
                } catch (InterruptedException ex) {
                    logger.warn(eldest.getKey().toString(), ex);
                    Thread.currentThread().interrupt();
                } catch (Exception ex) {
                    logger.warn(eldest.getKey().toString(), ex);
                }
                return true;
            } else {
                return false;
            }
        }
    }

}
