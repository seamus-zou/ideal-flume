package com.ideal.flume.source;

import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.BATCH_SIZE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.BUFFER_MAX_LINE_LENGTH;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.CLIENT_TYPE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.CONSUME_ORDER;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DATA_FILE_TYPE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DATA_REPAIR_DIRECTORY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_BUF_SIZE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_DELETE_POLICY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_DESERIALIZER;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_MAX_BACKOFF;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_REMOTE_USER_NAME;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_REMOTE_USER_PWD;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DEFAULT_SWITCH_TIME;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DELETE_POLICY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DESERIALIZER;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.DYNAMIC_DIR_REG;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.INPUT_CHARSET;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.MAX_BACKOFF;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.PREFIX;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_HOSTS;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_PASSIVE;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_USER_NAME;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.REMOTE_USER_PWD;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SUFFIX;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SWITCH_TIME;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.ideal.flume.cache.CacheClient;
import com.ideal.flume.cache.CacheUtils;
import com.ideal.flume.client.avro.YeeFileEventReader3;
import com.ideal.flume.clients.ClientProps;
import com.ideal.flume.enums.ClientType;
import com.ideal.flume.enums.ConsumeOrder;
import com.ideal.flume.enums.DataFileType;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.DateTimeUtils;
import com.ideal.flume.tools.HostnameUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YeeSpoolDirectorySource3 extends AbstractSource
        implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(YeeSpoolDirectorySource3.class);

    private String spoolDirectory; // 监听目录
    private String orgSpoolDirectory; // 基础目录(不变的)，在此目录下有动态目录
    private String dataRepairDirectory; // 补数据目录(到天级别的)
    // 目录名在日期后面还有其他层级 如：XXXX/AAA/20171101/output/pub
    private String busDir;
    private boolean isRepair;
    private String currentDay;
    private boolean needSwitchHour;
    private String dynamicDirReg; // 动态目录命名规则，同DateFormat
    private SimpleDateFormat dynamicFormat; // 同上
    private int switchTime; // 强制切换动态目录的时间（整点小时）
    private boolean needSwitchDir = false; // 是否需要动态切换目录

    private DataFileType dataFileType; // 数据文件类型
    private String prefix; // 文件匹配前缀
    private String suffix; // 文件匹配后缀

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

    private int batchSize;
    private String deserializerType;
    private Context deserializerContext;
    private String deletePolicy;
    private String inputCharset;
    private volatile boolean hasFatalError = false;

    private ScheduledExecutorService timerExecutor;
    private boolean backoff = true;
    private boolean hitChannelException = false;
    private int maxBackoff;
    private ConsumeOrder consumeOrder;

    private int bufferSize = DEFAULT_BUF_SIZE;

    private List<Thread> readThreads;
    private List<YeeFileEventReader3> readers;
    private int readThreadCount;
    private int channelCount;
    private int sendThreadCount;

    private String extraRedisKey;

    private List<CacheClient> cacheClients;
    private String hostname;

    private boolean interrupted = false;

    // 差异天数
    private int diffdayNum = 0;


    @Override
    public void configure(Context context) {
        logger.info("YeeSpoolDirectorySource configure start...");
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

        String fileTypeStr = context.getString(DATA_FILE_TYPE);
        if (DataFileType.TEXT.name().equalsIgnoreCase(fileTypeStr)) {
            dataFileType = DataFileType.TEXT;
        } else if (DataFileType.GZ.name().equalsIgnoreCase(fileTypeStr)) {
            dataFileType = DataFileType.GZ;
        } else {
            Preconditions.checkState(false, "Unknown data file type.");
        }

        orgSpoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(StringUtils.isNotBlank(orgSpoolDirectory),
                "Configuration must specify a spooling directory");

        orgSpoolDirectory =
                orgSpoolDirectory.endsWith("/") ? orgSpoolDirectory : orgSpoolDirectory + "/";

        busDir = context.getString("busDir");
        if (null == busDir) {
            busDir = "";
        } else {
            busDir = busDir.endsWith("/") ? busDir : busDir + "/";
        }

        spoolDirectory = orgSpoolDirectory;
        String dirDateFormat = StringUtils.trimToNull(context.getString(DYNAMIC_DIR_REG));
        if (StringUtils.isNotEmpty(dirDateFormat)) {
            currentDay = getDirDateStr(dirDateFormat);
            spoolDirectory = orgSpoolDirectory + currentDay;
            needSwitchDir = true;
            logger.info(
                    "******currentDay : " + currentDay + "*******spoolDirectory:" + spoolDirectory + "*******busDir:" + busDir);
        }

        dataRepairDirectory = context.getString(DATA_REPAIR_DIRECTORY);
        if (StringUtils.isNotBlank(dataRepairDirectory)) {
            isRepair = true;
            spoolDirectory = dataRepairDirectory;
            String tmp = dataRepairDirectory.endsWith("/")
                    ? dataRepairDirectory.substring(0, dataRepairDirectory.length() - 1)
                    : dataRepairDirectory;
            currentDay = tmp.substring(tmp.lastIndexOf('/') + 1);
            needSwitchDir = false;
        }

        needSwitchHour = context.getBoolean("needSwitchHour", false);

        prefix = context.getString(PREFIX, StringUtils.EMPTY);
        suffix = context.getString(SUFFIX, StringUtils.EMPTY);
        switchTime = context.getInteger(SWITCH_TIME, DEFAULT_SWITCH_TIME);

        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);
        inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
        deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));
        Integer bufferMaxLineLength = context.getInteger(BUFFER_MAX_LINE_LENGTH, 204800);
        if (bufferMaxLineLength != null && deserializerType != null
                && deserializerType.equalsIgnoreCase(DEFAULT_DESERIALIZER)) {
            deserializerContext.put(LineDeserializer.MAXLINE_KEY, bufferMaxLineLength.toString());
        }
        consumeOrder = ConsumeOrder.valueOf(
                context.getString(CONSUME_ORDER, DEFAULT_CONSUME_ORDER.toString()).toUpperCase());
        maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);

        readThreadCount = context.getInteger("threadCount", 1);
        channelCount = context.getInteger("channelCount", 1);
        sendThreadCount = context.getInteger("sendThreadCount", 4);
        readThreads = new ArrayList<Thread>(readThreadCount);
        readers = new ArrayList<YeeFileEventReader3>(readThreadCount);
        hostname = HostnameUtils.getHostname();

        extraRedisKey = context.getString("extraRedisKey", StringUtils.EMPTY);

        logger.info("YeeSpoolDirectorySource configure end.");
    }


    /**
     * 获取文件夹日期字符串
     * 
     * @param dateFormat
     * @return
     */
    private String getDirDateStr(String dateFormat) {
        if (null != dateFormat) {
            try {
                String[] strArray = dateFormat.split(";");
                if (strArray.length == 1) {
                    dynamicDirReg = dateFormat;
                } else if (strArray.length == 2) {
                    dynamicDirReg = strArray[0];
                    String diffDays = strArray[1];
                    diffdayNum = Integer.valueOf(diffDays);
                }
                dynamicFormat = new SimpleDateFormat(dynamicDirReg);
                Calendar ca = Calendar.getInstance();
                if (diffdayNum < 0) {
                    ca.add(Calendar.DATE, diffdayNum);
                }
                return DateTimeUtils.customDateTime(ca, dynamicDirReg);

            } catch (Exception e) {
                throw new IllegalStateException(
                        "dynamic directory regular is not a datetime format");
            }
        }

        return "";
    }

    @Override
    public synchronized void start() {
        logger.info("YeeSpoolDirectorySource source starting with directory: {}, {}", spoolDirectory, busDir);

        cacheClients = new ArrayList<CacheClient>();

        timerExecutor = Executors.newScheduledThreadPool(3);
        if (needSwitchHour) {
            timerExecutor.scheduleWithFixedDelay(new SwitchHourDirRunnable(), 1, 1,
                    TimeUnit.MINUTES);
        }
        if (needSwitchDir) {
            timerExecutor.scheduleWithFixedDelay(new TimerTaskRunnable(), 5, 5, TimeUnit.MINUTES);
        }
        timerExecutor.scheduleWithFixedDelay(new CacheCleanRunnable(), 1, 60, TimeUnit.MINUTES);

        if (clientType == ClientType.LOCAL) {
            String localIp = "127.0.0.1";
            try {
                localIp = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                logger.error("", e);
            }
            ClientProps clientProps = new ClientProps(localIp, clientType);
            clientProps.setIp(localIp);

            StatCounter statCounter = StatCounters.create(StatType.SOURCE, localIp);

            for (int i = 0; i < readThreadCount; i++) {
                Thread thread = new Thread(new SpoolDirectoryRunnable(clientProps, statCounter),
                        "source-" + localIp + "-" + i);
                thread.start();
                readThreads.add(thread);
            }
        } else if (clientType == ClientType.HDFS) {
            ClientProps clientProps = new ClientProps(orgSpoolDirectory, clientType);
            clientProps.setWorkingDirectory(orgSpoolDirectory);

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

            StatCounter statCounter = StatCounters.create(StatType.SOURCE, busDir);

            for (int i = 0; i < readThreadCount; i++) {
                Thread thread = new Thread(new SpoolDirectoryRunnable(clientProps, statCounter),
                        "source-hdfs-" + i);
                thread.start();
                readThreads.add(thread);
            }
        } else {
            String[] hosts = remoteHosts.split(",");
            for (String host : hosts) {
                int it = host.indexOf(":");
                String ip = host.substring(0, it);
                String portStr = host.substring(it + 1);

                StatCounter statCounter = StatCounters.create(StatType.SOURCE, ip);

                ClientProps clientProps = new ClientProps(ip, clientType);
                clientProps.setIp(ip);
                clientProps.setPort(Integer.parseInt(portStr));
                clientProps.setUserName(remoteUserName);
                clientProps.setPassword(remoteUserPwd);
                clientProps.setPassiveMode(passiveMode);
                for (int i = 0; i < readThreadCount; i++) {
                    Thread thread = new Thread(new SpoolDirectoryRunnable(clientProps, statCounter),
                            "source-" + ip + "-" + i);
                    thread.start();
                    readThreads.add(thread);
                }
            }
        }

        super.start();
        logger.info("SpoolDirectorySource source started");
    }

    @Override
    public synchronized void stop() {
        interrupted = true;
        for (YeeFileEventReader3 reader : readers) {
            reader.stop();
        }

        for (Thread t : readThreads) {
            t.interrupt();
        }

        timerExecutor.shutdown();
        try {
            timerExecutor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }

        timerExecutor.shutdown();
        super.stop();
        logger.info("SpoolDir source {} stopped.", getName());
    }

    @Override
    public String toString() {
        return "Spool Directory source " + getName() + ": { spoolDir: " + spoolDirectory + " }";
    }

    class SwitchHourDirRunnable implements Runnable {
        @Override
        public void run() {
            try {
                logger.info("switch hour task start...");
                boolean fileend = true;
                for (YeeFileEventReader3 reader : readers) {
                    fileend = reader.fileend() && fileend;
                }

                Calendar calendar = Calendar.getInstance();
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                // 切换动态目录
                if (fileend) {
                    for (YeeFileEventReader3 reader : readers) {
                        if (!currentDay.equals(reader.getCurrentDay())
                                || reader.getCurrentHour() <= hour)
                            reader.switchNextHour();
                    }

                    logger.info("switch into next hour directory.");
                }
                logger.info("switch hour task done.");
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
    }

    private class TimerTaskRunnable implements Runnable {
        @Override
        public void run() {
            try {
                logger.info("switch day task start...");
                boolean fileend = true;
                for (YeeFileEventReader3 reader : readers) {
                    if (needSwitchHour) {
                        fileend = reader.getCurrentHour() == 23 && reader.fileend() && fileend;
                    } else {
                        fileend = reader.fileend() && fileend;
                    }
                }

                // 切换动态目录
                Calendar calendar = Calendar.getInstance();
                if (diffdayNum < 0) {
                    calendar.add(Calendar.DATE, ++diffdayNum);
                }
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                if (((fileend && hour >= 0) || hour >= switchTime)) {
                    String today = dynamicFormat.format(calendar.getTime());
                    spoolDirectory =
                            orgSpoolDirectory + today + (needSwitchHour ? "/00/" : "/") + busDir;
                    boolean done = false;
                    for (YeeFileEventReader3 reader : readers) {
                        // if (!StringUtils.equals(reader.getSpoolDirectory(),
                        // spoolDirectory)) {
                        if (!StringUtils.equals(reader.getCurrentDay(), today)) {
                            done = true;
                            reader.setSpoolDirectory(today);
                        }
                    }

                    if (done) {
                        logger.info("switch into new spool directory: {}", spoolDirectory);
                        currentDay = today;
                    }
                }
                logger.info("switch day task done.");
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private final static long cache_keep = 7 * 24 * 3600 * 1000L;
    private final static long reading_cache_keep = 300000L;

    private class CacheCleanRunnable implements Runnable {
        @Override
        public void run() {
            try {
                logger.info("clean old cache file...");

                for (CacheClient cc : cacheClients) {
                    cc.cleanReadingCache(
                            getLifecycleState() == LifecycleState.START ? reading_cache_keep : 0);
                    cc.cleanReadedCache(cache_keep);
                    cc.cleanFailedCache(cache_keep);
                    cc.cleanReadedLineCache(cache_keep);
                }

                logger.info("clean old cache file done.");
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    private class SpoolDirectoryRunnable implements Runnable {
        private final YeeFileEventReader3 reader;
        private final StatCounter statCounter;

        public SpoolDirectoryRunnable(ClientProps clientProps, StatCounter statCounter) {
            this.statCounter = statCounter;

            try {
                CacheClient cacheClient =
                        CacheUtils.getCacheClient(clientProps.getName() + extraRedisKey);
                cacheClients.add(cacheClient);

                String path = spoolDirectory.endsWith("/") ? spoolDirectory : spoolDirectory + "/";
                if (needSwitchHour) {
                    path = path + "00/" + busDir;
                } else {
                    path = path + busDir;
                }

                this.reader = new YeeFileEventReader3.Builder().spoolDirectory(path)
                        .baseDir(orgSpoolDirectory).currentDayDir(currentDay).currentHourDir(0)
                        .needSwitchHour(needSwitchHour).clientProps(clientProps)
                        .cacheClient(cacheClient).deserializerType(deserializerType)
                        .deserializerContext(deserializerContext).deletePolicy(deletePolicy)
                        .inputCharset(inputCharset).consumeOrder(consumeOrder).bufSize(bufferSize)
                        .prefix(prefix).suffix(suffix).channelCount(channelCount)
                        .sendThreadCount(sendThreadCount).dataFileType(dataFileType)
                        .hostname(hostname).dynamicDirReg(dynamicDirReg).isRepair(isRepair)
                        .busDir(busDir).build();
                readers.add(reader);
            } catch (Exception ioe) {
                logger.error(ioe.getMessage(), ioe);
                throw new FlumeException("Error instantiating spooling event parser", ioe);
            }
        }

        @Override
        public void run() {
            int backoffInterval = 250;
            while (!interrupted) {
                try {
                    if (isRepair && reader.fileend()) {
                        // 补数据或重采完成后，停止线程
                        return;
                    }

                    List<Event> events = reader.readEvents(batchSize);
                    if (interrupted) {
                        return;
                    }

                    if (events.isEmpty()) {
                        reader.commit();
                        Thread.sleep(100);
                        continue;
                    }

                    for (Event event : events) {
                        int tmpLen = event.getBody().length;

                        statCounter.incrEvent();
                        statCounter.addByte(tmpLen);
                    }

                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelException ex) {
                        logger.error("", ex);
                        logger.warn("The channel is full, and cannot write data now. The "
                                + "source will try again after " + String.valueOf(backoffInterval)
                                + " milliseconds");
                        hitChannelException = true;
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval =
                                    backoffInterval >= maxBackoff ? maxBackoff : backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                } catch (Throwable t) {
                    String err = "FATAL: " + YeeSpoolDirectorySource3.this.toString() + ": "
                            + "Uncaught exception in SpoolDirectorySource thread. "
                            + "Restart or reconfigure Flume to continue processing.";
                    logger.error(err, t);
                    hasFatalError = true;
                }

                if (logger.isDebugEnabled()) {
                    // logger.debug("Spooling Directory Source runner has shutdown.");
                    // logger.debug("cost :{} ms.", System.currentTimeMillis() -
                    // startTime);
                    // logger.debug("logger sink number is {}.",
                    // org.apache.flume.sink.LoggerSink.count);
                }
            }
        }
    }

    @VisibleForTesting
    protected boolean hasFatalError() {
        return hasFatalError;
    }

    @VisibleForTesting
    protected boolean hitChannelException() {
        return hitChannelException;
    }
}
