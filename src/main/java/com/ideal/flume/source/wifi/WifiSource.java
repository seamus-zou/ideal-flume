package com.ideal.flume.source.wifi;

import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SUFFIX;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.ideal.flume.cache.CacheClient;
import com.ideal.flume.cache.CacheUtils;
import com.ideal.flume.clients.ClientProps;
import com.ideal.flume.clients.CollectClient;
import com.ideal.flume.clients.CollectClientFactory;
import com.ideal.flume.enums.ClientType;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.stat.Counters.Counter;

public class WifiSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory.getLogger(WifiSource.class);

    private WifiTypeEnum wifiType;
    private String wifiTypeStr;
    private String spoolDirectory;
    private String suffix;

    private String ftpIp; // 服务器IP地址
    private int ftpPort;// 端口号
    private String ftpUserName; // 用户名
    private String ftpUserPwd; // 密码
    private boolean passiveMode;
    private boolean remoteverification;
    private int bufSize = 16384;

    private String extraRedisKey;

    private Counter eventsCounter;

    private CacheClient cacheClient;
    private CollectClient ftpClient;

    private ScheduledExecutorService timerExecutor;

    private final static Charset READ_CHARSET = Charsets.toCharset("UTF-8");

    private final static Charset WRITE_CHARSET = Charsets.toCharset("UTF-8");

    private final List<Event> eventList = new ArrayList<Event>();

    @Override
    public void configure(Context context) {
        spoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null,
                "Configuration must specify a spooling directory");

        String typeStr = context.getString("wifiType");
        Preconditions.checkState(typeStr != null, "Configuration must specify a wifi type");

        wifiTypeStr = typeStr.toUpperCase();
        try {
            wifiType = WifiTypeEnum.valueOf(wifiTypeStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        spoolDirectory = spoolDirectory.endsWith("/") ? spoolDirectory : spoolDirectory + "/";

        suffix = context.getString(SUFFIX, StringUtils.EMPTY);

        ftpIp = context.getString("ftpIp");
        Preconditions.checkState(ftpIp != null, "Configuration must specify ftp id");

        ftpUserName = context.getString("ftpUserName");
        ftpUserPwd = context.getString("ftpUserPwd");
        ftpPort = context.getInteger("ftpPort", 21);
        passiveMode = context.getBoolean("passiveMode", false);
        remoteverification = context.getBoolean("remoteverification", true);
        
        extraRedisKey = context.getString("extraRedisKey", StringUtils.EMPTY);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Event event;
        Map<String, String> headers;

        try {
            List<CollectFile> files = listFiles();
            if (null == files || files.size() == 0) {
                logger.info(wifiTypeStr + " list file empty.");
                return Status.BACKOFF;
            }

            for (CollectFile file : files) {
                boolean hasException = false;
                String fileName = file.getName();
                cacheClient.cacheReading(fileName);

                String path = getPath(fileName);

                ZipInputStream zis = null;
                BufferedReader reader = null;
                try {
                    if (suffix.endsWith("zip")) {
                        zis = new ZipInputStream(
                                ftpClient.retrieveFileStream(spoolDirectory + fileName),
                                READ_CHARSET);
                        reader = new BufferedReader(new InputStreamReader(zis, READ_CHARSET),
                                bufSize);

                        ZipEntry entry = null;
                        while ((entry = zis.getNextEntry()) != null) {
                            String entryName = entry.getName();

                            String line;
                            while ((line = reader.readLine()) != null) {
                                event = new SimpleEvent();
                                headers = new HashMap<String, String>();
                                headers.put("%{file}", entryName);
                                headers.put("way", wifiTypeStr);
                                headers.put("%{path}", path);
                                event.setHeaders(headers);
                                event.setBody(line.getBytes(WRITE_CHARSET));
                                eventsCounter.incr();

                                eventList.add(event);

                                if (eventList.size() >= 1000) {
                                    getChannelProcessor().processEventBatch(eventList);
                                    eventList.clear();
                                }
                            }
                        }
                    } else {
                        reader = new BufferedReader(new InputStreamReader(
                                ftpClient.retrieveFileStream(spoolDirectory + fileName),
                                READ_CHARSET), bufSize);

                        String line;
                        while ((line = reader.readLine()) != null) {
                            event = new SimpleEvent();
                            headers = new HashMap<String, String>();
                            headers.put("%{file}", fileName);
                            headers.put("way", wifiTypeStr);
                            headers.put("%{path}", path);
                            event.setHeaders(headers);
                            event.setBody(line.getBytes(WRITE_CHARSET));
                            eventsCounter.incr();

                            eventList.add(event);

                            if (eventList.size() >= 1000) {
                                getChannelProcessor().processEventBatch(eventList);
                                eventList.clear();
                            }
                        }
                    }

                    if (eventList.size() > 0) {
                        getChannelProcessor().processEventBatch(eventList);
                        eventList.clear();
                    }

                } catch (Exception e) {
                    hasException = true;
                    logger.error("", e);
                    cacheClient.cacheFailed(fileName);
                } finally {
                    cacheClient.removeReading(fileName);

                    if (!hasException) {
                        cacheClient.cacheReaded(fileName);
                    }

                    ftpClient.completePendingCommand();

                    if (null != reader) {
                        reader.close();
                    }
                    if (null != zis) {
                        zis.close();
                    }
                }
            }

            return Status.READY;
        } catch (Exception e) {
            logger.error("WifiSource EXCEPTION, {}", e);
            return Status.BACKOFF;
        }
    }

    private String getPath(String name) {
        int len = name.length();

        switch (wifiType) {
            case AP:
                return "AP/" + name.substring(len - 12, len - 4);
            case PLACE:
                return "Place/" + name.substring(len - 12, len - 4);
            case SSID:
                return "SSID/" + name.substring(len - 12, len - 4);
            case CUSTOMER:
                return "Customer/" + name.substring(len - 12, len - 4);
            case CONNECT:
                return "Connect/" + name.substring(len - 18, len - 10) + "/"
                        + name.substring(len - 10, len - 8);
            case PV:
                return "PV/" + name.substring(len - 10, len - 2) + "/" + name.substring(len - 2);
        }

        return null;
    }

    private final static long delta = 14 * 24 * 3600 * 1000L;

    private List<CollectFile> listFiles() throws Exception {
        final long now = System.currentTimeMillis();
        return ftpClient.listFiles(spoolDirectory, new CollectFileFilter() {
            @Override
            public boolean accept(CollectFile file) {
                String fileName = file.getName();

                // PV数据有时会有gz包，不是我们需要的，通过文件名长度识别
                if (wifiType == WifiTypeEnum.PV && fileName.length() != 21) {
                    return false;
                }

                if (now - file.getTimeInMillis() > delta || file.isDirectory()
                        || fileName.startsWith(".") || !fileName.endsWith(suffix) ||
                // 已经被读过的文件
                (cacheClient.isReaded(fileName)) ||
                // 正在被处理的files
                (cacheClient.isReading(fileName)) // ||
                // 已确认失败的文件
                /*cacheClient.isFailed(fileName)*/) {
                    return false;
                }
                return true;
            }
        });
    }

    @Override
    public synchronized void start() {
        timerExecutor = Executors.newSingleThreadScheduledExecutor();
        timerExecutor.scheduleWithFixedDelay(new CacheCleanRunnable(), 1, 60, TimeUnit.MINUTES);

        String cacheKey = ftpIp + "_wifi_" + wifiTypeStr;
        cacheClient = CacheUtils.getCacheClient(cacheKey + extraRedisKey);

        ClientProps clientProps = new ClientProps(ftpIp, ClientType.FTP);
        clientProps.setIp(ftpIp);
        clientProps.setPort(ftpPort);
        clientProps.setUserName(ftpUserName);
        clientProps.setPassword(ftpUserPwd);
        clientProps.setPassiveMode(passiveMode);
        clientProps.setRemoteverification(remoteverification);

        try {
            ftpClient = CollectClientFactory.createClient(clientProps);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        eventsCounter = Counters.create("source", wifiTypeStr, 0);

        super.start();
        logger.info("WifiSource source started");
    }

    @Override
    public synchronized void stop() {
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

    private final static long cache_keep = 20 * 24 * 3600 * 1000L;
    private final static long reading_cache_keep = 300000L;

    private class CacheCleanRunnable implements Runnable {
        @Override
        public void run() {
            try {
                logger.info("clean old cache file...");

                cacheClient.cleanReadingCache(
                        getLifecycleState() == LifecycleState.START ? reading_cache_keep : 0);
                cacheClient.cleanReadedCache(cache_keep);
                cacheClient.cleanFailedCache(cache_keep);

                logger.info("clean old cache file done.");
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

}
