package com.ideal.flume.source.iptv;


import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.SUFFIX;
import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.PREFIX;

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
import com.google.common.collect.Maps;
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

public class IptvZipSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory.getLogger(IptvZipSource.class);

    private String spoolDirectory;
    private String prefix;
    private String suffix;

    private String ftpIp; // 服务器IP地址
    private int ftpPort;// 端口号
    private String ftpUserName; // 用户名
    private String ftpUserPwd; // 密码
    private int bufSize = 16384;

    private String extraRedisKey;

    private static final Map<IptvTypeEnum, Counter> eventsCounterMap = Maps.newConcurrentMap();

    private CacheClient cacheClient;
    private CollectClient ftpClient;

    private ScheduledExecutorService timerExecutor;

    private final static Charset READ_CHARSET = Charsets.toCharset("GBK");

    private final static Charset WRITE_CHARSET = Charsets.toCharset("UTF-8");

    private final List<Event> eventList = new ArrayList<Event>();

    static {
        for (IptvTypeEnum tp : IptvTypeEnum.values()) {
            eventsCounterMap.put(tp, Counters.create("source", tp.name(), 0));
        }
    }

    @Override
    public void configure(Context context) {
        spoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null,
                "Configuration must specify a spooling directory");

        spoolDirectory = spoolDirectory.endsWith("/") ? spoolDirectory : spoolDirectory + "/";

        prefix = context.getString(PREFIX, StringUtils.EMPTY);
        suffix = context.getString(SUFFIX, StringUtils.EMPTY);

        ftpIp = context.getString("ftpIp");
        Preconditions.checkState(ftpIp != null, "Configuration must specify ftp id");

        ftpUserName = context.getString("ftpUserName");
        ftpUserPwd = context.getString("ftpUserPwd");
        ftpPort = context.getInteger("ftpPort", 21);

        extraRedisKey = context.getString("extraRedisKey", StringUtils.EMPTY);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Event event;
        Map<String, String> headers;

        try {
            List<CollectFile> files = listFiles();
            if (null == files || files.size() == 0) {
                logger.info("list file empty.");
                return Status.BACKOFF;
            }

            logger.info("list file size: " + files.size());
            for (CollectFile file : files) {
                boolean hasException = false;
                String fileName = file.getName();
                cacheClient.cacheReading(fileName);

                String day = getDay(fileName);

                ZipInputStream zis = null;
                BufferedReader reader = null;
                try {
                    zis = new ZipInputStream(ftpClient.retrieveFileStream(file.getAbsolutePath()),
                            READ_CHARSET);
                    reader = new BufferedReader(new InputStreamReader(zis, READ_CHARSET), bufSize);

                    ZipEntry entry = null;
                    while ((entry = zis.getNextEntry()) != null) {
                        String entryName = entry.getName();
                        logger.info("read entry: " + entryName);
                        IptvTypeEnum type = IptvTypeEnum.getTypeByFileName(entryName);

                        String line;
                        while ((line = reader.readLine()) != null) {
                            event = new SimpleEvent();
                            headers = new HashMap<String, String>();
                            headers.put("type", type.name());
                            headers.put("saveType", String.valueOf(type.getSaveType()));

                            String pathName = type.name();
                            if (type == IptvTypeEnum.PRODUCT_ORDER_ALL) {
                                pathName = "PRODUCT_ORDER_all";
                                headers.put("type", "PRODUCT_ORDER_all");
                            }

                            if (type.getSaveType() == 0) {
                                headers.put("path", pathName + "/" + day + "/" + entryName);
                            } else if (type.getSaveType() == 1) {
                                headers.put("path", pathName + "/" + entryName);
                            } else if (type.getSaveType() == 2) {
                                headers.put("path", pathName + "/current/" + entryName);
                            }

                            event.setHeaders(headers);
                            event.setBody(IptvEncrypt.encrypt(type, line).getBytes(WRITE_CHARSET));
                            eventsCounterMap.get(type).incr();

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
            logger.error("IptvZipSource EXCEPTION, {}", e);
            return Status.BACKOFF;
        }
    }

    private String getDay(String name) {
        int len = name.length();
        return name.substring(len - 12, len - 4);
    }

    private final static long delta = 14 * 24 * 3600 * 1000L;

    private List<CollectFile> listFiles() throws Exception {
        final long now = System.currentTimeMillis();
        return ftpClient.listFiles(spoolDirectory, new CollectFileFilter() {
            @Override
            public boolean accept(CollectFile file) {
                String fileName = file.getName();
                if (now - file.getTimeInMillis() > delta || file.isDirectory()
                        || fileName.startsWith(".") || !fileName.startsWith(prefix)
                        || !fileName.endsWith(suffix) ||
                // 已经被读过的文件
                (cacheClient.isReaded(fileName)) ||
                // 正在被处理的files
                (cacheClient.isReading(fileName)) ||
                // 已确认失败的文件
                cacheClient.isFailed(fileName)) {
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

        String cacheKey = ftpIp + "_iptv";
        cacheClient = CacheUtils.getCacheClient(cacheKey + extraRedisKey);

        ClientProps clientProps = new ClientProps(ftpIp, ClientType.FTP);
        clientProps.setIp(ftpIp);
        clientProps.setPort(ftpPort);
        clientProps.setUserName(ftpUserName);
        clientProps.setPassword(ftpUserPwd);
        clientProps.setPassiveMode(false);

        try {
            ftpClient = CollectClientFactory.createClient(clientProps);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        super.start();
        logger.info("IptvZipSource source started");
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
