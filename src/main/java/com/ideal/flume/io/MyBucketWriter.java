package com.ideal.flume.io;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.ideal.flume.clients.CollectClient;
import com.ideal.flume.tools.HostnameUtils;
import com.ideal.flume.tools.PidUtils;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyBucketWriter {
    private static final Logger LOG = LoggerFactory.getLogger(MyBucketWriter.class);

    /**
     * 用以确保打开文件时的线程安全。HADOOP在某些时候是非线程安全的.
     */
    private static final Object staticLock = new Object();

    private final MyYeeFileWriter redcamWriter;
    private long rollInterval;
    private final long rollSize;
    private final long rollLine;
    private final long batchSize;
    private final CompressionCodec codeC;
    private final ScheduledExecutorService timedRollerPool;

    private long processLines;
    private long processBytes;

    private final CollectClient fileClient;

    private volatile String tempPath;
    private volatile String filePath;
    private volatile String fileName;
    private volatile String inUseSuffix;
    private volatile String fileSuffix;
    private volatile String bucketPath;
    private volatile String targetPath;
    private volatile AtomicLong bytesCounter = new AtomicLong(0L);

    private AtomicReference<ScheduledFuture<Void>> timedRollFuture;
    private final WriterCallback onCloseCallback;
    private final String onCloseCallbackPath;

    /**
     * 有文件句柄打开着，由synchronized方法保证同步性.
     */
    private volatile boolean isOpen;
    /**
     * 当前对象是否关闭，文件超时关闭时同时标记对象已关闭，需重新实例化，由synchronized方法保证同步性.
     */
    protected volatile boolean closed;

    /**
     * 文件处理过程中（写入、关闭）是否有异常发生，如有，不做rename处理，由synchronized方法保证同步性.
     */
    private volatile boolean sthError;

    private long lastAppendTime = -1L;

    private final boolean useTimestamp;

    /**
     * 构造器.
     * 
     * @param fileClient fileClient
     * @param redcamWriter redcamWriter
     * @param rollInterval 回滚间隔（秒）
     * @param rollSize 回滚字节
     * @param rollLine 回滚行数
     * @param batchSize batchSize
     * @param tempPath 临时目录
     * @param filePath 最终目录
     * @param fileName 文件名前缀
     * @param inUseSuffix 临时后缀
     * @param fileSuffix 最终后缀
     * @param codeC 压缩格式
     * @param timedRollerPool 线程池
     * @param onCloseCallback 关闭时的回调函数
     * @param onCloseCallbackPath 回调函数参数
     */
    public MyBucketWriter(CollectClient fileClient, MyYeeFileWriter redcamWriter, long rollInterval,
            long rollSize, long rollLine, long batchSize, String tempPath, String filePath,
            String fileName, String inUseSuffix, String fileSuffix, CompressionCodec codeC,
            ScheduledExecutorService timedRollerPool, WriterCallback onCloseCallback,
            String onCloseCallbackPath, boolean useTimestamp) {
        this.rollInterval = rollInterval;
        this.rollSize = rollSize;
        this.rollLine = rollLine;
        this.batchSize = batchSize;
        this.tempPath = tempPath;
        this.filePath = filePath;
        this.fileName = fileName;
        this.inUseSuffix = inUseSuffix;
        this.fileSuffix = fileSuffix;
        this.codeC = codeC;
        this.redcamWriter = redcamWriter;
        this.timedRollerPool = timedRollerPool;
        this.onCloseCallback = onCloseCallback;
        this.onCloseCallbackPath = onCloseCallbackPath;
        this.useTimestamp = useTimestamp;

        this.fileClient = fileClient;

        isOpen = false;
        closed = false;
        sthError = false;
    }

    /**
     * Clear the class counters.
     */
    private void resetCounters() {
        processLines = 0;
        processBytes = 0;
        bytesCounter.set(0L);
    }

    /**
     * open() is called by append().
     */
    private void open() throws IOException, InterruptedException {
        if (null == tempPath || null == filePath || null == redcamWriter) {
            throw new IOException("Invalid file settings");
        }

        synchronized (staticLock) {
            try {
            	//定义文件名格式：数据命名_yyyyMMdd_HHmmss_采集机hostname_进程号
                String signName =new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())+"_"+HostnameUtils.getHostname()+"_"+PidUtils.PID+"_"+Thread.currentThread().getId();
                String fullFileName =useTimestamp ? (fileName + signName) : fileName;

                if (fileSuffix != null && fileSuffix.length() > 0) {
                    fullFileName += fileSuffix;
                    LOG.info("fullFileName fileSuffix : " + fullFileName);
                } else if (codeC != null) {
                    fullFileName += codeC.getDefaultExtension();
                }

                bucketPath = tempPath + fullFileName + inUseSuffix;
                targetPath = filePath + fullFileName;
                
                LOG.info("Creating " + bucketPath);
                if (codeC == null) {
                    redcamWriter.open(bucketPath);
                } else {
                    redcamWriter.open(bucketPath, codeC);
                }
            } catch (Exception ex) {
                if (ex instanceof IOException) {
                    throw (IOException) ex;
                } else {
                    throw Throwables.propagate(ex);
                }
            }
        }
        resetCounters();

        // if time-based rolling is enabled, schedule the roll
        if (rollInterval > 0) {
            // 如果rollInterval大于100万，则关闭间隔是相对的，相对于最后写入时间；否则是绝对的
            final AtomicBoolean relativeInterval = new AtomicBoolean(false);
            if (rollInterval > 1000000) {
                relativeInterval.set(true);
                rollInterval = rollInterval % 1000000;
            }
            Callable<Void> action = new Callable<Void>() {
                public Void call() throws Exception {
                    LOG.debug("Rolling file ({}): Roll scheduled after {} sec elapsed.", bucketPath,
                            rollInterval);
                    try {
                        if (relativeInterval.get()) {
                            if (System.currentTimeMillis() - lastAppendTime >= rollInterval
                                    * 1000L) {
                                close(true);
                            }
                        } else {
                            close(true);
                        }
                    } catch (Throwable th) {
                        sthError = true;
                        LOG.error("Unexpected error", th);
                    }
                    return null;
                }
            };
            timedRollFuture = new AtomicReference<ScheduledFuture<Void>>(
                    timedRollerPool.schedule(action, rollInterval, TimeUnit.SECONDS));
        }

        isOpen = true;
        sthError = false;
    }

    /**
     * 关闭文件.
     * 
     * @throws Exception 执行异常时抛出
     * @throws InterruptedException 执行异常时抛出
     */
    public synchronized void close() throws Exception, InterruptedException {
        close(false);
    }

    /**
     * 关闭文件.
     * 
     * @param callCloseCallback 是否需要关闭writer
     * @throws Exception 执行异常时抛出
     * @throws InterruptedException 执行异常时抛出
     */
    public synchronized void close(boolean callCloseCallback)
            throws Exception, InterruptedException {
        boolean failedToClose = false;
        LOG.info("Closing {}", bucketPath);
        if (isOpen) {
            try {
                LOG.info("Close tries incremented");
                redcamWriter.close();
            } catch (IOException ex) {
                LOG.warn("failed to close() HDFSWriter for file (" + bucketPath
                        + "). Exception follows.", ex);
                failedToClose = true;
                sthError = true;
            }
            isOpen = false;
        } else {
            failedToClose = true;
            LOG.info("HDFSWriter is already closed: {}", bucketPath);
        }

        // 定时关闭文件线程也会到此
        if (timedRollFuture.get() != null && !timedRollFuture.get().isDone()) {
            timedRollFuture.get().cancel(false); // for 定时关闭文件线程，所以不要cancel掉自己
            timedRollFuture.set(null);
        }

        // 没有正常关闭就不要重命名，让文件永远呆在临时目录
        if (bucketPath != null && fileClient != null && !failedToClose) {
            renameBucket(bucketPath, targetPath, fileClient);
        }
        if (callCloseCallback) {
            runCloseAction();
            closed = true;
        }
    }

    /**
     * 把输出流中的数据flush出去.
     * 
     * @throws Exception 流flush异常
     * @throws InterruptedException 同步执行异常
     */
    public synchronized void flush() throws Exception, InterruptedException {
        if (!isBatchComplete()) {
            redcamWriter.flush();
            bytesCounter.set(0L);
        }
    }

    private void runCloseAction() {
        try {
            if (onCloseCallback != null) {
                onCloseCallback.run(onCloseCallbackPath);
            }
        } catch (Throwable th) {
            LOG.error("Unexpected error", th);
        }
    }

    /**
     * 写入数据.
     *
     * @param event 一行数据的
     * @param combinedSize 如果做过批量提交，表示合并的event个数
     * @throws Exception 执行异常时抛出
     * @throws InterruptedException 执行异常时抛出
     */
    public synchronized void append(final Event event, int combinedSize)
            throws Exception, InterruptedException {
        // 文件超时被关闭了，重新打开，计数器重置
        if (!isOpen) {
            if (closed) {
                throw new FlumeException(
                        "This bucket writer was closed and this handle is thus no longer valid");
            }
            open();
        }

        // 文件是否需要回滚
        if (shouldRotate()) {
            close();
            open();
        }

        // 写入数据
        try {
            redcamWriter.append(event);
            lastAppendTime = System.currentTimeMillis();
        } catch (IOException ex) {
            sthError = true;
            LOG.warn("Caught IOException writing to HDFSWriter ({}). Closing file (" + bucketPath
                    + ") and rethrowing exception.", ex.getMessage());
            try {
                close(true);
            } catch (IOException e2) {
                LOG.warn("Caught IOException while closing file (" + bucketPath
                        + "). Exception follows.", e2);
            }
            throw ex;
        }

        // update statistics
        processBytes += event.getBody().length;
        processLines += combinedSize;

        if (bytesCounter.addAndGet(event.getBody().length) >= batchSize) {
            flush();
        }
    }

    /**
     * 文件是否需要回滚.
     */
    private boolean shouldRotate() {
        if (rollLine > 0 && rollLine <= processLines) {
            LOG.debug("rolling: rollCount: {}, events: {}", rollLine, processLines);
            return true;
        }

        if (rollSize > 0 && rollSize <= processBytes) {
            LOG.debug("rolling: rollSize: {}, bytes: {}", rollSize, processBytes);
            return true;
        }

        return false;
    }

    private void renameBucket(String bucketPath, String targetPath, final CollectClient fs)
            throws Exception, InterruptedException {
        if (sthError) {
            LOG.info("something is wrong before rename, do nothing here.");
            return;
        }

        if (bucketPath.equals(targetPath)) {
            return;
        }

        if (fs.exists(bucketPath)) {
            LOG.info("Renaming " + bucketPath + " to " + targetPath);
            LOG.info("rename result: " + fs.rename(bucketPath, targetPath));
        }
    }

    @Override
    public String toString() {
        return "[ " + this.getClass().getSimpleName() + " targetPath = " + targetPath
                + ", bucketPath = " + bucketPath + " ]";
    }

    private boolean isBatchComplete() {
        return bytesCounter.get() == 0;
    }
}
