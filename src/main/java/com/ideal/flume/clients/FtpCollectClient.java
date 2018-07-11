package com.ideal.flume.clients;

import static com.ideal.flume.Constants.DEFAULT_BUFFER_SIZE;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.ftp.FTPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.file.FtpCollectFile;

public class FtpCollectClient implements CollectClient {

    private Logger logger = LoggerFactory.getLogger(FtpCollectClient.class);

    private final ClientProps clientProps;
    private FTPClient ftpClient;

    private final ExecutorService callTimeoutPool = Executors.newSingleThreadExecutor();
    private static final long callTimeout = 5000L;

    public FtpCollectClient(ClientProps clientProps) throws Exception {
        this.clientProps = clientProps;
        getFtpClient();
    }

    private FTPClient getFtpClient() throws Exception {
        if (ftpClient == null) {
            ftpClient = createClient();
        }

        return ftpClient;
    }

    private FTPClient createClient() throws Exception {
        try {
            FTPClient client = new FTPClient();
            try {
                String ip = clientProps.getIp();
                client.connect(ip, clientProps.getPort());
                int reply = client.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    throw new Exception("Connection to FTP server on \"" + ip + "\" rejected.");
                }

                // Login
                if (!client.login(clientProps.getUserName(), clientProps.getPassword())) {
                    throw new Exception("Could not login to FTP server on \"" + ip + "\" as user \""
                            + clientProps.getUserName() + "\".");
                }

                // Set binary mode
                if (!client.setFileType(FTP.BINARY_FILE_TYPE)) {
                    throw new Exception("Could not switch to binary transfer mode. " + ip);
                }

                // times out
                if (null != clientProps.getDefaultTimeout()) {
                    client.setDefaultTimeout(clientProps.getDefaultTimeout());
                }

                if (null != clientProps.getConnectTimeout()) {
                    client.setConnectTimeout(clientProps.getConnectTimeout());
                }

                if (null != clientProps.getDataTimeout()) {
                    client.setDataTimeout(clientProps.getDataTimeout());
                }

                if (null != clientProps.getSoTimeout()) {
                    client.setSoTimeout(clientProps.getSoTimeout());
                }

                if (StringUtils.isNotBlank(clientProps.getWorkingDirectory())) {
                    if (!client.changeWorkingDirectory(clientProps.getWorkingDirectory())) {
                        throw new Exception("Could not change to work directory \""
                                + clientProps.getWorkingDirectory() + "\".");
                    }
                }

                if (clientProps.isPassiveMode()) {
                    client.enterLocalPassiveMode();
                }
                
                if (!clientProps.isRemoteverification()) {
                    client.setRemoteVerificationEnabled(false);
                }

                String controlEncoding = clientProps.getControlEncoding();
                if (StringUtils.isNotBlank(controlEncoding)) {
                    client.setControlEncoding(controlEncoding);
                }
            } catch (final Exception e) {
                if (client.isConnected()) {
                    client.disconnect();
                }
                throw e;
            }

            return client;
        } catch (final Exception exc) {
            throw new Exception(
                    "Could not connect to FTP server on \"" + clientProps.getIp() + "\".", exc);
        }
    }

    @Override
    public boolean isConnected() throws Exception {
        return ftpClient != null && ftpClient.isConnected();
    }

    @Override
    public void disconnect() throws Exception {
        try {
            getFtpClient().disconnect();
        } finally {
            ftpClient = null;
        }
    }

    @Override
    public List<CollectFile> listFiles(String pathname, CollectFileFilter filter) throws Exception {
        return listFiles(pathname, filter, false);
    }



    @Override
    public List<CollectFile> listFiles(String pathname, CollectFileFilter filter, boolean recursive)
            throws Exception {
        try {
            return doListFiles(getFtpClient(), pathname, filter, recursive);
        } catch (Exception e) {
            disconnect();

            return doListFiles(getFtpClient(), pathname, filter, recursive);
        }
    }

    private List<CollectFile> doListFiles(final FTPClient client, final String pathname,
            final CollectFileFilter filter, final boolean recursive) throws Exception {
        final List<CollectFile> ret = new ArrayList<CollectFile>();
        logger.debug("ftp client : " + client + ",pathname:" + pathname);
        client.listFiles(pathname, new FTPFileFilter() {
            public boolean accept(FTPFile file) {
                logger.debug("ftp file : " + file.getName());
                if (file.isFile()) {
                    CollectFile cf = new FtpCollectFile(file, pathname);
                    if (filter.accept(cf)) {
                        ret.add(cf);
                        logger.debug("accept ftp file:" + file.getName());
                    }
                } else if (file.isDirectory() && recursive) {
                    try {
                        ret.addAll(doListFiles(client, pathname + file.getName(), filter, true));
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }
                return false;
            }
        });

        return ret;
    }

    @Override
    public void deleteFile(String relPath) throws Exception {
        try {
            getFtpClient().deleteFile(relPath);
        } catch (Exception e) {
            disconnect();
            getFtpClient().deleteFile(relPath);
        }
    }

    @Override
    public boolean rename(String oldName, String newName) throws Exception {
        Path dst = new Path(newName);
        Path dstParent = dst.getParent();
        if (null == dstParent || !mkdirs(getFtpClient(), dstParent)) {
            throw new RuntimeException("cann not creat dirs " + dstParent.toUri().getPath());
        }

        try {
            return getFtpClient().rename(oldName, newName);
        } catch (Exception e) {
            disconnect();
            return getFtpClient().rename(oldName, newName);
        }
    }

    @Override
    public InputStream retrieveFileStream(String relPath) throws Exception {
        try {
            return getFtpClient().retrieveFileStream(relPath);
        } catch (IOException e) {
            disconnect();
            return getFtpClient().retrieveFileStream(relPath);
        }
    }

    @Override
    public OutputStream create(String relPath) throws Exception {
        Path file = new Path(relPath);
        Preconditions.checkState(file.isAbsolute(), "The file must be in an absolute path.");

        final FTPClient client = getFtpClient();
        Path parent = file.getParent();
        if (null == parent || !mkdirs(client, parent)) {
            throw new IOException("create(): Mkdirs failed to create: " + parent);
        }

        client.allocate(DEFAULT_BUFFER_SIZE);
        OutputStream fos = new FilterOutputStream(client.storeFileStream(file.toUri().getPath())) {
            @Override
            public void close() throws IOException {
                super.close();
                if (!client.isConnected()) {
                    throw new FTPException("Client not connected");
                }
                if (!client.completePendingCommand()) {
                    throw new FTPException(
                            "Could not complete transfer, Reply Code - " + client.getReplyCode());
                }
            }
        };

        if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
            fos.close();
            throw new IOException("Unable to create file: " + file + ", Aborting");
        }
        return fos;
    }

    /**
     * 创建目录时，只要是绝对路径，workingDirectory就没影响.
     */
    private boolean mkdirs(FTPClient client, Path file) throws IOException {
        boolean created = true;
        FileStatus fstatus = getFileStatus(client, file);
        if (null == fstatus) {
            Path parent = file.getParent();
            created = null == parent || mkdirs(client, parent);
            if (created) {
                created = client.makeDirectory(file.toUri().getPath());
            }
        } else if (fstatus.isFile()) {
            throw new PathExistsException(
                    String.format("Can't make directory for path %s since it is a file.", file));
        }
        return created;
    }

    private FileStatus getFileStatus(FTPClient client, final Path file) throws IOException {
        Path parentPath = file.getParent();
        if (null == parentPath) {
            // 当前已经是根目录
            long length = -1;
            boolean isDir = true;
            int blockReplication = 1;
            long blockSize = DEFAULT_BUFFER_SIZE;
            long modTime = -1;
            Path root = new Path("/");
            return new FileStatus(length, isDir, blockReplication, blockSize, modTime, root);
        }

        FTPFile[] ftpFiles = client.listFiles(parentPath.toUri().getPath(), new FTPFileFilter() {
            @Override
            public boolean accept(FTPFile ftpFile) {
                return ftpFile.getName().equals(file.getName());
            }
        });
        if (ArrayUtils.isNotEmpty(ftpFiles)) {
            return getFileStatus(ftpFiles[0], parentPath);
        }
        return null;
    }

    private FileStatus getFileStatus(FTPFile ftpFile, Path parentPath) {
        long length = ftpFile.getSize();
        boolean isDir = ftpFile.isDirectory();
        int blockReplication = 1;
        long blockSize = DEFAULT_BUFFER_SIZE;
        long modTime = ftpFile.getTimestamp().getTimeInMillis();
        Path filePath = new Path(parentPath, ftpFile.getName());
        return new FileStatus(length, isDir, blockReplication, blockSize, modTime, filePath);
    }

    @Override
    public boolean exists(String relPath) throws Exception {
        try {
            return ArrayUtils.isNotEmpty(getFtpClient().listFiles(relPath));
        } catch (IOException e) {
            disconnect();
            return ArrayUtils.isNotEmpty(getFtpClient().listFiles(relPath));
        }
    }

    @Override
    public boolean abort() throws Exception {
        try {
            // imario@apache.org: 2005-02-14
            // it should be better to really "abort" the transfer, but
            // currently I didnt manage to make it work - so lets "abort" the
            // hard way.
            // return getFtpClient().abort();

            disconnect();
            return true;
        } catch (IOException e) {
            disconnect();
        }
        return true;
    }

    @Override
    public void completePendingCommand() throws Exception {
        try {
            if (ftpClient != null) {
                callWithTimeout(new CallRunner<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return ftpClient.completePendingCommand();
                    }
                });
            }
        } catch (IOException e) {
            disconnect();
            getFtpClient();
        }
    }

    @Override
    public boolean mkdirs(String path) throws Exception {
        return mkdirs(getFtpClient(), new Path(path));
    }

    @SuppressWarnings("unused")
    private <T> T callWithTimeout(final CallRunner<T> callRunner)
            throws IOException, InterruptedException {
        Future<T> future = callTimeoutPool.submit(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return callRunner.call();
            }
        });
        try {
            if (callTimeout > 0) {
                return future.get(callTimeout, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (TimeoutException eT) {
            future.cancel(true);
            throw new IOException("Callable timed out after " + callTimeout + " ms");
        } catch (ExecutionException e1) {
            Throwable cause = e1.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw new RuntimeException(e1);
            }
        } catch (CancellationException ce) {
            throw new InterruptedException("Blocked callable interrupted by rotation event");
        } catch (InterruptedException ex) {
            logger.warn("Unexpected Exception " + ex.getMessage(), ex);
            throw ex;
        }
    }

    private interface CallRunner<T> {
        T call() throws Exception;
    }

}
