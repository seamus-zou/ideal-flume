package com.ideal.flume.clients;

import static com.ideal.flume.Constants.DEFAULT_BUFFER_SIZE;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.file.SftpCollectFile;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpCollectClient implements CollectClient {
  private static final Logger logger = LoggerFactory.getLogger(SftpCollectClient.class);
  private final ClientProps clientProps;
  private Session sftpSession;
  private ChannelSftp channelSftp;

  public SftpCollectClient(final ClientProps clientProps) throws Exception {
    this.clientProps = clientProps;
    getSftpClient(); // fail-fast
  }

  private ChannelSftp getSftpClient() throws Exception {
    if (channelSftp == null) {
      channelSftp = createClient();
    }

    // channelSftp.cd(clientProps.getWorkingDirectory());
    return channelSftp;
  }

  private ChannelSftp createClient() throws Exception {
    JSch jsch = new JSch(); // 创建JSch对象
    sftpSession =
        jsch.getSession(clientProps.getUserName(), clientProps.getIp(), clientProps.getPort()); // 根据用户名，主机ip，端口获取一个Session对象
    sftpSession.setPassword(clientProps.getPassword()); // 设置密码

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");
    //ssh连接时让本本地的jsch支持hellman-group-exchange-sha256
//    config.put("kex", "diffie-hellman-group1-sha1,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group-exchange-sha256");
//    config.put("kex", "KexAlgorithms curve25519-sha256@libssh.org,ecdh-sha2-nistp256,ecdh-sha2-nistp384,ecdh-sha2-nistp521,diffie-hellman-group-exchange-sha256,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group1-sha1"); 
    
    sftpSession.setConfig(config); // 为Session对象设置properties

    // times out
    if (null != clientProps.getDefaultTimeout()) {
      sftpSession.setTimeout(clientProps.getDefaultTimeout());
    }

    if (null != clientProps.getSoTimeout()) {
      sftpSession.setTimeout(clientProps.getSoTimeout());
    }

    sftpSession.connect(); // 通过Session建立链接

    Channel channel = sftpSession.openChannel("sftp"); // 打开SFTP通道
    channel.connect(); // 建立SFTP通道的连接
    return (ChannelSftp) channel;
  }

  @Override
  public boolean isConnected() throws Exception {
    return sftpSession != null && sftpSession.isConnected() && channelSftp != null
        && channelSftp.isConnected();
  }

  @Override
  public void disconnect() throws Exception {
    try {
      if (null != channelSftp)
        channelSftp.disconnect();

      if (null != sftpSession)
        sftpSession.disconnect();
    } finally {
      channelSftp = null;
      sftpSession = null;
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
      return doListFiles(getSftpClient(), pathname, filter, recursive);
    } catch (Exception e) {
      disconnect();

      return doListFiles(getSftpClient(), pathname, filter, recursive);
    }
  }

  private List<CollectFile> doListFiles(final ChannelSftp client, final String pathname,
      final CollectFileFilter filter, final boolean recursive) throws Exception {
    final List<CollectFile> ret = new ArrayList<CollectFile>();
    client.ls(pathname, new LsEntrySelector() {
      @Override
      public int select(LsEntry entry) {
        if (entry.getAttrs().isDir()) {
          if (recursive) {
            try {
              ret.addAll(doListFiles(client, pathname + entry.getFilename(), filter, true));
            } catch (Exception e) {
              logger.error("", e);
            }
          }
        } else {
          CollectFile cf = new SftpCollectFile(entry, pathname);
          if (filter.accept(cf)) {
            ret.add(cf);
          }
        }

        return CONTINUE;
      }
    });

    return ret;
  }

  @Override
  public void deleteFile(String relPath) throws Exception {
    try {
      getSftpClient().rm(relPath);
    } catch (Exception e) {
      disconnect();

      getSftpClient().rm(relPath);
    }
  }

  @Override
  public boolean rename(String oldName, String newName) throws Exception {
    Path dst = new Path(newName);
    Path dstParent = dst.getParent();
    if (null == dstParent || !mkdirs(getSftpClient(), dstParent)) {
      throw new RuntimeException("cann not creat dirs " + dstParent.toUri().getPath());
    }

    try {
      getSftpClient().rename(oldName, newName);
    } catch (Exception e) {
      disconnect();

      getSftpClient().rename(oldName, newName);
    }
    return true;
  }

  @Override
  public InputStream retrieveFileStream(String relPath) throws Exception {
    try {
      return getSftpClient().get(relPath);
    } catch (Exception e) {
      disconnect();

      return getSftpClient().get(relPath);
    }
  }



  @Override
  public OutputStream create(String relPath) throws Exception {
    Path file = new Path(relPath);
    Preconditions.checkState(file.isAbsolute(), "The file must be in an absolute path.");

    final ChannelSftp channel = getSftpClient();

    Path parent = file.getParent();
    if (null == parent || !mkdirs(channel, parent)) {
      throw new IOException("create(): Mkdirs failed to create: " + parent);
    }

    OutputStream fos = new FilterOutputStream(channel.put(file.toUri().getPath())) {
      @Override
      public void close() throws IOException {
        super.close();
        if (!channel.isConnected()) {
          throw new IOException("Channel not connected");
        }
      }
    };
    return fos;
  }

  private boolean mkdirs(ChannelSftp channel, Path file) throws Exception {
    boolean created = true;
    FileStatus fstatus = getFileStatus(channel, file);
    if (null == fstatus) {
      Path parent = file.getParent();
      created = null == parent || mkdirs(channel, parent);
      if (created) {
        channel.mkdir(file.toUri().getPath());
      }
    } else if (fstatus.isFile()) {
      throw new PathExistsException(
          String.format("Can't make directory for path %s since it is a file.", file));
    }
    return created;
  }

  private FileStatus getFileStatus(ChannelSftp channel, final Path file) throws Exception {
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

    final List<LsEntry> files = Lists.newArrayList();
    channel.ls(parentPath.toUri().getPath(), new LsEntrySelector() {

      @Override
      public int select(LsEntry entry) {
        if (entry.getFilename().equals(file.getName())) {
          files.add(entry);
        }
        return CONTINUE;
      }
    });

    if (files.size() > 0) {
      return getFileStatus(files.get(0), parentPath);
    }
    return null;
  }

  private FileStatus getFileStatus(LsEntry sftpFile, Path parentPath) {
    long length = sftpFile.getAttrs().getSize();
    boolean isDir = sftpFile.getAttrs().isDir();
    int blockReplication = 1;
    long blockSize = DEFAULT_BUFFER_SIZE;
    long modTime = sftpFile.getAttrs().getMTime() * 1000L;
    Path filePath = new Path(parentPath, sftpFile.getFilename());
    return new FileStatus(length, isDir, blockReplication, blockSize, modTime, filePath);
  }

  @Override
  public boolean exists(String relPath) throws Exception {
    try {
      return CollectionUtils.isNotEmpty(getSftpClient().ls(relPath));
    } catch (IOException e) {
      disconnect();
      return CollectionUtils.isNotEmpty(getSftpClient().ls(relPath));
    }
  }

  @Override
  public boolean abort() throws Exception {
    try {
      disconnect();
      return true;
    } catch (Exception e) {
      disconnect();
    }
    return true;
  }

  @Override
  public void completePendingCommand() throws Exception {
    // do nothing
  }

  @Override
  public boolean mkdirs(String path) throws Exception {
    return mkdirs(getSftpClient(), new Path(path));
  }

}
