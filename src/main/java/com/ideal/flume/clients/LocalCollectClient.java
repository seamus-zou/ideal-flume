package com.ideal.flume.clients;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.ideal.flume.Constants;
import com.ideal.flume.file.CollectFile;
import com.ideal.flume.file.CollectFileFilter;
import com.ideal.flume.file.LocalCollectFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 所有方法的路径参数都是绝对路径
 * 
 * @author yukai
 *
 */
public class LocalCollectClient implements CollectClient {
  private static final Logger logger = LoggerFactory.getLogger(LocalCollectClient.class);

  @Override
  public boolean isConnected() throws Exception {
    return true;
  }

  @Override
  public void disconnect() throws Exception {
    // do nothing
  }

  @Override
  public List<CollectFile> listFiles(String pathname, final CollectFileFilter filter)
      throws Exception {
    return listFiles(pathname, filter, false);
  }

  @Override
  public List<CollectFile> listFiles(String pathname, final CollectFileFilter filter,
      final boolean recursive) throws Exception {
    final List<CollectFile> ret = new ArrayList<CollectFile>();
    File dir = new File(pathname);
    if (!dir.exists() || !dir.isDirectory()) {
      return ret;
    }

    dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        if (pathname.isFile()) {
          CollectFile f = new LocalCollectFile(pathname);
          if (filter.accept(f)) {
            ret.add(f);
          }
        } else if (pathname.isDirectory() && recursive) {
          try {
            ret.addAll(listFiles(pathname.getAbsolutePath(), filter, true));
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
    new File(relPath).delete();
  }

  @Override
  public boolean rename(String oldName, String newName) throws Exception {
    File target = new File(newName);
    if (target.exists()) {
      target.delete();
    }

    Path dst = new Path(newName);
    Path dstParent = dst.getParent();
    if (null == dstParent || !mkdirs(dstParent)) {
      throw new RuntimeException("cann not creat dirs " + dstParent.toUri().getPath());
    }

    return new File(oldName).renameTo(target);
  }

  @Override
  public InputStream retrieveFileStream(String relPath) throws Exception {
    return new FileInputStream(relPath);
  }



  @Override
  public OutputStream create(String relPath) throws Exception {
    Path file = new Path(relPath);
    Preconditions.checkState(file.isAbsolute(), "The file must be in an absolute path.");

    Path parent = file.getParent();
    if (null == parent || !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent);
    }
    return new BufferedOutputStream(new FileOutputStream(pathToFile(file)),
        Constants.DEFAULT_BUFFER_SIZE);
  }

  public boolean mkdirs(Path file) throws Exception {
    Preconditions.checkState(file.isAbsolute(), "The file must be in an absolute path.");

    File p2f = pathToFile(file);
    if (!p2f.exists()) {
      return p2f.mkdirs();
    } else if (!p2f.isDirectory()) {
      throw new PathExistsException(String.format(
          "Can't make directory for path %s since it is existed and is not a directory.", file));
    }
    return true;
  }

  private File pathToFile(Path path) {
    return new File(path.toUri().getPath());
  }

  @Override
  public boolean exists(String relPath) throws Exception {
    return new File(relPath).exists();
  }

  @Override
  public boolean abort() throws Exception {
    return true;
  }

  @Override
  public void completePendingCommand() throws Exception {
    // do nothing
  }

  @Override
  public boolean mkdirs(String path) throws Exception {
    return mkdirs(new Path(path));
  }

}
