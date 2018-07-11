package com.ideal.flume.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.ideal.flume.source.YeeSpoolDirectorySourceConfigurationConstants.*;

public class LocalCacheClient implements CacheClient {
  private final static Logger logger = LoggerFactory.getLogger(LocalCacheClient.class);

  private final String readedDir;
  private final String readingDir;
  private final String failedDir;

  public LocalCacheClient() {
    this.readedDir = READED_TMP_FILE;
    File dir = new File(readedDir);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IllegalArgumentException("can not make cache tmp dir.");
      }
    }

    this.readingDir = READING_TMP_FILE;
    dir = new File(readingDir);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IllegalArgumentException("can not make cache tmp dir.");
      }
    }

    this.failedDir = FAILED_TMP_FILE;
    dir = new File(failedDir);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IllegalArgumentException("can not make cache tmp dir.");
      }
    }
  }

  @Override
  public void cacheReaded(String fileName) throws Exception {
    new File(readedDir + fileName).createNewFile();
  }

  @Override
  public boolean cacheReading(String fileName) throws Exception {
    File f = new File(readingDir + fileName);
    if (f.exists()) {
      f.delete();
    }

    return f.createNewFile();
  }


  /**
   * 暂不支持已读行数的缓存
   * 
   * @see com.ideal.flume.cache.CacheClient#getFileReadedLines(java.lang.String)
   */
  @Override
  public int getFileReadedLines(String fileName) {
    return 0;
  }


  /**
   * 暂不支持已读行数的缓存
   * 
   * @see com.ideal.flume.cache.CacheClient#getFileReadedLines(java.lang.String)
   */
  @Override
  public void cacheReadedLines(String fileName, int lines) {
    // no-op
  }

  @Override
  public void removeReading(String fileName) {
    new File(readingDir + fileName).delete();
  }

  @Override
  public void cacheFailed(String fileName) throws Exception {
    new File(failedDir + fileName).createNewFile();
  }

  @Override
  public boolean isReaded(String fileName) {
    return new File(readedDir + fileName).exists();
  }

  @Override
  public boolean isReading(String fileName) {
    return new File(readingDir + fileName).exists();
  }

  @Override
  public boolean isFailed(String fileName) {
    return new File(failedDir + fileName).exists();
  }

  @Override
  public void cleanReadedCache(long interval) {
    long now = System.currentTimeMillis();

    File[] fs = new File(readedDir).listFiles();
    int s = 0;
    if (null != fs && fs.length > 0) {
      for (File f : fs) {
        if (now - f.lastModified() > interval) {
          if (f.delete()) {
            s++;
          }
        }
      }
    }
    logger.info("deleted {} local readed cache.", s);
  }

  @Override
  public void cleanReadingCache(long interval) {
    long now = System.currentTimeMillis();

    File[] fs = new File(readingDir).listFiles();
    int s = 0;
    if (null != fs && fs.length > 0) {
      for (File f : fs) {
        if (now - f.lastModified() > interval) {
          if (f.delete()) {
            s++;
          }
        }
      }
    }
    logger.info("deleted {} local reading cache.", s);
  }

  @Override
  public void cleanFailedCache(long interval) {
    long now = System.currentTimeMillis();

    File[] fs = new File(failedDir).listFiles();
    int s = 0;
    if (null != fs && fs.length > 0) {
      for (File f : fs) {
        if (now - f.lastModified() > interval) {
          if (f.delete()) {
            s++;
          }
        }
      }
    }
    logger.info("deleted {} local failed cache.", s);
  }

  @Override
  public void cleanReadedLineCache(long interval) {
    // no-op
  }

  @Override
  public void cache(String key, String field, String value) {
    throw new RuntimeException("Unsupported function.");
  }

  @Override
  public String get(String key, String field) {
    throw new RuntimeException("Unsupported function.");
  }

}
