package com.ideal.flume.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheUtils {
  private static final Logger logger = LoggerFactory.getLogger(CacheUtils.class);

  private static String redisPwd = null;
  private static String redisUrl = null;

  private static final Lock LOCK = new ReentrantLock();

  private static final AtomicBoolean SETTED = new AtomicBoolean(false);

  /**
   *
   * @param redisUrl redisPwd$$$redisurl
   */
  public static void setRedis(String url) {
    LOCK.lock();
    try {
      if (SETTED.get()) {
        logger.info("cache has been setted redisurl.");
        return;
      }

      if (StringUtils.isBlank(url)) {
        return;
      }

      String[] arr = url.split("\\$\\$\\$");
      if (arr.length != 2) {
        throw new IllegalArgumentException("error redisUrl. e.g. $$$redisPwd$$$redisurl");
      }

      redisPwd = StringUtils.trimToNull(arr[0]);
      redisUrl = arr[1];
    } finally {
      LOCK.unlock();
    }
  }

  public static CacheClient getCacheClient(String keyPrefix) {
    if (null == redisUrl) {
      return new LocalCacheClient();
    } else {
      return new RedisCacheClient(redisUrl, redisPwd, keyPrefix, keyPrefix + "_reading",
          keyPrefix + "_failed", keyPrefix + "_readline");
    }
  }

  public static void main(String[] args) {
    String s = "1$$$redisurl";
    String[] arr = s.split("\\$\\$\\$");
    System.out.println(arr.length);

    System.out.println(arr[0]);
    System.out.println(arr[1]);
  }
}
