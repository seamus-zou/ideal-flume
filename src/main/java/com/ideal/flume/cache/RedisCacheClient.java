package com.ideal.flume.cache;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ScanResult;

public class RedisCacheClient implements CacheClient {
  private final static Logger logger = LoggerFactory.getLogger(RedisCacheClient.class);

  private JedisCommands jCluster;
  private final String redisUrl;
  private final String redisPwd;
  /**
   * 文件读完后存入
   */
  private final String readedKey;
  /**
   * 文件正在被某个线程读取时存入，读完后从这里删除，存入readedKey，此key有效时间5分钟，每次存入刷新有效时间
   */
  private final String readingKey;
  /**
   * 文件读取失败后存入
   */
  private final String failedKey;
  /**
   * 文件读取commit时，保存文件的已读取行数
   */
  private final String readLineKey;

  public RedisCacheClient(String redisUrl, String redisPwd, String readedKey, String readingKey,
      String failedKey, String readLineKey) {
    this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
    this.redisUrl = redisUrl;
    this.redisPwd = redisPwd;
    this.readedKey = readedKey;
    this.readingKey = readingKey;
    this.failedKey = failedKey;
    this.readLineKey = readLineKey;
  }

  @Override
  public void cacheReaded(String fileName) throws Exception {
    try {
      jCluster.hset(readedKey, fileName, String.valueOf(System.currentTimeMillis()));
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      jCluster.hset(readedKey, fileName, String.valueOf(System.currentTimeMillis()));
    }
  }

  @Override
  public boolean cacheReading(String fileName) throws Exception {
    try {
      long i = jCluster.hsetnx(readingKey, fileName, String.valueOf(System.currentTimeMillis()));
      jCluster.expire(readingKey, 300);
      return 1 == i;
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      long i = jCluster.hsetnx(readingKey, fileName, String.valueOf(System.currentTimeMillis()));
      jCluster.expire(readingKey, 300);
      return 1 == i;
    }
  }



  @Override
  public int getFileReadedLines(String fileName) {
    try {
      String v = jCluster.hget(readLineKey, fileName);
      if (StringUtils.isNumeric(v)) {
        return Integer.parseInt(v);
      }
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      String v = jCluster.hget(readLineKey, fileName);
      if (StringUtils.isNumeric(v)) {
        return Integer.parseInt(v);
      }
    }
    return 0;
  }



  @Override
  public void cacheReadedLines(String fileName, int lines) {
    try {
      jCluster.hset(readLineKey, fileName, System.currentTimeMillis() + "," + lines);
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      jCluster.hset(readLineKey, fileName, System.currentTimeMillis() + "," + lines);
    }
  }

  @Override
  public void removeReading(String fileName) {
    try {
      jCluster.hdel(readingKey, fileName);
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      jCluster.hdel(readingKey, fileName);
    }
  }

  @Override
  public void cacheFailed(String fileName) throws Exception {
    try {
      jCluster.hset(failedKey, fileName, String.valueOf(System.currentTimeMillis()));
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      jCluster.hset(failedKey, fileName, String.valueOf(System.currentTimeMillis()));
    }
  }

  @Override
  public boolean isReaded(String fileName) {
    try {
      return null != jCluster.hget(readedKey, fileName);
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      return null != jCluster.hget(readedKey, fileName);
    }
  }

  @Override
  public boolean isReading(String fileName) {
    try {
      return null != jCluster.hget(readingKey, fileName);
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      return null != jCluster.hget(readingKey, fileName);
    }
  }

  @Override
  public boolean isFailed(String fileName) {
    try {
      return null != jCluster.hget(failedKey, fileName);
    } catch (Exception e) {
      this.jCluster = RedisClientUtils.getJedisCmd(redisUrl, redisPwd);
      return null != jCluster.hget(failedKey, fileName);
    }
  }

  @Override
  public void cleanReadedCache(long interval) {
    long now = System.currentTimeMillis();

    int s = 0;
    int cursor = 0;
    ScanResult<Map.Entry<String, String>> scanResult;
    List<Map.Entry<String, String>> scanList;
    do {
    	try {
			
//    	logger.info("===readedKey:"+readedKey);
      scanResult = jCluster.hscan(readedKey, String.valueOf(cursor));
      cursor = Integer.parseInt(scanResult.getStringCursor());
//      logger.info("===cursor:"+cursor);
      scanList = scanResult.getResult();
//      logger.info("===scanList:"+scanList.size());
      for (Map.Entry<String, String> m : scanList) {
        String key = m.getKey();
        String value = m.getValue();
//        logger.info("===key: "+key+" ===valuve: "+value);
        try {
          int i = value.indexOf(',');
          String ts;
          if (i > 0) {
            ts = value.substring(0, i);
          } else {
            ts = value;
          }

          if (now - Long.parseLong(ts) > interval) {
            jCluster.hdel(readedKey, key);
            s++;
          }
        } catch (Exception e) {
          jCluster.hdel(readedKey, key);
          s++;
        }
      }
      } catch (Exception e) {
    	  logger.error("=====报错了"+e+" :===readedKey: "+readedKey+" ; cursor: "+cursor);
    	  throw e;
		}
    	 logger.info("====================");
    } while (cursor > 0);
    logger.info("deleted {} redis readed cache. ", s);
  }

  @Override
  public void cleanReadingCache(long interval) {
    // no-op
    // readingKey自动超时，不用手动删除
  }

  @Override
  public void cleanFailedCache(long interval) {
    long now = System.currentTimeMillis();

    int s = 0;
    int cursor = 0;
    ScanResult<Map.Entry<String, String>> scanResult;
    List<Map.Entry<String, String>> scanList;
    do {
      scanResult = jCluster.hscan(failedKey, String.valueOf(cursor));
      cursor = Integer.parseInt(scanResult.getStringCursor());
      scanList = scanResult.getResult();
      for (Map.Entry<String, String> m : scanList) {
        String key = m.getKey();
        try {
          if (now - Long.parseLong(m.getValue()) > interval) {
            jCluster.hdel(failedKey, key);
            s++;
          }
        } catch (Exception e) {
          jCluster.hdel(failedKey, key);
          s++;
        }
      }
    } while (cursor > 0);
    logger.info("deleted {} redis failed cache. ", s);
  }



  @Override
  public void cleanReadedLineCache(long interval) {
    long now = System.currentTimeMillis();

    int s = 0;
    int cursor = 0;
    ScanResult<Map.Entry<String, String>> scanResult;
    List<Map.Entry<String, String>> scanList;
    do {
      scanResult = jCluster.hscan(readLineKey, String.valueOf(cursor));
      cursor = Integer.parseInt(scanResult.getStringCursor());
      scanList = scanResult.getResult();
      for (Map.Entry<String, String> m : scanList) {
        String key = m.getKey();
        String value = m.getValue();
        try {
          int i = value.indexOf(',');
          String ts;
          if (i > 0) {
            ts = value.substring(0, i);
          } else {
            ts = value;
          }

          if (now - Long.parseLong(ts) > interval) {
            jCluster.hdel(readLineKey, key);
            s++;
          }
        } catch (Exception e) {
          jCluster.hdel(readedKey, key);
          s++;
        }
      }
    } while (cursor > 0);
    logger.info("deleted {} redis readed line cache. ", s);
  }

  @Override
  public void cache(String key, String field, String value) {
    try {
      jCluster.hset(key, field, value);
    } catch (Exception e) {
      jCluster.hset(key, field, value);
    }
  }

  @Override
  public String get(String key, String field) {
    try {
      return jCluster.hget(key, field);
    } catch (Exception e) {
      return jCluster.hget(key, field);
    }
  }

}
