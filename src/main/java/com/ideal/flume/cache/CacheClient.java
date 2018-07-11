package com.ideal.flume.cache;

public interface CacheClient {
  void cacheReaded(String fileName) throws Exception;

  boolean cacheReading(String fileName) throws Exception;

  void removeReading(String fileName);

  void cacheFailed(String fileName) throws Exception;

  boolean isReaded(String fileName);

  boolean isReading(String fileName);

  boolean isFailed(String fileName);

  void cleanReadedCache(long interval);

  void cleanReadingCache(long interval);

  void cleanFailedCache(long interval);
  
  void cleanReadedLineCache(long interval);

  int getFileReadedLines(String fileName);

  void cacheReadedLines(String fileName, int lines);

  void cache(String key, String field, String value);

  String get(String key, String field);
}
