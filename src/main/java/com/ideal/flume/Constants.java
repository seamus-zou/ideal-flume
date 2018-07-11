package com.ideal.flume;

import org.apache.kafka.clients.CommonClientConfigs;

public final class Constants {
  public static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  public static final String READED_TMP_DIR = TMP_DIR + ".redcam/.readed/";
  public static final String READING_TMP_DIR = TMP_DIR + ".redcam/.reading/";
  public static final String FAILED_TMP_DIR = TMP_DIR + ".redcam/.failed/";

  public static final String TIMESTAMP = "timestamp";

  public static final String KEY_IP = "ip";
  public static final String KEY_PORT = "port";
  public static final String KEY_USERNAME = "username";
  public static final String KEY_PASSWORD = "password";
  public static final String KEY_WORKINGDIR = "workingdir";
  public static final String KEY_PASSIVE = "passive";

  public static final String TOPIC = "topic";
  public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION_MS = "batchDurationMillis";
  public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";
  public static final String GROUP_ID = "group.id";
  public static final String GROUP_ID_REDCAM = "groupId";
  public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
  public static final String ZOOKEEPER_CONNECT_REDCAM = "zookeeperConnect";
  public static final String KAFKA_PREFIX = "kafka.";
  
  public static final int DEFAULT_WRITER_THREADS = 1;

  public static final String KAFKA_CONSUMER_PREFIX = KAFKA_PREFIX + "consumer.";
  public static final String DEFAULT_KEY_DESERIALIZER =
      "org.apache.kafka.common.serialization.StringDeserializer";
  public static final String DEFAULT_VALUE_DESERIALIZER =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  public static final String BOOTSTRAP_SERVERS = KAFKA_PREFIX
      + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String DEFAULT_PARTITION_ASSIGNMENT =
      "org.apache.kafka.clients.consumer.RoundRobinAssignor";
  
  public static final String KAFKA_PRODUCER_PREFIX = KAFKA_PREFIX + "producer.";
  public static final String DEFAULT_ACKS = "1";
  public static final String DEFAULT_KEY_SERIALIZER =
      "org.apache.kafka.common.serialization.StringSerializer";
  public static final String DEFAULT_VALUE_SERIAIZER =
      "org.apache.kafka.common.serialization.ByteArraySerializer";

  public static final String TOPIC_HEADER = "topic";
  public static final String KEY_HEADER = "key";
  public static final String PARTITION_HEADER = "partition";

  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int DEFAULT_BATCH_DURATION = 1000;
  public static final String DEFAULT_AUTO_COMMIT = "false";
  public static final String DEFAULT_CONSUMER_TIMEOUT = "10";
  public static final String DEFAULT_GROUP_ID = "redcam";

  public static final String FILE_CFG_PATH = "path";
  public static final String FILE_CFG_TMP_PATH = "tmpPath";
  public static final String FILE_CFG_PREFIX = "prefix";
  public static final String FILE_CFG_SUFFIX = "suffix";
  public static final String FILE_CFG_FINISH_POLICY = "finishPolicy";
  public static final String FILE_CFG_DAYS_PATTERN = "daysPattern";
  public static final String FILE_CFG_HOURS_PATTERN = "hoursPattern";
  public static final String FILE_CFG_CODEC = "codec";
  public static final String FILE_CFG_INUSE_SUFFIX = "inUseSuffix";
  public static final String FILE_CFG_MAX_OPEN_FILES = "maxOpenFiles";
  public static final String FILE_CFG_ROLLTIMER_POOL_SIZE = "rollTimerPoolSize";
  public static final String FILE_CFG_USE_LOCAL_TIME = "useLocalTimeStamp";

  public static final String FILE_CFG_ROLL_SIZE = "rollSize";
  public static final String FILE_CFG_ROLL_INTERVAL = "rollInterval";
  public static final String FILE_CFG_ROLL_LINE = "rollLine";

  public static final String CFG_TYPE = "type";
  public static final String CACHE_CFG_SUBDIR = "subDir";
  public static final String CACHE_CFG_URL = "url";
  public static final String CACHE_CFG_KEY_PREFIX = "keyPrefix";
  public static final String CACHE_CFG_FINISHED_KEY = "finishedKey";
  public static final String CACHE_PROPERTY_PREFIX = "cache.";

  /* for file system */
  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final int DEFAULT_BLOCK_SIZE = 32 * 1024 * 1024;

  public static final String FS_HOST = "fs.host";
  public static final String FS_PORT = "fs.port";
  public static final String FS_USER = "fs.user";
  public static final String FS_PASSWORD = "fs.password";
  public static final String FS_PASSIVE = "fs.passive";
  public static final String FS_PATH = "fs.path";
  /**
   * hdfs专用，core-site.xml、hdfs-site.xml所在目录的绝对路径.
   */
  public static final String FS_HDFS_CONFDIR = "fs.confdir";
  /**
   * hdfs专用，keytab文件或者票据缓存的绝对路径.
   */
  public static final String FS_HDFS_KEYTAB = "fs.keytab";

  public static final int FS_SFTP_DEFAULT_PORT = 22;

  public static final String IO_FILE_BUFFER_SIZE_KEY = "io.file.buffer.size";
  public static final String FS_LOCAL_BLOCK_SIZE_KEY = "fs.local.block.size";

  public static final String DIRECTORY_DELIMITER = System.getProperty("file.separator");

  public static final byte LINE_SEPARATOR = '\n';

  /* for file system */
  
  public static void main(String[] args) {
    System.out.println(TMP_DIR);
  }
}
