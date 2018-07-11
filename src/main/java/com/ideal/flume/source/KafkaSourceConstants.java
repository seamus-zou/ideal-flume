package com.ideal.flume.source;

import org.apache.kafka.clients.CommonClientConfigs;

public class KafkaSourceConstants {

    public static final String KAFKA_PREFIX = "kafka.";
    public static final String KAFKA_CONSUMER_PREFIX = KAFKA_PREFIX + "consumer.";
    public static final String DEFAULT_KEY_DESERIALIZER =
            "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String DEFAULT_VALUE_DESERIALIZER =
            "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String BOOTSTRAP_SERVERS =
            KAFKA_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    public static final String DEFAULT_AUTO_COMMIT = "false";
    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_DURATION_MS = "batchDurationMillis";
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final int DEFAULT_BATCH_DURATION = 1000;
    public static final String DEFAULT_GROUP_ID = "flume";

    public static final String MIGRATE_ZOOKEEPER_OFFSETS = "migrateZookeeperOffsets";
    public static final boolean DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS = true;

    public static final String AVRO_EVENT = "useFlumeEventFormat";
    public static final boolean DEFAULT_AVRO_EVENT = false;

    /* Old Properties */
    public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";
    public static final String TOPIC = "topic";
    public static final String OLD_GROUP_ID = "groupId";

    // flume event headers
    public static final String TOPIC_HEADER = "topic";
    public static final String KEY_HEADER = "key";
    public static final String TIMESTAMP_HEADER = "timestamp";
    public static final String PARTITION_HEADER = "partition";

    public static final String DEFAULT_PARTITION_ASSIGNMENT =
            "org.apache.kafka.clients.consumer.RoundRobinAssignor";

    public static final String TOPICS = "topics";
    public static final String TOPICS_REGEX = TOPICS + "." + "regex";

}
