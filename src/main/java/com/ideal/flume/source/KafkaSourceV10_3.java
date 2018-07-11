package com.ideal.flume.source;

import static com.ideal.flume.source.KafkaSourceConstants.BATCH_DURATION_MS;
import static com.ideal.flume.source.KafkaSourceConstants.BATCH_SIZE;
import static com.ideal.flume.source.KafkaSourceConstants.BOOTSTRAP_SERVERS;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_AUTO_COMMIT;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_BATCH_DURATION;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_BATCH_SIZE;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_GROUP_ID;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_KEY_DESERIALIZER;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_PARTITION_ASSIGNMENT;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER;
import static com.ideal.flume.source.KafkaSourceConstants.KAFKA_CONSUMER_PREFIX;
import static com.ideal.flume.source.KafkaSourceConstants.KAFKA_PREFIX;
import static com.ideal.flume.source.KafkaSourceConstants.KEY_HEADER;
import static com.ideal.flume.source.KafkaSourceConstants.PARTITION_HEADER;
import static com.ideal.flume.source.KafkaSourceConstants.TOPIC;
import static com.ideal.flume.source.KafkaSourceConstants.TOPIC_HEADER;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.HostnameUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceV10_3 extends AbstractSource implements Configurable, PollableSource {
  private static final Logger log = LoggerFactory.getLogger(KafkaSourceV10_3.class);

  private String bootstrapServers;
  private String groupId;
  private String topic;
  private int batchUpperLimit;
  private int timeUpperLimit;
  private Properties kafkaProps;

  private int sendThreadCount;
  private String hostname;

  private int readThreadCount;

  private StatCounter statCounter;

  private boolean interruped = false;

  private final List<KafkaConsumer<String, byte[]>> consumers = Lists.newArrayList();

  class GetRunnable implements Runnable {
    private KafkaConsumer<String, byte[]> consumer;
    private Iterator<ConsumerRecord<String, byte[]>> it;

    private final List<Event> eventList = Lists.newArrayList();

    private Map<TopicPartition, OffsetAndMetadata> tpAndOffsetMetadata = Maps.newHashMap();

    public GetRunnable() {
      try {
        consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
        consumers.add(consumer);
      } catch (Exception ex) {
        throw new FlumeException(
            "Unable to create consumer. " + "Check whether the ZooKeeper server is up and that the "
                + "Flume agent can connect to it.",
            ex);
      }

      try {
        consumer.subscribe(Arrays.asList(topic));
        it = consumer.poll(1000).iterator();
      } catch (Exception ex) {
        throw new FlumeException("Unable to get message iterator from Kafka", ex);
      }
    }

    @Override
    public void run() {
      while (!interruped) {
        final String batchUuid = UUID.randomUUID().toString();
        try {
          long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
          byte[] kafkaMessage;
          String kafkaKey;
          Map<String, String> headers;

          while (eventList.size() < batchUpperLimit && System.currentTimeMillis() < batchEndTime) {
            if (null == it || !it.hasNext()) {
              ConsumerRecords<String, byte[]> records =
                  consumer.poll(Math.max(0, batchEndTime - System.currentTimeMillis()));
              it = records.iterator();

              if (!it.hasNext()) {
                if (log.isDebugEnabled()) {
                  log.debug("Returning with backoff. No more data to read");
                }
                break;
              }
            }

            try {
              ConsumerRecord<String, byte[]> message = it.next();
              kafkaKey = message.key();
              kafkaMessage = message.value();

              if (null != kafkaMessage) {
                headers = Maps.newHashMap();
                headers.put(TOPIC_HEADER, message.topic());
                headers.put(PARTITION_HEADER, String.valueOf(message.partition()));
                headers.put("%{thread}", "" + ((int) (Math.random() * sendThreadCount)));
                headers.put("%{hostname}", hostname);
                if (null != kafkaKey) {
                  headers.put(KEY_HEADER, kafkaKey);
                }

                Event event = EventBuilder.withBody(kafkaMessage, headers);
                eventList.add(event);

                statCounter.incrEvent();
                statCounter.addByte(kafkaMessage.length);

                tpAndOffsetMetadata.put(new TopicPartition(message.topic(), message.partition()),
                    new OffsetAndMetadata(message.offset() + 1, batchUuid));
              }
            } catch (Exception ex) {
              log.error("", ex);
            }
          }

          if (eventList.size() > 0) {
            getChannelProcessor().processEventBatch(eventList);
            eventList.clear();

            if (!tpAndOffsetMetadata.isEmpty()) {
              consumer.commitSync(tpAndOffsetMetadata);
              tpAndOffsetMetadata.clear();
            }
          }
        } catch (Exception ex) {
          log.error("KafkaSource EXCEPTION, {}", ex);
        }
      }
    }

  }

  @Override
  public Status process() throws EventDeliveryException {
    return Status.BACKOFF;
  }

  @Override
  public synchronized void start() {
    log.info("Starting {}...", this);

    statCounter = StatCounters.create(StatType.SOURCE, topic);

    for (int i = 0; i < readThreadCount; i++) {
      new Thread(new GetRunnable(), "kafka-read-" + i).start();
    }

    log.info("Kafka source {} started.", getName());
    super.start();
  }

  @Override
  public synchronized void stop() {
    interruped = true;

    super.stop();

    for (KafkaConsumer<String, byte[]> consumer : consumers) {
      consumer.wakeup();
      consumer.close();
    }
  }

  @Override
  public void configure(Context context) {
    bootstrapServers = context.getString(BOOTSTRAP_SERVERS);
    Preconditions.checkState(StringUtils.isNotBlank(bootstrapServers),
        "Bootstrap Servers must be specified.");

    topic = context.getString(KAFKA_PREFIX + TOPIC);
    Preconditions.checkState(StringUtils.isNotBlank(topic), "Kafka topic must be specified.");

    groupId =
        context.getString(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);

    kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
    // Defaults overridden based on config
    kafkaProps.putAll(context.getSubProperties(KAFKA_CONSUMER_PREFIX));
    // These always take precedence over config
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, DEFAULT_AUTO_COMMIT);
    kafkaProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        DEFAULT_PARTITION_ASSIGNMENT);

    String truststorePath = context.getString(KAFKA_CONSUMER_PREFIX + "ssl.truststore.location");
    String truststorePassword =
        context.getString(KAFKA_CONSUMER_PREFIX + "ssl.truststore.password");
    String keystorePath = context.getString(KAFKA_CONSUMER_PREFIX + "ssl.keystore.location");
    String keystorePassword = context.getString(KAFKA_CONSUMER_PREFIX + "ssl.keystore.password");
    String keyPassword = context.getString(KAFKA_CONSUMER_PREFIX + "ssl.key.password");
    if (null != truststorePath && null != truststorePassword && null != keystorePath
        && null != keystorePassword && null != keyPassword) {
      // no-op
    } else {
      kafkaProps.remove("security.protocol");
      kafkaProps.remove("ssl.truststore.location");
      kafkaProps.remove("ssl.truststore.password");
      kafkaProps.remove("ssl.keystore.location");
      kafkaProps.remove("ssl.keystore.password");
      kafkaProps.remove("ssl.key.password");
    }

    this.batchUpperLimit = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    this.timeUpperLimit = context.getInteger(BATCH_DURATION_MS, DEFAULT_BATCH_DURATION); // ms
    sendThreadCount = context.getInteger("sendThreadCount", 4);
    hostname = HostnameUtils.getHostname();
    readThreadCount = context.getInteger("readThreadCount", 1);
  }

}
