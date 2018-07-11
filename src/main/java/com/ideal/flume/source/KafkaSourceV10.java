package com.ideal.flume.source;

import static com.ideal.flume.source.KafkaSourceConstants.BATCH_DURATION_MS;
import static com.ideal.flume.source.KafkaSourceConstants.BATCH_SIZE;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_BATCH_DURATION;
import static com.ideal.flume.source.KafkaSourceConstants.DEFAULT_BATCH_SIZE;
import static com.ideal.flume.source.KafkaSourceConstants.KAFKA_PREFIX;
import static com.ideal.flume.source.KafkaSourceConstants.KEY_HEADER;
import static com.ideal.flume.source.KafkaSourceConstants.PARTITION_HEADER;
import static com.ideal.flume.source.KafkaSourceConstants.TOPICS;
import static com.ideal.flume.source.KafkaSourceConstants.TOPICS_REGEX;
import static com.ideal.flume.source.KafkaSourceConstants.TOPIC_HEADER;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

public class KafkaSourceV10 extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger log = LoggerFactory.getLogger(KafkaSourceV10.class);

    private Properties kafkaProps;

    private int batchUpperLimit;
    private int timeUpperLimit;

    @SuppressWarnings("rawtypes")
    private Subscriber subscriber;

    private StatCounter statCounter;

    private int sendThreadCount;
    private int readThreadCount;

    private boolean interruped = false;

    private final List<KafkaConsumer<String, byte[]>> consumers = Lists.newArrayList();

    public abstract class Subscriber<T> {
        public abstract void subscribe(KafkaConsumer<?, ?> consumer,
                SourceRebalanceListener listener);

        public T get() {
            return null;
        }
    }

    private class TopicListSubscriber extends Subscriber<List<String>> {
        private List<String> topicList;

        public TopicListSubscriber(String commaSeparatedTopics) {
            this.topicList = Arrays.asList(commaSeparatedTopics.split("^\\s+|\\s*,\\s*|\\s+$"));
        }

        @Override
        public void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener) {
            consumer.subscribe(topicList, listener);
        }

        @Override
        public List<String> get() {
            return topicList;
        }
    }

    private class PatternSubscriber extends Subscriber<Pattern> {
        private Pattern pattern;

        public PatternSubscriber(String regex) {
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public void subscribe(KafkaConsumer<?, ?> consumer, SourceRebalanceListener listener) {
            consumer.subscribe(pattern, listener);
        }

        @Override
        public Pattern get() {
            return pattern;
        }
    }

    @Override
    public void configure(Context context) {
        kafkaProps = new Properties();
        kafkaProps.putAll(context.getSubProperties(KAFKA_PREFIX));
        
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put("enable.auto.commit", "true");

        String topicProperty = context.getString(TOPICS_REGEX);
        if (StringUtils.isNotBlank(topicProperty)) {
            subscriber = new PatternSubscriber(topicProperty);
        } else {
            topicProperty = context.getString(TOPICS);
            if (StringUtils.isNotBlank(topicProperty)) {
                subscriber = new TopicListSubscriber(topicProperty);
            }
        }

        if (null == subscriber) {
            throw new ConfigurationException("At least one Kafka topic must be specified.");
        }

        batchUpperLimit = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        timeUpperLimit = context.getInteger(BATCH_DURATION_MS, DEFAULT_BATCH_DURATION);

        sendThreadCount = context.getInteger("sendThreadCount", 4);
        readThreadCount = context.getInteger("readThreadCount", 1);
    }

    @Override
    public synchronized void start() {
        log.info("Starting {}...", this);

        statCounter = StatCounters.create(StatType.SOURCE, getName());

        for (int i = 0; i < readThreadCount; i++) {
            new Thread(new GetRunnable(), "kafka-read-" + i).start();
        }

        super.start();
        log.info("Kafka source {} started.", getName());
    }

    @Override
    public synchronized void stop() {
        interruped = true;

        super.stop();

        for (KafkaConsumer<String, byte[]> consumer : consumers) {
            consumer.wakeup();
            consumer.close();
        }
        log.info("Kafka Source {} stopped.", getName());
    }

    class GetRunnable implements Runnable {
        private KafkaConsumer<String, byte[]> consumer;
        private Iterator<ConsumerRecord<String, byte[]>> it;
        private AtomicBoolean rebalanceFlag;

        private final List<Event> eventList = Lists.newArrayList();

        @SuppressWarnings("unchecked")
        public GetRunnable() {
            try {
                consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
                consumers.add(consumer);
            } catch (Exception ex) {
                throw new FlumeException("Unable to create consumer. "
                        + "Check whether the ZooKeeper server is up and that the "
                        + "Flume agent can connect to it.", ex);
            }

            try {
                rebalanceFlag = new AtomicBoolean(false);
                subscriber.subscribe(consumer, new SourceRebalanceListener(rebalanceFlag));
                it = consumer.poll(1000).iterator();
            } catch (Exception ex) {
                throw new FlumeException("Unable to get message iterator from Kafka", ex);
            }
        }

        @Override
        public void run() {
            while (!interruped) {
                try {
                    long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
                    byte[] kafkaMessage;
                    String kafkaKey;
                    Map<String, String> headers;

                    while (eventList.size() < batchUpperLimit
                            && System.currentTimeMillis() < batchEndTime) {
                        if (null == it || !it.hasNext()) {
                            ConsumerRecords<String, byte[]> records = consumer
                                    .poll(Math.max(0, batchEndTime - System.currentTimeMillis()));
                            it = records.iterator();

                            if (rebalanceFlag.get()) {
                                rebalanceFlag.set(false);
                                break;
                            }

                            if (!it.hasNext()) {
                                break;
                            }
                        }

                        try {
                            ConsumerRecord<String, byte[]> message = it.next();
                            kafkaKey = message.key();
                            kafkaMessage = message.value();

                            if (null != kafkaMessage) {
                                headers = Maps.newHashMap();
                                headers.put("%{topic}", message.topic());
                                headers.put(PARTITION_HEADER, String.valueOf(message.partition()));
                                headers.put("%{thread}",
                                        String.valueOf((int) (Math.random() * sendThreadCount)));
                                if (null != kafkaKey) {
                                    headers.put(KEY_HEADER, kafkaKey);
                                }

                                Event event = EventBuilder.withBody(kafkaMessage, headers);
                                eventList.add(event);

                                statCounter.incrEvent();
                                statCounter.addByte(kafkaMessage.length);
                            }
                        } catch (Exception ex) {
                            log.error("", ex);
                        }
                    }

                    if (eventList.size() > 0) {
                        getChannelProcessor().processEventBatch(eventList);
                        eventList.clear();
                    }
                } catch (Exception ex) {
                    log.error("KafkaSource EXCEPTION, {}", ex);
                }
            }
        }

    }
}


class SourceRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(SourceRebalanceListener.class);
    private AtomicBoolean rebalanceFlag;

    public SourceRebalanceListener(AtomicBoolean rebalanceFlag) {
        this.rebalanceFlag = rebalanceFlag;
    }

    // Set a flag that a rebalance has occurred. Then commit already read events to kafka.
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
            rebalanceFlag.set(true);
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
        }
    }
}


