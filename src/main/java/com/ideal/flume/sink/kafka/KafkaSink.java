package com.ideal.flume.sink.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.stat.Counters.Counter;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

public class KafkaSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    private Properties kafkaProps;
    private String topic;
    private int batchSize;
    private int threadCount;

    private StatCounter statCounter;

    private List<KafkaProducer<String, byte[]>> producers = Lists.newArrayList();

    private boolean interrupted = false;

    private final LinkedBlockingQueue<ProducerRecord<String, byte[]>> msgQueue =
            new LinkedBlockingQueue<ProducerRecord<String, byte[]>>();
    private Counter msgQueueCounter;

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            int txnEventCount = 0;
            String eventTopic = null;
            String eventKey = null;
            for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
                Event event = channel.take();
                if (null == event) {
                    break;
                }

                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();

                if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
                    eventTopic = topic;
                }

                eventKey = headers.get(KEY_HDR);

                msgQueue.put(new ProducerRecord<String, byte[]>(topic, eventKey, eventBody));
                msgQueueCounter.incr();

                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
                            + new String(eventBody, "UTF-8"));
                    logger.debug("event #{}", txnEventCount);
                }

            }

            transaction.commit();

            if (txnEventCount == 0) {
                return Status.BACKOFF;
            } else {
                return Status.READY;
            }
        } catch (Throwable th) {
            transaction.rollback();
            logger.error("process failed", th);
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new FlumeException(th);
            }
        } finally {
            transaction.close();
        }

    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);
        super.start();

        msgQueueCounter = Counters.create("msgQueue", getName(), 1);

        for (int i = 0; i < threadCount; i++) {
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);
            Thread thread = new Thread(new SendRunnable(producer), "kafka-send-" + topic + "_" + i);
            thread.setDaemon(true);
            thread.start();
            producers.add(producer);
        }
    }

    @Override
    public synchronized void stop() {
        while (msgQueue.size() > 0) {
            logger.info("msg queue is not empty. sleep 5 seconds.");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (KafkaProducer<String, byte[]> producer : producers) {
            producer.close();
        }

        interrupted = true;

        super.stop();
        logger.info("kafka sink {} stopped.", getName());
    }

    @Override
    public void configure(Context context) {
        batchSize = 1000;
        logger.debug("Using batch size: {}", batchSize);

        topic = context.getString(KafkaSinkConstants.TOPIC, KafkaSinkConstants.DEFAULT_TOPIC);
        if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
            logger.warn("The Property 'topic' is not set. " + "Using the default topic name: "
                    + KafkaSinkConstants.DEFAULT_TOPIC);
        } else {
            logger.info("Using the static topic: " + topic
                    + " this may be over-ridden by event headers");
        }

        statCounter = StatCounters.create(StatType.SINK, getName() + "_" + topic);

        kafkaProps = getKafkaProperties(context);

        threadCount = context.getInteger("threadCount", 1);

        logger.info("Kafka producer properties: " + kafkaProps);
    }

    private Properties getKafkaProperties(Context context) {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "1");
//        props.put("compression.type", "gzip");

        Map<String, String> kafkaProperties =
                context.getSubProperties(KafkaSinkConstants.PROPERTY_PREFIX);
        for (Map.Entry<String, String> prop : kafkaProperties.entrySet()) {
            props.put(prop.getKey(), prop.getValue());
        }

        return props;
    }

    class SendRunnable implements Runnable {
        private final List<ProducerRecord<String, byte[]>> toSends =
                new ArrayList<ProducerRecord<String, byte[]>>(batchSize);
        private final KafkaProducer<String, byte[]> producer;

        public SendRunnable(KafkaProducer<String, byte[]> producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            while (!interrupted) {
                toSends.clear();
                int i = msgQueue.drainTo(toSends, batchSize);
                msgQueueCounter.add(-i);
                try {
                    for (ProducerRecord<String, byte[]> msg : toSends) {
                        producer.send(msg);
                        statCounter.incrEvent();
                        statCounter.addByte(msg.value().length);
                    }
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }
    }

}
