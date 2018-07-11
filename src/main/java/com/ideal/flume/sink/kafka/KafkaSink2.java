/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. limitations under the License.
 */

package com.ideal.flume.sink.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Throwables;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * A Flume Sink that can publish messages to Kafka. This is a general implementation that can be
 * used with any Flume agent and a channel. The message can be any event and the key is a string
 * that we read from the header For use of partitioning, use an interceptor to generate a header
 * with the partition key
 * <p/>
 * Mandatory properties are: brokerList -- can be a partial list, but at least 2 are recommended for
 * HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the Kafka producer Read the
 * Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties topic - there's a default, and also - this can be in the event header if you
 * need to support events with different topics batchSize - how many messages to process in one
 * batch. Larger batches improve throughput while adding latency. requiredAcks -- 0 (unsafe), 1
 * (accepted by at least one broker, default), -1 (accepted by all brokers)
 * <p/>
 * header properties (per event): topic key
 */
public class KafkaSink2 extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSink2.class);
  public static final String KEY_HDR = "key";
  public static final String TOPIC_HDR = "topic";
  private Properties kafkaProps;
  private String topic;
  private int batchSize;
  private int threadCount;

  private StatCounter statCounter;

  private final static Map<String, LinkedBlockingQueue<KeyedMessage<String, byte[]>>> msgQueueMap =
      new ConcurrentHashMap<String, LinkedBlockingQueue<KeyedMessage<String, byte[]>>>();
  private static Map<String, Long> startSendMap = new ConcurrentHashMap<String, Long>();
  private static Map<String, Boolean> startedSendMap = new ConcurrentHashMap<String, Boolean>();
  private List<Producer<String, byte[]>> producers = new ArrayList<Producer<String, byte[]>>();

  class SendRunnable implements Runnable {
    private final List<KeyedMessage<String, byte[]>> toSends =
        new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
    private final Producer<String, byte[]> producer;
    private final String topic;

    public SendRunnable(String topic) {
      this.topic = topic;
      ProducerConfig config = new ProducerConfig(kafkaProps);
      producer = new Producer<String, byte[]>(config);
      producers.add(producer);
    }

    @Override
    public void run() {
      while (true) {
        LinkedBlockingQueue<KeyedMessage<String, byte[]>> msgQueue = msgQueueMap.get(topic);
        if (null != msgQueue && msgQueue.size() > 0) {
          toSends.clear();
          msgQueue.drainTo(toSends, batchSize);
          try {
            producer.send(toSends);
            for (KeyedMessage<String, byte[]> msg : toSends) {
              statCounter.incrEvent();
              statCounter.addByte(msg.message().length);
            }
          } catch (Exception e) {
            logger.error("", e);
          }
        } else {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  private void initSendTask(String topic) {
    Boolean b = startedSendMap.get(topic);
    if (null != b && b) {
      return;
    }

    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread(new SendRunnable(topic), "kafka-send-" + topic + "_" + i);
      thread.setDaemon(true);
      thread.start();
    }

    startedSendMap.put(topic, true);
    startSendMap.put(topic, System.currentTimeMillis());
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = null;
    Event event = null;
    String eventTopic = null;
    String eventKey = null;
    Long charactersSize = 0l;
    try {
      long processedEvents = 0;

      transaction = channel.getTransaction();
      transaction.begin();
      for (; processedEvents < batchSize; processedEvents += 1) {
        event = channel.take();

        if (event == null) {
          // no events available in channel
          break;
        }

        byte[] eventBody = event.getBody();
        Map<String, String> headers = event.getHeaders();

        if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
          eventTopic = topic;
        }

        initSendTask(eventTopic);

        eventKey = headers.get(KEY_HDR);

        if (logger.isDebugEnabled()) {
          logger.debug(
              "{Event} " + eventTopic + " : " + eventKey + " : " + new String(eventBody, "UTF-8"));
          logger.debug("event #{}", processedEvents);
        }

        // create a message and add to buffer
        KeyedMessage<String, byte[]> data =
            new KeyedMessage<String, byte[]>(eventTopic, eventKey, eventBody);
        LinkedBlockingQueue<KeyedMessage<String, byte[]>> msgQue = msgQueueMap.get(eventTopic);
        if (null == msgQue) {
          msgQue = new LinkedBlockingQueue<KeyedMessage<String, byte[]>>(500000);
          msgQueueMap.put(eventTopic, msgQue);
        }
        msgQue.put(data);
        charactersSize += eventBody.length;
      }

      transaction.commit();
    } catch (Exception ex) {
      String errorMsg = "Failed to publish events";
      logger.error("Failed to publish events", ex);
      result = Status.BACKOFF;
      if (transaction != null) {
        try {
          transaction.rollback();
        } catch (Exception e) {
          logger.error("Transaction rollback failed", e);
          throw Throwables.propagate(e);
        }
      }
      throw new EventDeliveryException(errorMsg, ex);
    } finally {
      if (transaction != null) {
        transaction.close();
      }
    }

    return result;
  }

  @Override
  public synchronized void start() {
    logger.info("Starting {}...", this);
    super.start();
  }

  @Override
  public synchronized void stop() {
    for (Producer<String, byte[]> producer : producers) {
      producer.close();
    }

    super.stop();
    logger.info("kafka sink {} stopped.", getName());
  }

  /**
   * We configure the sink and generate properties for the Kafka Producer
   * <p/>
   * Kafka producer properties is generated as follows: 1. We generate a properties object with some
   * static defaults that can be overridden by Sink configuration 2. We add the configuration users
   * added for Kafka (parameters starting with .kafka. and must be valid Kafka Producer properties
   * 3. We add the sink's documented parameters which can override other properties
   *
   * @param context
   */
  @Override
  public void configure(Context context) {
    batchSize = 300;
    logger.debug("Using batch size: {}", batchSize);

    topic = context.getString(KafkaSinkConstants.TOPIC, KafkaSinkConstants.DEFAULT_TOPIC);
    if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
      logger.warn("The Property 'topic' is not set. " + "Using the default topic name: "
          + KafkaSinkConstants.DEFAULT_TOPIC);
    } else {
      logger.info("Using the static topic: " + topic + " this may be over-ridden by event headers");
    }

    statCounter = StatCounters.create(StatType.SINK, topic);

    kafkaProps = KafkaSinkUtil.getKafkaProperties(context);

    if (logger.isDebugEnabled()) {
      logger.debug("Kafka producer properties: " + kafkaProps);
    }

    this.threadCount = context.getInteger("threadCount", 1);
  }

}
