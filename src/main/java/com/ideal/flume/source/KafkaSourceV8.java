package com.ideal.flume.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;
import com.ideal.flume.tools.HostnameUtils;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.kafka.KafkaSourceConstants;
import org.apache.flume.source.kafka.KafkaSourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaSourceV8 extends AbstractSource implements Configurable, PollableSource {
  private static final Logger log = LoggerFactory.getLogger(KafkaSourceV8.class);
  private ConsumerConnector consumer;
  private ConsumerIterator<byte[], byte[]> it;
  private String topic;
  private int batchUpperLimit;
  private int timeUpperLimit;
  private int consumerTimeout;
  private boolean kafkaAutoCommitEnabled;
  private Context context;
  private Properties kafkaProps;
  private final List<Event> eventList = new ArrayList<Event>();
  private int sendThreadCount;

  private String hostname;

  private StatCounter statCounter;

  public Status process() throws EventDeliveryException {

    byte[] bytes;
    Event event;
    Map<String, String> headers;
    long batchStartTime = System.currentTimeMillis();
    long batchEndTime = System.currentTimeMillis() + timeUpperLimit;
    try {
      boolean iterStatus = false;
      while (eventList.size() < batchUpperLimit && System.currentTimeMillis() < batchEndTime) {
        iterStatus = hasNext();
        if (iterStatus) {
          // get next message
          bytes = it.next().message();

          headers = new HashMap<String, String>();
          headers.put(KafkaSourceConstants.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
          headers.put(KafkaSourceConstants.TOPIC, topic);
          if (log.isDebugEnabled()) {
            log.debug("Message: {}", new String(bytes));
          }

          headers.put("%{thread}", String.valueOf(bytes.length % sendThreadCount));
          headers.put("%{hostname}", hostname);

          event = EventBuilder.withBody(bytes, headers);
          eventList.add(event);

          statCounter.incrEvent();
          statCounter.addByte(bytes.length);
        }
        if (log.isDebugEnabled()) {
          log.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
          log.debug("Event #: {}", eventList.size());
        }
      }

      // If we have events, send events to channel
      // clear the event list
      // and commit if Kafka doesn't auto-commit
      if (eventList.size() > 0) {
        getChannelProcessor().processEventBatch(eventList);
        eventList.clear();
        if (log.isDebugEnabled()) {
          log.debug("Wrote {} events to channel", eventList.size());
        }
        if (!kafkaAutoCommitEnabled) {
          // commit the read transactions to Kafka to avoid duplicates
          consumer.commitOffsets();
        }
      }
      if (!iterStatus) {
        if (log.isDebugEnabled()) {
          log.debug("Returning with backoff. No more data to read");
        }
        return Status.BACKOFF;
      }
      return Status.READY;
    } catch (Exception e) {
      log.error("KafkaSource EXCEPTION, {}", e);
      return Status.BACKOFF;
    }
  }

  /**
   * We configure the source and generate properties for the Kafka Consumer
   *
   * Kafka Consumer properties are generated as follows: 1. Generate a properties object with some
   * static defaults that can be overridden by Source configuration 2. We add the configuration
   * users added for Kafka (parameters starting with kafka. and must be valid Kafka Consumer
   * properties 3. We add the source documented parameters which can override other properties
   *
   * @param context
   */
  public void configure(Context context) {
    this.context = context;
    batchUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_SIZE,
        KafkaSourceConstants.DEFAULT_BATCH_SIZE);
    timeUpperLimit = context.getInteger(KafkaSourceConstants.BATCH_DURATION_MS,
        KafkaSourceConstants.DEFAULT_BATCH_DURATION);
    topic = context.getString(KafkaSourceConstants.TOPIC);

    if (topic == null) {
      throw new ConfigurationException("Kafka topic must be specified.");
    }

    kafkaProps = KafkaSourceUtil.getKafkaProperties(context);
    consumerTimeout =
        Integer.parseInt(kafkaProps.getProperty(KafkaSourceConstants.CONSUMER_TIMEOUT));
    kafkaAutoCommitEnabled =
        Boolean.parseBoolean(kafkaProps.getProperty(KafkaSourceConstants.AUTO_COMMIT_ENABLED));
    sendThreadCount = context.getInteger("sendThreadCount", 4);
    hostname = HostnameUtils.getHostname();
  }

  @Override
  public synchronized void start() {
    log.info("Starting {}...", this);

    try {
      // initialize a consumer. This creates the connection to ZooKeeper
      consumer = KafkaSourceUtil.getConsumer(kafkaProps);
    } catch (Exception e) {
      throw new FlumeException(
          "Unable to create consumer. " + "Check whether the ZooKeeper server is up and that the "
              + "Flume agent can connect to it.",
          e);
    }

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    // We always have just one topic being read by one thread
    topicCountMap.put(topic, 1);

    // Get the message iterator for our topic
    // Note that this succeeds even if the topic doesn't exist
    // in that case we simply get no messages for the topic
    // Also note that currently we only support a single topic
    try {
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
          consumer.createMessageStreams(topicCountMap);
      List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(topic);
      KafkaStream<byte[], byte[]> stream = topicList.get(0);
      it = stream.iterator();
    } catch (Exception e) {
      throw new FlumeException("Unable to get message iterator from Kafka", e);
    }

    statCounter = StatCounters.create(StatType.SOURCE, getName());
    log.info("Kafka source {} started.", getName());
    super.start();
  }

  @Override
  public synchronized void stop() {
    if (consumer != null) {
      // exit cleanly. This syncs offsets of messages read to ZooKeeper
      // to avoid reading the same messages again
      consumer.shutdown();
    }
    super.stop();
  }

  /**
   * Check if there are messages waiting in Kafka, waiting until timeout (10ms by default) for
   * messages to arrive. and catching the timeout exception to return a boolean
   */
  boolean hasNext() {
    try {
      it.hasNext();
      return true;
    } catch (ConsumerTimeoutException e) {
      return false;
    }
  }

}
