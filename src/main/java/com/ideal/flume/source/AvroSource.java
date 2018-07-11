package com.ideal.flume.source;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ipfilter.IpFilterRule;
import org.jboss.netty.handler.ipfilter.IpFilterRuleHandler;
import org.jboss.netty.handler.ipfilter.PatternRule;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSource extends AbstractSource
    implements EventDrivenSource, Configurable, AvroSourceProtocol {

  private static final String THREADS = "threads";

  private static final Logger logger = LoggerFactory.getLogger(AvroSource.class);

  private static final String PORT_KEY = "port";
  private static final String BIND_KEY = "bind";
  private static final String COMPRESSION_TYPE = "compression-type";
  private static final String SSL_KEY = "ssl";
  private static final String IP_FILTER_KEY = "ipFilter";
  private static final String IP_FILTER_RULES_KEY = "ipFilterRules";
  private static final String KEYSTORE_KEY = "keystore";
  private static final String KEYSTORE_PASSWORD_KEY = "keystore-password";
  private static final String KEYSTORE_TYPE_KEY = "keystore-type";
  private int port;
  private String bindAddress;
  private String compressionType;
  private String keystore;
  private String keystorePassword;
  private String keystoreType;
  private boolean enableSsl = false;
  private boolean enableIpFilter;
  private String patternRuleConfigDefinition;

  private Server server;

  private int maxThreads;
  private ScheduledExecutorService connectionCountUpdater;

  private List<IpFilterRule> rules;

  private StatCounter statCounter;

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(context, PORT_KEY, BIND_KEY);

    port = context.getInteger(PORT_KEY);
    bindAddress = context.getString(BIND_KEY);
    compressionType = context.getString(COMPRESSION_TYPE, "none");

    try {
      maxThreads = context.getInteger(THREADS, 0);
    } catch (NumberFormatException e) {
      logger.warn("AVRO source\'s \"threads\" property must specify an integer value.",
          context.getString(THREADS));
    }

    enableSsl = context.getBoolean(SSL_KEY, false);
    keystore = context.getString(KEYSTORE_KEY);
    keystorePassword = context.getString(KEYSTORE_PASSWORD_KEY);
    keystoreType = context.getString(KEYSTORE_TYPE_KEY, "JKS");

    if (enableSsl) {
      Preconditions.checkNotNull(keystore, KEYSTORE_KEY + " must be specified when SSL is enabled");
      Preconditions.checkNotNull(keystorePassword,
          KEYSTORE_PASSWORD_KEY + " must be specified when SSL is enabled");
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());
      } catch (Exception ex) {
        throw new FlumeException("Avro source configured with invalid keystore: " + keystore, ex);
      }
    }

    enableIpFilter = context.getBoolean(IP_FILTER_KEY, false);
    if (enableIpFilter) {
      patternRuleConfigDefinition = context.getString(IP_FILTER_RULES_KEY);
      if (patternRuleConfigDefinition == null || patternRuleConfigDefinition.trim().isEmpty()) {
        throw new FlumeException(
            "ipFilter is configured with true but ipFilterRules is not defined:" + " ");
      }
      String[] patternRuleDefinitions = patternRuleConfigDefinition.split(",");
      rules = new ArrayList<IpFilterRule>(patternRuleDefinitions.length);
      for (String patternRuleDefinition : patternRuleDefinitions) {
        rules.add(generateRule(patternRuleDefinition));
      }
    }
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);
    Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);

    NioServerSocketChannelFactory socketChannelFactory = initSocketChannelFactory();

    ChannelPipelineFactory pipelineFactory = initChannelPipelineFactory();

    server = new NettyServer(responder, new InetSocketAddress(bindAddress, port),
        socketChannelFactory, pipelineFactory, null);

    server.start();
    super.start();

    statCounter = StatCounters.create(StatType.SOURCE, bindAddress + ":" + port);

    logger.info("Avro source {} started.", getName());
  }

  private NioServerSocketChannelFactory initSocketChannelFactory() {
    NioServerSocketChannelFactory socketChannelFactory;
    if (maxThreads <= 0) {
      socketChannelFactory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(new ThreadFactoryBuilder()
              .setNameFormat("Avro " + NettyTransceiver.class.getSimpleName() + " Boss-%d")
              .build()),
          Executors.newCachedThreadPool(new ThreadFactoryBuilder()
              .setNameFormat("Avro " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d")
              .build()));
    } else {
      socketChannelFactory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(new ThreadFactoryBuilder()
              .setNameFormat("Avro " + NettyTransceiver.class.getSimpleName() + " Boss-%d")
              .build()),
          Executors.newFixedThreadPool(maxThreads,
              new ThreadFactoryBuilder()
                  .setNameFormat(
                      "Avro " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d")
                  .build()));
    }
    return socketChannelFactory;
  }

  private ChannelPipelineFactory initChannelPipelineFactory() {
    ChannelPipelineFactory pipelineFactory;
    boolean enableCompression = compressionType.equalsIgnoreCase("deflate");
    if (enableCompression || enableSsl || enableIpFilter) {
      pipelineFactory = new AdvancedChannelPipelineFactory(enableCompression, enableSsl, keystore,
          keystorePassword, keystoreType, enableIpFilter, patternRuleConfigDefinition);
    } else {
      pipelineFactory = new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline();
        }
      };
    }
    return pipelineFactory;
  }

  @Override
  public void stop() {
    logger.info("Avro source {} stopping: {}", getName(), this);

    server.close();

    try {
      server.join();
    } catch (InterruptedException e) {
      logger.info("Avro source " + getName() + ": Interrupted while waiting "
          + "for Avro server to stop. Exiting. Exception follows.", e);
    }
    connectionCountUpdater.shutdown();
    while (!connectionCountUpdater.isTerminated()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        logger.error("Interrupted while waiting for connection count executor " + "to terminate",
            ex);
        Throwables.propagate(ex);
      }
    }
    super.stop();
    logger.info("Avro source {} stopped.", getName());
  }

  @Override
  public String toString() {
    return "Avro source " + getName() + ": { bindAddress: " + bindAddress + ", port: " + port
        + " }";
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  @Override
  public Status append(AvroFlumeEvent avroEvent) {
    logger.debug("Avro source {}: Received avro event: {}", getName(), avroEvent);

    Event event =
        EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));

    statCounter.incrEvent();
    statCounter.addByte(event.getBody().length);

    try {
      getChannelProcessor().processEvent(event);
    } catch (ChannelException ex) {
      logger.warn("Avro source " + getName() + ": Unable to process event. " + "Exception follows.",
          ex);
      return Status.FAILED;
    }

    return Status.OK;
  }

  @Override
  public Status appendBatch(List<AvroFlumeEvent> events) {
    logger.debug("Avro source {}: Received avro event batch of {} events.", getName(),
        events.size());

    List<Event> batch = new ArrayList<Event>();
    for (AvroFlumeEvent avroEvent : events) {
      Event event =
          EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));
      batch.add(event);

      statCounter.incrEvent();
      statCounter.addByte(event.getBody().length);
    }

    try {
      getChannelProcessor().processEventBatch(batch);
    } catch (Throwable t) {
      logger.error(
          "Avro source " + getName() + ": Unable to process event " + "batch. Exception follows.",
          t);
      if (t instanceof Error) {
        throw (Error) t;
      }
      return Status.FAILED;
    }

    return Status.OK;
  }

  private PatternRule generateRule(String patternRuleDefinition) throws FlumeException {
    patternRuleDefinition = patternRuleDefinition.trim();
    // first validate the format
    int firstColonIndex = patternRuleDefinition.indexOf(":");
    if (firstColonIndex == -1) {
      throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition
          + "' should look like <'allow'  or 'deny'>:<'ip' or " + "'name'>:<pattern>");
    } else {
      String ruleAccessFlag = patternRuleDefinition.substring(0, firstColonIndex);
      int secondColonIndex = patternRuleDefinition.indexOf(":", firstColonIndex + 1);
      if ((!ruleAccessFlag.equals("allow") && !ruleAccessFlag.equals("deny"))
          || secondColonIndex == -1) {
        throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition
            + "' should look like <'allow'  or 'deny'>:<'ip' or " + "'name'>:<pattern>");
      }

      String patternTypeFlag =
          patternRuleDefinition.substring(firstColonIndex + 1, secondColonIndex);
      if ((!patternTypeFlag.equals("ip") && !patternTypeFlag.equals("name"))) {
        throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition
            + "' should look like <'allow'  or 'deny'>:<'ip' or " + "'name'>:<pattern>");
      }

      boolean isAllow = ruleAccessFlag.equals("allow");
      String patternRuleString = (patternTypeFlag.equals("ip") ? "i" : "n") + ":"
          + patternRuleDefinition.substring(secondColonIndex + 1);
      logger.info(
          "Adding ipFilter PatternRule: " + (isAllow ? "Allow" : "deny") + " " + patternRuleString);
      return new PatternRule(isAllow, patternRuleString);
    }
  }

  /**
   * Factory of SSL-enabled server worker channel pipelines Copied from Avro's
   * org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private class AdvancedChannelPipelineFactory implements ChannelPipelineFactory {

    private boolean enableCompression;
    private boolean enableSsl;
    private String keystore;
    private String keystorePassword;
    private String keystoreType;

    private boolean enableIpFilter;
    private String patternRuleConfigDefinition;

    public AdvancedChannelPipelineFactory(boolean enableCompression, boolean enableSsl,
        String keystore, String keystorePassword, String keystoreType, boolean enableIpFilter,
        String patternRuleConfigDefinition) {
      this.enableCompression = enableCompression;
      this.enableSsl = enableSsl;
      this.keystore = keystore;
      this.keystorePassword = keystorePassword;
      this.keystoreType = keystoreType;
      this.enableIpFilter = enableIpFilter;
      this.patternRuleConfigDefinition = patternRuleConfigDefinition;
    }

    private SSLContext createServerSSLContext() {
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());

        // Set up key manager factory to use our key store
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(getAlgorithm());
        kmf.init(ks, keystorePassword.toCharArray());

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);
        return serverContext;
      } catch (Exception e) {
        throw new Error("Failed to initialize the server-side SSLContext", e);
      }
    }

    private String getAlgorithm() {
      String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
      if (algorithm == null) {
        algorithm = "SunX509";
      }
      return algorithm;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      if (enableCompression) {
        ZlibEncoder encoder = new ZlibEncoder(6);
        pipeline.addFirst("deflater", encoder);
        pipeline.addFirst("inflater", new ZlibDecoder());
      }


      if (enableSsl) {
        SSLEngine sslEngine = createServerSSLContext().createSSLEngine();
        sslEngine.setUseClientMode(false);
        // addFirst() will make SSL handling the first stage of decoding
        // and the last stage of encoding this must be added after
        // adding compression handling above
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
      }

      if (enableIpFilter) {

        logger.info("Setting up ipFilter with the following rule definition: "
            + patternRuleConfigDefinition);
        IpFilterRuleHandler ipFilterHandler = new IpFilterRuleHandler();
        ipFilterHandler.addAll(rules);
        logger.info("Adding ipFilter with " + ipFilterHandler.size() + " rules");

        pipeline.addFirst("ipFilter", ipFilterHandler);
      }

      return pipeline;
    }
  }
}
