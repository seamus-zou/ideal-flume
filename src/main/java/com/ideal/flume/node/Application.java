/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.ideal.flume.node;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.Channel;
import org.apache.flume.Constants;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.MaterializedConfiguration;
import org.apache.flume.node.PollingPropertiesFileConfigurationProvider;
import org.apache.flume.node.PropertiesFileConfigurationProvider;
import org.apache.flume.node.StaticZooKeeperConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.ideal.flume.cache.CacheUtils;
import com.ideal.flume.lifecycle.LifecycleSupervisor;
import com.ideal.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import com.ideal.flume.stat.Counters;
import com.ideal.flume.stat.MongoStatWriter;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.ZeromqStatWriter;
import com.ideal.flume.tools.DynamicEnum;
import com.ideal.flume.tools.ZmqEmbeddedLibraryTools;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";
    public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

    private final List<LifecycleAware> components;
    private final LifecycleSupervisor supervisor;
    private MaterializedConfiguration materializedConfiguration;
    private MonitorService monitorServer;
    private final String startTime;

    private static String agentName;
    private static int agentId;

    public Application() {
        this(new ArrayList<LifecycleAware>(0));
    }

    public Application(List<LifecycleAware> components) {
        this.components = components;
        supervisor = new LifecycleSupervisor();
        this.startTime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
    }

    public synchronized void start() {
        logger.info("Application start method begin...");
        for (LifecycleAware component : components) {
            logger.info("component:" + component);
            supervisor.supervise(component, new SupervisorPolicy.AlwaysRestartPolicy(),
                    LifecycleState.START);
        }

        Counters.startPrint(startTime);
        logger.info("Application start method end.");

    }

    @Subscribe
    public synchronized void handleConfigurationEvent(MaterializedConfiguration conf) {
        stopAllComponents();
        startAllComponents(conf);
    }

    public synchronized void stop() {
        supervisor.stop();
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    private void stopAllComponents() {
        if (this.materializedConfiguration != null) {
            logger.info("Shutting down configuration: {}", this.materializedConfiguration);
            for (Entry<String, SourceRunner> entry : this.materializedConfiguration
                    .getSourceRunners().entrySet()) {
                try {
                    logger.info("Stopping Source " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, SinkRunner> entry : this.materializedConfiguration.getSinkRunners()
                    .entrySet()) {
                try {
                    logger.info("Stopping Sink " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, Channel> entry : this.materializedConfiguration.getChannels()
                    .entrySet()) {
                try {
                    logger.info("Stopping Channel " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }
        }
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
        logger.info("Starting new configuration:{}", materializedConfiguration);

        this.materializedConfiguration = materializedConfiguration;

        for (Entry<String, Channel> entry : materializedConfiguration.getChannels().entrySet()) {
            try {
                logger.info("Starting Channel " + entry.getKey());
                supervisor.supervise(entry.getValue(), new SupervisorPolicy.AlwaysRestartPolicy(),
                        LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        /*
         * Wait for all channels to start.
         */
        for (Channel ch : materializedConfiguration.getChannels().values()) {
            while (ch.getLifecycleState() != LifecycleState.START
                    && !supervisor.isComponentInErrorState(ch)) {
                try {
                    logger.info("Waiting for channel: " + ch.getName()
                            + " to start. Sleeping for 500 ms");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for channel to start.", e);
                    Throwables.propagate(e);
                }
            }
        }

        for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners()
                .entrySet()) {
            try {
                logger.info("Starting Sink " + entry.getKey());
                initSinksZk(entry.getValue());
                supervisor.supervise(entry.getValue(), new SupervisorPolicy.AlwaysRestartPolicy(),
                        LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        for (Entry<String, SourceRunner> entry : materializedConfiguration.getSourceRunners()
                .entrySet()) {
            try {
                logger.info("Starting Source " + entry.getKey());
                supervisor.supervise(entry.getValue(), new SupervisorPolicy.AlwaysRestartPolicy(),
                        LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        this.loadMonitoring();
    }

    private void initSinksZk(SinkRunner sinkRunner) {
        SinkProcessor sinkProcess = sinkRunner.getPolicy();
        try {
            Field sinkField = sinkProcess.getClass().getDeclaredField("sink");
            if (null == sinkField) {
                return;
            }

            sinkField.setAccessible(true);
            Sink sink = (Sink) sinkField.get(sinkProcess);
            if (null == sink) {
                return;
            }
        } catch (Throwable e) {
        }
    }

    @SuppressWarnings("unchecked")
    private void loadMonitoring() {
        Properties systemProps = System.getProperties();
        Set<String> keys = systemProps.stringPropertyNames();
        try {
            if (keys.contains(CONF_MONITOR_CLASS)) {
                String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
                Class<? extends MonitorService> klass;
                try {
                    // Is it a known type?
                    klass = MonitoringType.valueOf(monitorType.toUpperCase()).getMonitorClass();
                } catch (Exception e) {
                    // Not a known type, use FQCN
                    klass = (Class<? extends MonitorService>) Class.forName(monitorType);
                }
                this.monitorServer = klass.newInstance();
                Context context = new Context();
                for (String key : keys) {
                    if (key.startsWith(CONF_MONITOR_PREFIX)) {
                        context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                                systemProps.getProperty(key));
                    }
                }
                monitorServer.configure(context);
                monitorServer.start();
            }
        } catch (Exception e) {
            logger.warn("Error starting monitoring. " + "Monitoring might not be available.", e);
        }

    }

    /**
     * 把自定义的组件注册到类型枚举，方便配置
     */
    private static void addIdealType() {
        final String notExistsConfigClass = "com.ideal.empty.NotExistsConfigClass";

        // channel
        DynamicEnum.addEnum(org.apache.flume.conf.channel.ChannelType.class, "IDEAL-MEMORY",
                com.ideal.flume.channel.MemoryChannel.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.channel.ChannelConfiguration.ChannelConfigurationType.class,
                "IDEAL-MEMORY", notExistsConfigClass);

        // channel selector

        // sink
        DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-KAFKA",
                com.ideal.flume.sink.kafka.KafkaSink.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
                "IDEAL-KAFKA", notExistsConfigClass);

        // DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-HDFS",
        // com.ideal.flume.sink.hdfs.HDFSEventSink.class.getName());
        // DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
        // "IDEAL-HDFS", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-DATABASE-SINK",
                com.ideal.flume.sink.database.DataBaseSink.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
                "IDEAL-DATABASE-SINK", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-FILE-SINK",
                com.ideal.flume.sink.FileDataSink.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
                "IDEAL-FILE-SINK", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-HBASE-SINK",
                com.ideal.flume.sink.hbase.HBaseSink.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
                "IDEAL-HBASE-SINK", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-IPTV-SINK",
                com.ideal.flume.sink.iptv.IptvHDFSEventSink.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
                "IDEAL-IPTV-SINK", notExistsConfigClass);
        
        DynamicEnum.addEnum(org.apache.flume.conf.sink.SinkType.class, "IDEAL-KV-SINK",
        		com.ideal.flume.sink.kv.KVSink.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.sink.SinkConfiguration.SinkConfigurationType.class,
                "IDEAL-KV-SINK", notExistsConfigClass);

        // source
        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-CAP",
                com.ideal.flume.source.YeeSpoolDirectorySource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-CAP", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-CARRY-FILE",
                com.ideal.flume.source.YeeFileCarrySource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-CARRY-FILE", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-KAFKA-V8",
                com.ideal.flume.source.KafkaSourceV8.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-KAFKA-V8", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-KAFKA-V10",
                com.ideal.flume.source.KafkaSourceV10.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-KAFKA-V10", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-DATABASE",
                com.ideal.flume.source.database.DataBaseSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-DATABASE", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-AVRO",
                com.ideal.flume.source.AvroSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-AVRO", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-HBASE",
                com.ideal.flume.source.HbaseSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-HBASE", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-NETCAT",
                com.ideal.flume.source.netcat.NetcatSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-NETCAT", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-HTTP",
                com.ideal.flume.source.http.HTTPSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-HTTP", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-IPTV",
                com.ideal.flume.source.iptv.IptvZipSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-IPTV", notExistsConfigClass);

        DynamicEnum.addEnum(org.apache.flume.conf.source.SourceType.class, "IDEAL-WIFI",
                com.ideal.flume.source.wifi.WifiSource.class.getName());
        DynamicEnum.addEnum(
                org.apache.flume.conf.source.SourceConfiguration.SourceConfigurationType.class,
                "IDEAL-WIFI", notExistsConfigClass);
    }

    public static void main(String[] args) {
        try {
            logger.info("dynamic add Enum types.");
            addIdealType();

            boolean isZkConfigured = false;

            Options options = new Options();

            Option option = new Option("n", "name", true, "the name of this agent");
            option.setRequired(true);
            options.addOption(option);

            option = new Option("f", "conf-file", true,
                    "specify a config file (required if -z missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option(null, "no-reload-conf", false,
                    "do not reload config file if changed");
            options.addOption(option);

            // Options for Zookeeper
            option = new Option("z", "zkConnString", false,
                    "specify the ZooKeeper connection to use (required if -f missing)");
            option.setRequired(false);
            options.addOption(option);

            /** 默认/flume */
            option = new Option("p", "zkBasePath", false,
                    "specify the base path in ZooKeeper for agent configs");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("m", "mongodbUrl", true,
                    "specify a mongodb to save the statistical data");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("r", "redisUrl", true, "redis url to save cache");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("q", "zmqUrl", true, "zeromq url to save the statistical data");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("i", "statInterval", true, "stat send interval in second");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("h", "help", false, "display help text");
            options.addOption(option);

            CommandLineParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(options, args);

            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp("flume-ng agent", options, true);
                return;
            }

            String tmpAgentName = commandLine.getOptionValue('n');
            // int i = tmpAgentName.lastIndexOf('_');
            // if (i > 0) {
            // agentName = tmpAgentName.substring(0, i);
            // agentId = Integer.parseInt(tmpAgentName.substring(i + 1));
            // } else {
            agentName = tmpAgentName;
            agentId = -1;
            // }
            boolean reload = !commandLine.hasOption("no-reload-conf");

            if (commandLine.hasOption('r') || commandLine.hasOption("redisUrl")) {
                String redisUrl = commandLine.getOptionValue('r');
                CacheUtils.setRedis(redisUrl);
            }

            if (commandLine.hasOption('m') || commandLine.hasOption("mongodbUrl")) {
                String mongodbUrl = commandLine.getOptionValue('m');
                StatCounters.addStatWriter(new MongoStatWriter(mongodbUrl));
            }

            if (commandLine.hasOption('q') || commandLine.hasOption("zmqUrl")) {
                if (!ZmqEmbeddedLibraryTools.LOADED_EMBEDDED_LIBRARY) {
                    throw new RuntimeException("there is no zmq lib in "
                            + ZmqEmbeddedLibraryTools.getCurrentPlatformIdentifier());
                }

                String zmqUrl = commandLine.getOptionValue('q');
                StatCounters.addStatWriter(new ZeromqStatWriter(zmqUrl));
            }

            int interval = 30;
            if (commandLine.hasOption('i') || commandLine.hasOption("statInterval")) {
                interval = Integer.parseInt(commandLine.getOptionValue('i'));
            }

            if (commandLine.hasOption('z') || commandLine.hasOption("zkConnString")) {
                isZkConfigured = true;
            }
            Application application = null;
            if (isZkConfigured) {
                // get options
                String zkConnectionStr = commandLine.getOptionValue('z');
                String baseZkPath = commandLine.getOptionValue('p');

                if (reload) {
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    List<LifecycleAware> components = Lists.newArrayList();
                    PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new PollingZooKeeperConfigurationProvider(tmpAgentName, zkConnectionStr,
                                    baseZkPath, eventBus);
                    components.add(zookeeperConfigurationProvider);
                    application = new Application(components);
                    eventBus.register(application);
                    logger.info("reload zk config and register application");

                } else {
                    StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new StaticZooKeeperConfigurationProvider(tmpAgentName, zkConnectionStr,
                                    baseZkPath);
                    application = new Application();
                    application.handleConfigurationEvent(
                            zookeeperConfigurationProvider.getConfiguration());
                    logger.info("no reload zk config and register application");

                }
            } else {
                File configurationFile = new File(commandLine.getOptionValue('f'));

                /*
                 * The following is to ensure that by default the agent will fail on startup if the
                 * file does not exist.
                 */
                if (!configurationFile.exists()) {
                    // If command line invocation, then need to fail fast
                    if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) == null) {
                        String path = configurationFile.getPath();
                        try {
                            path = configurationFile.getCanonicalPath();
                        } catch (IOException ex) {
                            logger.error("Failed to read canonical path for file: " + path, ex);
                        }
                        throw new ParseException(
                                "The specified configuration file does not exist: " + path);
                    }
                }
                List<LifecycleAware> components = Lists.newArrayList();

                if (reload) {
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    PollingPropertiesFileConfigurationProvider configurationProvider =
                            new PollingPropertiesFileConfigurationProvider(tmpAgentName,
                                    configurationFile, eventBus, 30);
                    components.add(configurationProvider);
                    application = new Application(components);
                    eventBus.register(application);
                } else {
                    PropertiesFileConfigurationProvider configurationProvider =
                            new PropertiesFileConfigurationProvider(tmpAgentName,
                                    configurationFile);
                    application = new Application();
                    application.handleConfigurationEvent(configurationProvider.getConfiguration());
                }
            }
            application.start();

            // ScheduledExecutorService timerExecutor =
            // Executors.newSingleThreadScheduledExecutor();
            // timerExecutor.scheduleWithFixedDelay(new Runnable() {
            // @Override
            // public void run() {
            // try {
            // StatCounters.writeStat();
            // } catch (Exception e) {
            // logger.error("", e);
            // }
            // }
            // }, interval, interval, TimeUnit.SECONDS);

            final Application appReference = application;
            Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
                @Override
                public void run() {
                    Counters.stopPrint();
                    appReference.stop();
                }
            });

        } catch (Exception e) {
            logger.error("A fatal error occurred while running. Exception follows.", e);
        }
    }

    public static class AgentName {
        public static String getName() {
            return agentName;
        }

        public static int getId() {
            return agentId;
        }
    }

}
