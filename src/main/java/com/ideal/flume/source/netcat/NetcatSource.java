package com.ideal.flume.source.netcat;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.NetcatSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ideal.flume.stat.StatCounters;
import com.ideal.flume.stat.StatCounters.StatCounter;
import com.ideal.flume.stat.StatCounters.StatType;

public class NetcatSource extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(NetcatSource.class);

    private String hostName;
    private int port;
    private int maxLineLength;
    private boolean ackEveryEvent;

    private ServerSocketChannel serverSocket;
    private AtomicBoolean acceptThreadShouldStop;
    private Thread acceptThread;
    private ExecutorService handlerService;

    private StatCounter statCounter;

    public NetcatSource() {
        super();

        port = 0;
        acceptThreadShouldStop = new AtomicBoolean(false);
    }

    @Override
    public void configure(Context context) {
        String hostKey = NetcatSourceConfigurationConstants.CONFIG_HOSTNAME;
        String portKey = NetcatSourceConfigurationConstants.CONFIG_PORT;
        String ackEventKey = NetcatSourceConfigurationConstants.CONFIG_ACKEVENT;

        Configurables.ensureRequiredNonNull(context, hostKey, portKey);

        hostName = context.getString(hostKey);
        port = context.getInteger(portKey);
        ackEveryEvent = context.getBoolean(ackEventKey, false);
        maxLineLength = context
                .getInteger(NetcatSourceConfigurationConstants.CONFIG_MAX_LINE_LENGTH, 33554432);
    }

    @Override
    public void start() {
        logger.info("Source starting");

        statCounter = StatCounters.create(StatType.SOURCE, getName());

        handlerService = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("netcat-handler-%d").build());

        try {
            SocketAddress bindPoint = new InetSocketAddress(hostName, port);

            serverSocket = ServerSocketChannel.open();
            serverSocket.socket().setReuseAddress(true);
            serverSocket.socket().bind(bindPoint);

            logger.info("Created serverSocket:{}", serverSocket);
        } catch (IOException e) {
            logger.error("Unable to bind to socket. Exception follows.", e);
            throw new FlumeException(e);
        }

        AcceptHandler acceptRunnable = new AcceptHandler(maxLineLength);
        acceptThreadShouldStop.set(false);
        acceptRunnable.statCounter = statCounter;
        acceptRunnable.handlerService = handlerService;
        acceptRunnable.shouldStop = acceptThreadShouldStop;
        acceptRunnable.ackEveryEvent = ackEveryEvent;
        acceptRunnable.source = this;
        acceptRunnable.serverSocket = serverSocket;

        acceptThread = new Thread(acceptRunnable);

        acceptThread.start();

        logger.info("Source started");
        super.start();
    }

    @Override
    public void stop() {
        logger.info("Source stopping");

        acceptThreadShouldStop.set(true);

        if (acceptThread != null) {
            logger.info("Stopping accept handler thread");

            while (acceptThread.isAlive()) {
                try {
                    logger.info("Waiting for accept handler to finish");
                    acceptThread.interrupt();
                    acceptThread.join(500);
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for accept handler to finish");
                    Thread.currentThread().interrupt();
                }
            }

            logger.info("Stopped accept handler thread");
        }

        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.error("Unable to close socket. Exception follows.", e);
                return;
            }
        }

        if (handlerService != null) {
            handlerService.shutdown();

            logger.info("Waiting for handler service to stop");

            // wait 500ms for threads to stop
            try {
                handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.info("Interrupted while waiting for netcat handler service to stop");
                Thread.currentThread().interrupt();
            }

            if (!handlerService.isShutdown()) {
                handlerService.shutdownNow();
            }

            logger.info("Handler service stopped");
        }

        logger.info("Source stopped.");
        super.stop();
    }

    private static class AcceptHandler implements Runnable {
        private ServerSocketChannel serverSocket;
        private StatCounter statCounter;
        private ExecutorService handlerService;
        private EventDrivenSource source;
        private AtomicBoolean shouldStop;
        private boolean ackEveryEvent;

        private final int maxLineLength;

        public AcceptHandler(int maxLineLength) {
            this.maxLineLength = maxLineLength;
        }

        @Override
        public void run() {
            logger.info("Starting accept handler");

            while (!shouldStop.get()) {
                try {
                    SocketChannel socketChannel = serverSocket.accept();

                    NetcatSocketHandler request = new NetcatSocketHandler(maxLineLength);
                    request.socketChannel = socketChannel;
                    request.statCounter = statCounter;
                    request.source = source;
                    request.ackEveryEvent = ackEveryEvent;
                    request.shouldStop = shouldStop;

                    handlerService.submit(request);
                } catch (ClosedByInterruptException e) {
                    // Parent is canceling us.
                } catch (IOException e) {
                    logger.error("Unable to accept connection. Exception follows.", e);
                }
            }

            logger.info("Accept handler exiting");
        }
    }

    private static class NetcatSocketHandler implements Runnable {
        private Source source;
        private StatCounter statCounter;
        private SocketChannel socketChannel;
        private boolean ackEveryEvent;

        private final int maxLineLength;
        private AtomicBoolean shouldStop;

        public NetcatSocketHandler(int maxLineLength) {
            this.maxLineLength = maxLineLength;
        }

        @Override
        public void run() {
            logger.info("Starting connection handler");

            try {
                Reader reader = Channels.newReader(socketChannel, "utf-8");
                Writer writer = Channels.newWriter(socketChannel, "utf-8");
                CharBuffer buffer = CharBuffer.allocate(maxLineLength);
                buffer.flip(); // flip() so fill() sees buffer as initially empty

                while (!shouldStop.get()) {
                    // this method blocks until new data is available in the socket
                    int charsRead = fill(buffer, reader);
                    logger.debug("Chars read = {}", charsRead);

                    // attempt to process all the events in the buffer
                    int eventsProcessed = processEvents(buffer, writer);
                    logger.debug("Events processed = {}", eventsProcessed);

                    if (charsRead == -1) {
                        // if we received EOF before last event processing attempt, then we
                        // have done everything we can
                        break;
                    } else if (charsRead == 0 && eventsProcessed == 0) {
                        if (buffer.remaining() == buffer.capacity()) {
                            // If we get here it means:
                            // 1. Last time we called fill(), no new chars were buffered
                            // 2. After that, we failed to process any events => no newlines
                            // 3. The unread data in the buffer == the size of the buffer
                            // Therefore, we are stuck because the client sent a line longer
                            // than the size of the buffer. Response: Drop the connection.
                            logger.warn("Client sent event exceeding the maximum length");
                            writer.write("FAILED: Event exceeds the maximum length ("
                                    + buffer.capacity() + " chars, including newline)\n");
                            writer.flush();
                            break;
                        }
                    }
                }

                socketChannel.close();
            } catch (IOException e) {
                logger.error("", e);
            }

            logger.info("Connection handler exiting");
        }

        private int processEvents(CharBuffer buffer, Writer writer) throws IOException {
            int numProcessed = 0;

            boolean foundNewLine = true;
            while (foundNewLine) {
                foundNewLine = false;

                int limit = buffer.limit();
                for (int pos = buffer.position(); pos < limit; pos++) {
                    if (buffer.get(pos) == '\n') {

                        // parse event body bytes out of CharBuffer
                        buffer.limit(pos); // temporary limit
                        ByteBuffer bytes = Charsets.UTF_8.encode(buffer);
                        buffer.limit(limit); // restore limit

                        // build event object
                        byte[] body = new byte[bytes.remaining()];
                        bytes.get(body);
                        Event event = EventBuilder.withBody(body);
                        statCounter.addByte(body.length);

                        // process event
                        ChannelException ex = null;
                        try {
                            source.getChannelProcessor().processEvent(event);
                        } catch (ChannelException chEx) {
                            ex = chEx;
                        }

                        if (ex == null) {
                            statCounter.incrEvent();
                            numProcessed++;
                            if (true == ackEveryEvent) {
                                writer.write("OK\n");
                            }
                        } else {
                            logger.warn("Error processing event. Exception follows.", ex);
                            writer.write("FAILED: " + ex.getMessage() + "\n");
                        }
                        writer.flush();

                        // advance position after data is consumed
                        buffer.position(pos + 1); // skip newline
                        foundNewLine = true;

                        break;
                    }
                }

            }

            return numProcessed;
        }

        private int fill(CharBuffer buffer, Reader reader) throws IOException {
            // move existing data to the front of the buffer
            buffer.compact();

            // pull in as much data as we can from the socket
            int charsRead = reader.read(buffer);

            // flip so the data can be consumed
            buffer.flip();

            return charsRead;
        }

    }

}
