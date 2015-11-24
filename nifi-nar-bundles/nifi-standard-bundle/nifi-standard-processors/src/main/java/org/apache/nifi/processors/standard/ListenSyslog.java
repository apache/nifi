/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.io.nio.BufferPool;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.SyslogEvent;
import org.apache.nifi.processors.standard.util.SyslogParser;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"syslog", "listen", "udp", "tcp", "logs"})
@CapabilityDescription("Listens for Syslog messages being sent to a given port over TCP or UDP. Incoming messages are checked against regular " +
        "expressions for RFC5424 and RFC3164 formatted messages. The format of each message is: (<PRIORITY>)(VERSION )(TIMESTAMP) (HOSTNAME) (BODY) " +
        "where version is optional. The timestamp can be an RFC5424 timestamp with a format of \"yyyy-MM-dd'T'HH:mm:ss.SZ\" or \"yyyy-MM-dd'T'HH:mm:ss.S+hh:mm\", " +
        "or it can be an RFC3164 timestamp with a format of \"MMM d HH:mm:ss\". If an incoming messages matches one of these patterns, the message will be " +
        "parsed and the individual pieces will be placed in FlowFile attributes, with the original message in the content of the FlowFile. If an incoming " +
        "message does not match one of these patterns it will not be parsed and the syslog.valid attribute will be set to false with the original message " +
        "in the content of the FlowFile. Valid messages will be transferred on the success relationship, and invalid messages will be transferred on the " +
        "invalid relationship.")
@WritesAttributes({ @WritesAttribute(attribute="syslog.priority", description="The priority of the Syslog message."),
                    @WritesAttribute(attribute="syslog.severity", description="The severity of the Syslog message derived from the priority."),
                    @WritesAttribute(attribute="syslog.facility", description="The facility of the Syslog message derived from the priority."),
                    @WritesAttribute(attribute="syslog.version", description="The optional version from the Syslog message."),
                    @WritesAttribute(attribute="syslog.timestamp", description="The timestamp of the Syslog message."),
                    @WritesAttribute(attribute="syslog.hostname", description="The hostname of the Syslog message."),
                    @WritesAttribute(attribute="syslog.sender", description="The hostname of the Syslog server that sent the message."),
                    @WritesAttribute(attribute="syslog.body", description="The body of the Syslog message, everything after the hostname."),
                    @WritesAttribute(attribute="syslog.valid", description="An indicator of whether this message matched the expected formats. " +
                            "If this value is false, the other attributes will be empty and only the original message will be available in the content."),
                    @WritesAttribute(attribute="syslog.protocol", description="The protocol over which the Syslog message was received."),
                    @WritesAttribute(attribute="syslog.port", description="The port over which the Syslog message was received."),
                    @WritesAttribute(attribute="mime.type", description="The mime.type of the FlowFile which will be text/plain for Syslog messages.")})
public class ListenSyslog extends AbstractSyslogProcessor {

    public static final PropertyDescriptor RECV_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Receive Buffer Size")
            .description("The size of each buffer used to receive Syslog messages. Adjust this value appropriately based on the expected size of the " +
                    "incoming Syslog messages. When UDP is selected each buffer will hold one Syslog message. When TCP is selected messages are read " +
                    "from an incoming connection until the buffer is full, or the connection is closed. ")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("65507 B")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Buffer")
            .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Number of TCP Connections")
            .description("The maximum number of concurrent connections to accept Syslog messages in TCP mode.")
            .addValidator(StandardValidators.createLongValidator(1, 65535, true))
            .defaultValue("2")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Syslog messages that match one of the expected formats will be sent out this relationship as a FlowFile per message.")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("Syslog messages that do not match one of the expected formats will be sent out this relationship as a FlowFile per message.")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    private volatile BufferPool bufferPool;
    private volatile ChannelReader channelReader;
    private volatile SyslogParser parser;
    private volatile BlockingQueue<SyslogEvent> syslogEvents;
    private volatile BlockingQueue<SyslogEvent> errorEvents;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROTOCOL);
        descriptors.add(PORT);
        descriptors.add(RECV_BUFFER_SIZE);
        descriptors.add(MAX_SOCKET_BUFFER_SIZE);
        descriptors.add(MAX_CONNECTIONS);
        descriptors.add(CHARSET);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_INVALID);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // since properties were changed, clear any events that were queued
        if (syslogEvents != null) {
            syslogEvents.clear();
        }
        if (errorEvents != null) {
            errorEvents.clear();
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int port = context.getProperty(PORT).asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxChannelBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final String charSet = context.getProperty(CHARSET).getValue();
        final int maxConnections;

        if (protocol.equals(UDP_VALUE.getValue())) {
            maxConnections = 1;
        } else {
            maxConnections = context.getProperty(MAX_CONNECTIONS).asLong().intValue();
        }

        parser = new SyslogParser(Charset.forName(charSet));
        bufferPool = new BufferPool(maxConnections, bufferSize, false, Integer.MAX_VALUE);
        syslogEvents = new LinkedBlockingQueue<>(10);
        errorEvents = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());

        // create either a UDP or TCP reader and call open() to bind to the given port
        channelReader = createChannelReader(protocol, bufferPool, parser, syslogEvents, maxConnections);
        channelReader.open(port, maxChannelBufferSize);

        final Thread readerThread = new Thread(channelReader);
        readerThread.setName("ListenSyslog [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    // visible for testing to be overridden and provide a mock ChannelReader if desired
    protected ChannelReader createChannelReader(final String protocol, final BufferPool bufferPool, final SyslogParser syslogParser, final BlockingQueue<SyslogEvent> syslogEvents, int maxConnections)
            throws IOException {
        if (protocol.equals(UDP_VALUE.getValue())) {
            return new DatagramChannelReader(bufferPool, syslogParser, syslogEvents, getLogger());
        } else {
            return new SocketChannelReader(bufferPool, syslogParser, syslogEvents, getLogger(), maxConnections);
        }
    }

    // used for testing to access the random port that was selected
    protected int getPort() {
        return channelReader == null ? 0 : channelReader.getPort();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (channelReader != null) {
            channelReader.stop();
            channelReader.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // try to pull from the error queue first, if empty then pull from main queue
        SyslogEvent initialEvent = errorEvents.poll();
        if (initialEvent == null) {
            initialEvent = syslogEvents.poll();
        }

        // if nothing in either queue then yield and return
        if (initialEvent == null) {
            context.yield();
            return;
        }

        final SyslogEvent event = initialEvent;
        final String port = context.getProperty(PORT).getValue();
        final String protocol = context.getProperty(PROTOCOL).getValue();

        final Map<String,String> attributes = new HashMap<>();
        attributes.put(SyslogAttributes.PRIORITY.key(), event.getPriority());
        attributes.put(SyslogAttributes.SEVERITY.key(), event.getSeverity());
        attributes.put(SyslogAttributes.FACILITY.key(), event.getFacility());
        attributes.put(SyslogAttributes.VERSION.key(), event.getVersion());
        attributes.put(SyslogAttributes.TIMESTAMP.key(), event.getTimeStamp());
        attributes.put(SyslogAttributes.HOSTNAME.key(), event.getHostName());
        attributes.put(SyslogAttributes.SENDER.key(), event.getSender());
        attributes.put(SyslogAttributes.BODY.key(), event.getMsgBody());
        attributes.put(SyslogAttributes.VALID.key(), String.valueOf(event.isValid()));
        attributes.put(SyslogAttributes.PROTOCOL.key(), protocol);
        attributes.put(SyslogAttributes.PORT.key(), port);
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");

        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);

        final String transitUri = new StringBuilder().append(protocol).append("://").append(event.getSender())
                .append(":").append(port).toString();

        try {
            // write the raw bytes of the message as the FlowFile content
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(event.getRawMessage());
                }
            });

            if (event.isValid()) {
                getLogger().info("Transferring {} to success", new Object[]{flowFile});
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, transitUri);
            } else {
                getLogger().info("Transferring {} to invalid", new Object[]{flowFile});
                session.transfer(flowFile, REL_INVALID);
            }

        } catch (ProcessException e) {
            getLogger().error("Error processing Syslog message", e);
            errorEvents.offer(event);
            session.remove(flowFile);
        }
    }

    /**
     * Reads messages from a channel until told to stop.
     */
    public interface ChannelReader extends Runnable {

        void open(int port, int maxBufferSize) throws IOException;

        int getPort();

        void stop();

        void close();
    }

    /**
     * Reads from the Datagram channel into an available buffer. If data is read then the buffer is queued for
     * processing, otherwise the buffer is returned to the buffer pool.
     */
    public static class DatagramChannelReader implements ChannelReader {

        private final BufferPool bufferPool;
        private final SyslogParser syslogParser;
        private final BlockingQueue<SyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private DatagramChannel datagramChannel;
        private volatile boolean stopped = false;
        private Selector selector;

        public DatagramChannelReader(final BufferPool bufferPool, final SyslogParser syslogParser, final BlockingQueue<SyslogEvent> syslogEvents,
                                     final ProcessorLog logger) {
            this.bufferPool = bufferPool;
            this.syslogParser = syslogParser;
            this.syslogEvents = syslogEvents;
            this.logger = logger;
        }

        @Override
        public void open(final int port, int maxBufferSize) throws IOException {
            datagramChannel = DatagramChannel.open();
            datagramChannel.configureBlocking(false);
            if (maxBufferSize > 0) {
                datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, maxBufferSize);
                final int actualReceiveBufSize = datagramChannel.getOption(StandardSocketOptions.SO_RCVBUF);
                if (actualReceiveBufSize < maxBufferSize) {
                    logMaxBufferWarning(logger, maxBufferSize, actualReceiveBufSize);
                }
            }
            datagramChannel.socket().bind(new InetSocketAddress(port));
            selector = Selector.open();
            datagramChannel.register(selector, SelectionKey.OP_READ);
        }

        @Override
        public void run() {
            final ByteBuffer buffer = bufferPool.poll();
            while (!stopped) {
                try {
                    int selected = selector.select();
                    if (selected > 0){
                        Iterator<SelectionKey> selectorKeys = selector.selectedKeys().iterator();
                        while (selectorKeys.hasNext()) {
                            SelectionKey key = selectorKeys.next();
                            selectorKeys.remove();
                            if (!key.isValid()) {
                                continue;
                            }
                            DatagramChannel channel = (DatagramChannel) key.channel();
                            SocketAddress sender;
                            buffer.clear();
                            while (!stopped && (sender = channel.receive(buffer)) != null) {
                                final SyslogEvent event;
                                if (sender instanceof InetSocketAddress) {
                                    event = syslogParser.parseEvent(buffer, ((InetSocketAddress)sender).getAddress().toString());
                                } else {
                                    event = syslogParser.parseEvent(buffer);
                                }
                                logger.trace(event.getFullMessage());
                                syslogEvents.put(event); // block until space is available
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    stopped = true;
                } catch (IOException e) {
                    logger.error("Error reading from DatagramChannel", e);
                }
            }
            if (buffer != null) {
                bufferPool.returnBuffer(buffer, 0);
            }
        }

        @Override
        public int getPort() {
            return datagramChannel == null ? 0 : datagramChannel.socket().getLocalPort();
        }

        @Override
        public void stop() {
            selector.wakeup();
            stopped = true;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(selector);
            IOUtils.closeQuietly(datagramChannel);
        }
    }

    /**
     * Accepts Socket connections on the given port and creates a handler for each connection to
     * be executed by a thread pool.
     */
    public static class SocketChannelReader implements ChannelReader {

        private final BufferPool bufferPool;
        private final SyslogParser syslogParser;
        private final BlockingQueue<SyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private final ExecutorService executor;
        private volatile boolean stopped = false;
        private Selector selector;
        private final BlockingQueue<SelectionKey> keyQueue;
        private final int maxConnections;
        private final AtomicInteger currentConnections = new AtomicInteger(0);

        public SocketChannelReader(final BufferPool bufferPool, final SyslogParser syslogParser, final BlockingQueue<SyslogEvent> syslogEvents,
                                   final ProcessorLog logger, final int maxConnections) {
            this.bufferPool = bufferPool;
            this.syslogParser = syslogParser;
            this.syslogEvents = syslogEvents;
            this.logger = logger;
            this.maxConnections = maxConnections;
            this.keyQueue = new LinkedBlockingQueue<>(maxConnections);
            this.executor = Executors.newFixedThreadPool(maxConnections);
        }

        @Override
        public void open(final int port, int maxBufferSize) throws IOException {
            final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            if (maxBufferSize > 0) {
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, maxBufferSize);
                final int actualReceiveBufSize = serverSocketChannel.getOption(StandardSocketOptions.SO_RCVBUF);
                if (actualReceiveBufSize < maxBufferSize) {
                    logMaxBufferWarning(logger, maxBufferSize, actualReceiveBufSize);
                }
            }
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
            selector = Selector.open();
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    int selected = selector.select();
                    if (selected > 0){
                        Iterator<SelectionKey> selectorKeys = selector.selectedKeys().iterator();
                        while (selectorKeys.hasNext()){
                            SelectionKey key = selectorKeys.next();
                            selectorKeys.remove();
                            if (!key.isValid()){
                                continue;
                            }
                            if (key.isAcceptable()) {
                                // Handle new connections coming in
                                final ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                                final SocketChannel socketChannel = channel.accept();
                                // Check for available connections
                                if (currentConnections.incrementAndGet() > maxConnections){
                                    currentConnections.decrementAndGet();
                                    logger.warn("Rejecting connection from {} because max connections has been met",
                                            new Object[]{ socketChannel.getRemoteAddress().toString() });
                                    IOUtils.closeQuietly(socketChannel);
                                    continue;
                                }
                                logger.debug("Accepted incoming connection from {}",
                                        new Object[]{socketChannel.getRemoteAddress().toString()} );
                                // Set socket to non-blocking, and register with selector
                                socketChannel.configureBlocking(false);
                                SelectionKey readKey = socketChannel.register(selector, SelectionKey.OP_READ);
                                // Prepare the byte buffer for the reads, clear it out and attach to key
                                ByteBuffer buffer = bufferPool.poll();
                                buffer.clear();
                                buffer.mark();
                                readKey.attach(buffer);
                            } else if (key.isReadable()) {
                                // Clear out the operations the select is interested in until done reading
                                key.interestOps(0);
                                // Create and execute the read handler
                                final SocketChannelHandler handler = new SocketChannelHandler(key, this,
                                        syslogParser, syslogEvents, logger);
                                // and launch the thread
                                executor.execute(handler);
                            }
                        }
                    }
                    // Add back all idle sockets to the select
                    SelectionKey key;
                    while((key = keyQueue.poll()) != null){
                        key.interestOps(SelectionKey.OP_READ);
                    }
                } catch (IOException e) {
                    logger.error("Error accepting connection from SocketChannel", e);
                }
            }
        }

        @Override
        public int getPort() {
            // Return the port for the key listening for accepts
            for(SelectionKey key : selector.keys()){
                if (key.isValid()) {
                    final Channel channel = key.channel();
                    if (channel instanceof  ServerSocketChannel) {
                        return ((ServerSocketChannel)channel).socket().getLocalPort();
                    }
                }
            }
            return 0;
        }

        @Override
        public void stop() {
            stopped = true;
            selector.wakeup();
        }

        @Override
        public void close() {
            executor.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executor.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                executor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
            for(SelectionKey key : selector.keys()){
                IOUtils.closeQuietly(key.channel());
            }
            IOUtils.closeQuietly(selector);
        }

        public void completeConnection(SelectionKey key) {
            // connection is done. Return the buffer to the pool
            bufferPool.returnBuffer((ByteBuffer) key.attachment(), 0);
            currentConnections.decrementAndGet();
        }

        public void addBackForSelection(SelectionKey key) {
            keyQueue.offer(key);
            selector.wakeup();
        }

    }

    /**
     * Reads from the given SocketChannel into the provided buffer. If data is read then the buffer is queued for
     * processing, otherwise the buffer is returned to the buffer pool.
     */
    public static class SocketChannelHandler implements Runnable {

        private final SelectionKey key;
        private final SocketChannelReader dispatcher;
        private final SyslogParser syslogParser;
        private final BlockingQueue<SyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

        public SocketChannelHandler(final SelectionKey key, final SocketChannelReader dispatcher, final SyslogParser syslogParser,
                                    final BlockingQueue<SyslogEvent> syslogEvents, final ProcessorLog logger) {
            this.key = key;
            this.dispatcher = dispatcher;
            this.syslogParser = syslogParser;
            this.syslogEvents = syslogEvents;
            this.logger = logger;
        }

        @Override
        public void run() {
            boolean eof = false;
            SocketChannel socketChannel = null;
            ByteBuffer socketBuffer = null;

            try {
                int bytesRead;
                socketChannel = (SocketChannel) key.channel();
                socketBuffer = (ByteBuffer) key.attachment();
                // read until the buffer is full
                while ((bytesRead = socketChannel.read(socketBuffer)) > 0) {
                    // prepare byte buffer for reading
                    socketBuffer.flip();
                    // mark the current position as start, in case of partial message read
                    socketBuffer.mark();

                    // get total bytes in buffer
                    int total = socketBuffer.remaining();
                    // go through the buffer looking for the end of each message
                    currBytes.reset();
                    for (int i = 0; i < total; i++) {
                        // NOTE: For higher throughput, the looking for \n and copying into the byte
                        // stream could be improved
                        // Pull data out of buffer and cram into byte array
                        byte currByte = socketBuffer.get();
                        currBytes.write(currByte);

                        // check if at end of a message
                        if (currByte == '\n') {
                            // parse an event, reset the buffer
                            final SyslogEvent event = syslogParser.parseEvent(currBytes.toByteArray(),
                                    socketChannel.socket().getInetAddress().toString());
                            logger.trace(event.getFullMessage());
                            syslogEvents.put(event); // block until space is available
                            currBytes.reset();
                            // Mark this as the start of the next message
                            socketBuffer.mark();
                        }
                    }
                    // Preserve bytes in buffer for next call to run
                    // NOTE: This code could benefit from the  two ByteBuffer read calls to avoid
                    //  this compact for higher throughput
                    socketBuffer.reset();
                    socketBuffer.compact();
                    logger.debug("done handling SocketChannel");
                }
                // Check for closed socket
                if( bytesRead < 0 ){
                    eof = true;
                }
            } catch (ClosedByInterruptException | InterruptedException e) {
                logger.debug("read loop interrupted, closing connection");
                // Treat same as closed socket
                eof = true;
            } catch (IOException e) {
                logger.error("Error reading from channel", e);
                // Treat same as closed socket
                eof = true;
            } finally {
                if(eof == true) {
                    IOUtils.closeQuietly(socketChannel);
                    dispatcher.completeConnection(key);
                } else {
                    dispatcher.addBackForSelection(key);
                }
            }
        }
    }

    static void logMaxBufferWarning(final ProcessorLog logger, int maxBufferSize, int actualReceiveBufSize) {
        logger.warn("Attempted to set Socket Buffer Size to " + maxBufferSize + " bytes but could only set to "
                + actualReceiveBufSize + "bytes. You may want to consider changing the Operating System's "
                + "maximum receive buffer");
    }

}
