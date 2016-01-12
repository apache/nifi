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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
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
import java.util.Collection;
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

@SupportsBatching
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
                    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the FlowFile which will be text/plain for Syslog messages.")})
@SeeAlso({PutSyslog.class, ParseSyslog.class})
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
    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Max Batch Size")
        .description(
                "The maximum number of Syslog events to add to a single FlowFile. If multiple events are available, they will be concatenated along with "
                        + "the <Message Delimiter> up to this configured maximum number of messages")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("1")
        .required(true)
        .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
        .name("Message Delimiter")
        .description("Specifies the delimiter to place between Syslog messages when multiple messages are bundled together (see <Max Batch Size> property).")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("\\n")
        .required(true)
        .build();
    public static final PropertyDescriptor PARSE_MESSAGES = new PropertyDescriptor.Builder()
        .name("Parse Messages")
        .description("Indicates if the processor should parse the Syslog messages. If set to false, each outgoing FlowFile will only " +
            "contain the sender, protocol, and port, and no additional attributes.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, syslog " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
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

    private volatile ChannelReader channelReader;
    private volatile SyslogParser parser;
    private volatile BlockingQueue<ByteBuffer> bufferPool;
    private volatile BlockingQueue<RawSyslogEvent> syslogEvents = new LinkedBlockingQueue<>(10);
    private volatile BlockingQueue<RawSyslogEvent> errorEvents = new LinkedBlockingQueue<>();
    private volatile byte[] messageDemarcatorBytes; //it is only the array reference that is volatile - not the contents.

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROTOCOL);
        descriptors.add(PORT);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(RECV_BUFFER_SIZE);
        descriptors.add(MAX_SOCKET_BUFFER_SIZE);
        descriptors.add(MAX_CONNECTIONS);
        descriptors.add(MAX_BATCH_SIZE);
        descriptors.add(MESSAGE_DELIMITER);
        descriptors.add(PARSE_MESSAGES);
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
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // if we are changing the protocol, the events that we may have queued up are no longer valid, as they
        // were received using a different protocol and may be from a completely different source
        if (PROTOCOL.equals(descriptor)) {
            if (syslogEvents != null) {
                syslogEvents.clear();
            }
            if (errorEvents != null) {
                errorEvents.clear();
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(MAX_BATCH_SIZE).asInteger() > 1 && validationContext.getProperty(PARSE_MESSAGES).asBoolean()) {
            results.add(new ValidationResult.Builder().subject("Parse Messages").input("true").valid(false)
                .explanation("Cannot set Parse Messages to 'true' if Batch Size is greater than 1").build());
        }

        final String protocol = validationContext.getProperty(PROTOCOL).getValue();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (UDP_VALUE.getValue().equals(protocol) && sslContextService != null) {
            results.add(new ValidationResult.Builder()
                    .explanation("SSL can not be used with UDP")
                    .valid(false).subject("SSL Context").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int port = context.getProperty(PORT).asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxChannelBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final String charSet = context.getProperty(CHARSET).getValue();
        final String msgDemarcator = context.getProperty(MESSAGE_DELIMITER).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        final String charsetName = context.getProperty(CHARSET).getValue();
        messageDemarcatorBytes = msgDemarcator.getBytes(Charset.forName(charsetName));

        final int maxConnections;
        if (protocol.equals(UDP_VALUE.getValue())) {
            maxConnections = 1;
        } else {
            maxConnections = context.getProperty(MAX_CONNECTIONS).asLong().intValue();
        }

        bufferPool = new LinkedBlockingQueue<>(maxConnections);
        for (int i = 0; i < maxConnections; i++) {
            bufferPool.offer(ByteBuffer.allocate(bufferSize));
        }

        parser = new SyslogParser(Charset.forName(charSet));

        // create either a UDP or TCP reader and call open() to bind to the given port
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        channelReader = createChannelReader(protocol, bufferPool, syslogEvents, maxConnections, sslContextService);
        channelReader.open(port, maxChannelBufferSize);

        final Thread readerThread = new Thread(channelReader);
        readerThread.setName("ListenSyslog [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    // visible for testing.
    protected SyslogParser getParser() {
        return parser;
    }

    // visible for testing to be overridden and provide a mock ChannelReader if desired
    protected ChannelReader createChannelReader(final String protocol, final BlockingQueue<ByteBuffer> bufferPool, final BlockingQueue<RawSyslogEvent> syslogEvents,
        int maxConnections, final SSLContextService sslContextService)
            throws IOException {
        if (protocol.equals(UDP_VALUE.getValue())) {
            return new DatagramChannelReader(bufferPool, syslogEvents, getLogger());
        } else {
            // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
            SSLContext sslContext = null;
            if (sslContextService != null) {
                sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            }

            return new SocketChannelDispatcher(bufferPool, syslogEvents, getLogger(), maxConnections, sslContext);
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

    protected RawSyslogEvent getMessage(final boolean longPoll, final boolean pollErrorQueue) {
        RawSyslogEvent rawSyslogEvent = null;
        if (pollErrorQueue) {
            rawSyslogEvent = errorEvents.poll();
        }

        if (rawSyslogEvent == null) {
            try {
                if (longPoll) {
                    rawSyslogEvent = syslogEvents.poll(100, TimeUnit.MILLISECONDS);
                } else {
                    rawSyslogEvent = syslogEvents.poll();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        return rawSyslogEvent;
    }

    protected int getErrorQueueSize() {
        return errorEvents.size();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // poll the queue with a small timeout to avoid unnecessarily yielding below
        RawSyslogEvent rawSyslogEvent = getMessage(true, true);

        // if nothing in the queue then yield and return
        if (rawSyslogEvent == null) {
            context.yield();
            return;
        }

        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();

        final String port = context.getProperty(PORT).getValue();
        final String protocol = context.getProperty(PROTOCOL).getValue();

        final Map<String, String> defaultAttributes = new HashMap<>(4);
        defaultAttributes.put(SyslogAttributes.PROTOCOL.key(), protocol);
        defaultAttributes.put(SyslogAttributes.PORT.key(), port);
        defaultAttributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");


        final int numAttributes = SyslogAttributes.values().length + 2;
        final boolean shouldParse = context.getProperty(PARSE_MESSAGES).asBoolean();

        final Map<String, FlowFile> flowFilePerSender = new HashMap<>();
        final SyslogParser parser = getParser();

        for (int i = 0; i < maxBatchSize; i++) {
            SyslogEvent event = null;

            // If this is our first iteration, we have already polled our queues. Otherwise, poll on each iteration.
            if (i > 0) {
                rawSyslogEvent = getMessage(false, false);

                if (rawSyslogEvent == null) {
                    break;
                }
            }

            final String sender = rawSyslogEvent.getSender();
            FlowFile flowFile = flowFilePerSender.get(sender);
            if (flowFile == null) {
                flowFile = session.create();
                flowFilePerSender.put(sender, flowFile);
            }

            if (shouldParse) {
                boolean valid = true;
                try {
                    event = parser.parseEvent(rawSyslogEvent.getRawMessage(), sender);
                } catch (final ProcessException pe) {
                    getLogger().warn("Failed to parse Syslog event; routing to invalid");
                    valid = false;
                }

                // If the event is invalid, route it to 'invalid' and then stop.
                // We create a separate FlowFile for this case instead of using 'flowFile',
                // because the 'flowFile' object may already have data written to it.
                if (!valid || !event.isValid()) {
                    FlowFile invalidFlowFile = session.create();
                    invalidFlowFile = session.putAllAttributes(invalidFlowFile, defaultAttributes);
                    if (sender != null) {
                        invalidFlowFile = session.putAttribute(invalidFlowFile, SyslogAttributes.SENDER.key(), sender);
                    }

                    try {
                        final byte[] rawBytes = rawSyslogEvent.getRawMessage();
                        invalidFlowFile = session.write(invalidFlowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                out.write(rawBytes);
                            }
                        });
                    } catch (final Exception e) {
                        getLogger().error("Failed to write contents of Syslog message to FlowFile due to {}; will re-queue message and try again", e);
                        errorEvents.offer(rawSyslogEvent);
                        session.remove(invalidFlowFile);
                        break;
                    }

                    session.transfer(invalidFlowFile, REL_INVALID);
                    break;
                }

                getLogger().trace(event.getFullMessage());

                final Map<String, String> attributes = new HashMap<>(numAttributes);
                attributes.put(SyslogAttributes.PRIORITY.key(), event.getPriority());
                attributes.put(SyslogAttributes.SEVERITY.key(), event.getSeverity());
                attributes.put(SyslogAttributes.FACILITY.key(), event.getFacility());
                attributes.put(SyslogAttributes.VERSION.key(), event.getVersion());
                attributes.put(SyslogAttributes.TIMESTAMP.key(), event.getTimeStamp());
                attributes.put(SyslogAttributes.HOSTNAME.key(), event.getHostName());
                attributes.put(SyslogAttributes.BODY.key(), event.getMsgBody());
                attributes.put(SyslogAttributes.VALID.key(), String.valueOf(event.isValid()));

                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            // figure out if we should write the bytes from the raw event or parsed event
            final boolean writeDemarcator = (i > 0);

            try {
                // write the raw bytes of the message as the FlowFile content
                final byte[] rawMessage = (event == null) ? rawSyslogEvent.getRawMessage() : event.getRawMessage();
                flowFile = session.append(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        if (writeDemarcator) {
                            out.write(messageDemarcatorBytes);
                        }

                        out.write(rawMessage);
                    }
                });
            } catch (final Exception e) {
                getLogger().error("Failed to write contents of Syslog message to FlowFile due to {}; will re-queue message and try again", e);
                errorEvents.offer(rawSyslogEvent);
                break;
            }

            session.adjustCounter("Messages Received", 1L, false);
            flowFilePerSender.put(sender, flowFile);
        }


        for (final Map.Entry<String, FlowFile> entry : flowFilePerSender.entrySet()) {
            final String sender = entry.getKey();
            FlowFile flowFile = entry.getValue();

            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from Sender {}; removing FlowFile", new Object[] {sender});
                continue;
            }

            final Map<String, String> newAttributes = new HashMap<>(defaultAttributes.size() + 1);
            newAttributes.putAll(defaultAttributes);
            newAttributes.put(SyslogAttributes.SENDER.key(), sender);
            flowFile = session.putAllAttributes(flowFile, newAttributes);

            getLogger().debug("Transferring {} to success", new Object[] {flowFile});
            session.transfer(flowFile, REL_SUCCESS);
            final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
            final String transitUri = new StringBuilder().append(protocol.toLowerCase()).append("://").append(senderHost).append(":").append(port).toString();
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    /**
     * Reads messages from a channel until told to stop.
     */
    private interface ChannelReader extends Runnable {

        void open(int port, int maxBufferSize) throws IOException;

        int getPort();

        void stop();

        void close();
    }

    /**
     * Reads from the Datagram channel into an available buffer. If data is read then the buffer is queued for
     * processing, otherwise the buffer is returned to the buffer pool.
     */
    private static class DatagramChannelReader implements ChannelReader {

        private final BlockingQueue<ByteBuffer> bufferPool;
        private final BlockingQueue<RawSyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private DatagramChannel datagramChannel;
        private volatile boolean stopped = false;
        private Selector selector;

        public DatagramChannelReader(final BlockingQueue<ByteBuffer> bufferPool, final BlockingQueue<RawSyslogEvent> syslogEvents, final ProcessorLog logger) {
            this.bufferPool = bufferPool;
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
                            SocketAddress socketAddress;
                            buffer.clear();
                            while (!stopped && (socketAddress = channel.receive(buffer)) != null) {
                                String sender = "";
                                if (socketAddress instanceof InetSocketAddress) {
                                    sender = ((InetSocketAddress) socketAddress).getAddress().toString();
                                }

                                // create a byte array from the buffer
                                buffer.flip();
                                byte bytes[] = new byte[buffer.limit()];
                                buffer.get(bytes, 0, buffer.limit());

                                // queue the raw message with the sender, block until space is available
                                syslogEvents.put(new RawSyslogEvent(bytes, sender));
                                buffer.clear();
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
                try {
                    bufferPool.put(buffer);
                } catch (InterruptedException e) {
                    // nothing to do here
                }
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
    private static class SocketChannelDispatcher implements ChannelReader {

        private final BlockingQueue<ByteBuffer> bufferPool;
        private final BlockingQueue<RawSyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private final ExecutorService executor;
        private volatile boolean stopped = false;
        private Selector selector;
        private final BlockingQueue<SelectionKey> keyQueue;
        private final int maxConnections;
        private final AtomicInteger currentConnections = new AtomicInteger(0);
        private final SSLContext sslContext;

        public SocketChannelDispatcher(final BlockingQueue<ByteBuffer> bufferPool, final BlockingQueue<RawSyslogEvent> syslogEvents,
                                       final ProcessorLog logger, final int maxConnections, final SSLContext sslContext) {
            this.bufferPool = bufferPool;
            this.syslogEvents = syslogEvents;
            this.logger = logger;
            this.maxConnections = maxConnections;
            this.keyQueue = new LinkedBlockingQueue<>(maxConnections);
            this.sslContext = sslContext;
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
                                        new Object[]{socketChannel.getRemoteAddress().toString()});
                                // Set socket to non-blocking, and register with selector
                                socketChannel.configureBlocking(false);
                                SelectionKey readKey = socketChannel.register(selector, SelectionKey.OP_READ);

                                // Prepare the byte buffer for the reads, clear it out
                                ByteBuffer buffer = bufferPool.poll();
                                buffer.clear();
                                buffer.mark();

                                // If we have an SSLContext then create an SSLEngine for the channel
                                SSLEngine sslEngine = null;
                                if (sslContext != null) {
                                    sslEngine = sslContext.createSSLEngine();
                                }

                                // Attach the buffer and SSLEngine to the key
                                SocketChannelAttachment attachment = new SocketChannelAttachment(buffer, sslEngine);
                                readKey.attach(attachment);
                            } else if (key.isReadable()) {
                                // Clear out the operations the select is interested in until done reading
                                key.interestOps(0);
                                // Create a handler based on whether an SSLEngine was provided or not
                                final Runnable handler;
                                if (sslContext != null) {
                                    handler = new SSLSocketChannelHandler(key, this, syslogEvents, logger);
                                } else {
                                    handler = new SocketChannelHandler(key, this, syslogEvents, logger);
                                }
                                // run the handler
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
            try {
                SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();
                bufferPool.put(attachment.getByteBuffer());
            } catch (InterruptedException e) {
                // nothing to do here
            }
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
    private static class SocketChannelHandler implements Runnable {

        private final SelectionKey key;
        private final SocketChannelDispatcher dispatcher;
        private final BlockingQueue<RawSyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

        public SocketChannelHandler(final SelectionKey key, final SocketChannelDispatcher dispatcher, final BlockingQueue<RawSyslogEvent> syslogEvents, final ProcessorLog logger) {
            this.key = key;
            this.dispatcher = dispatcher;
            this.syslogEvents = syslogEvents;
            this.logger = logger;
        }

        @Override
        public void run() {
            boolean eof = false;
            SocketChannel socketChannel = null;

            try {
                int bytesRead;
                socketChannel = (SocketChannel) key.channel();

                SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();
                ByteBuffer socketBuffer = attachment.getByteBuffer();

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
                            String sender = socketChannel.socket().getInetAddress().toString();
                            // queue the raw event blocking until space is available, reset the buffer
                            syslogEvents.put(new RawSyslogEvent(currBytes.toByteArray(), sender));
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
                logger.error("Error reading from channel due to {}", new Object[] {e.getMessage()}, e);
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

    /**
     * Wraps a SocketChannel with an SSLSocketChannel for receiving messages over TLS.
     */
    private static class SSLSocketChannelHandler implements Runnable {

        private final SelectionKey key;
        private final SocketChannelDispatcher dispatcher;
        private final BlockingQueue<RawSyslogEvent> syslogEvents;
        private final ProcessorLog logger;
        private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(4096);

        public SSLSocketChannelHandler(final SelectionKey key, final SocketChannelDispatcher dispatcher, final BlockingQueue<RawSyslogEvent> syslogEvents, final ProcessorLog logger) {
            this.key = key;
            this.dispatcher = dispatcher;
            this.syslogEvents = syslogEvents;
            this.logger = logger;
        }

        @Override
        public void run() {
            boolean eof = false;
            SSLSocketChannel sslSocketChannel = null;
            try {
                int bytesRead;
                final SocketChannel socketChannel = (SocketChannel) key.channel();
                final SocketChannelAttachment attachment = (SocketChannelAttachment) key.attachment();

                // wrap the SocketChannel with an SSLSocketChannel using the SSLEngine from the attachment
                sslSocketChannel = new SSLSocketChannel(attachment.getSslEngine(), socketChannel, false);

                // SSLSocketChannel deals with byte[] so ByteBuffer isn't used here, but we'll use the size to create a new byte[]
                final ByteBuffer socketBuffer = attachment.getByteBuffer();
                byte[] socketBufferArray = new byte[socketBuffer.limit()];

                // read until no more data
                while ((bytesRead = sslSocketChannel.read(socketBufferArray)) > 0) {
                    // go through the buffer looking for the end of each message
                    for (int i = 0; i < bytesRead; i++) {
                        final byte currByte = socketBufferArray[i];
                        currBytes.write(currByte);

                        // check if at end of a message
                        if (currByte == '\n') {
                            final String sender = socketChannel.socket().getInetAddress().toString();
                            // queue the raw event blocking until space is available, reset the temporary buffer
                            syslogEvents.put(new RawSyslogEvent(currBytes.toByteArray(), sender));
                            currBytes.reset();
                        }
                    }
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
                logger.error("Error reading from channel due to {}", new Object[] {e.getMessage()}, e);
                // Treat same as closed socket
                eof = true;
            } finally {
                if(eof == true) {
                    IOUtils.closeQuietly(sslSocketChannel);
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

    // Wrapper class to pass around the raw message and the host/ip that sent it
    static class RawSyslogEvent {

        final byte[] rawMessage;
        final String sender;

        public RawSyslogEvent(byte[] rawMessage, String sender) {
            this.rawMessage = rawMessage;
            this.sender = sender;
        }

        public byte[] getRawMessage() {
            return this.rawMessage;
        }

        public String getSender() {
            return this.sender;
        }

    }

    // Wrapper class so we can attach a buffer and/or an SSLEngine to the selector key
    private static class SocketChannelAttachment {

        private final ByteBuffer byteBuffer;
        private final SSLEngine sslEngine;

        public SocketChannelAttachment(ByteBuffer byteBuffer, SSLEngine sslEngine) {
            this.byteBuffer = byteBuffer;
            this.sslEngine = sslEngine;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public SSLEngine getSslEngine() {
            return sslEngine;
        }

    }

}
