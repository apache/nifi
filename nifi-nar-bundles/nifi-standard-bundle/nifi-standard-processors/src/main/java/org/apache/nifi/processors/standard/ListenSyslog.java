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

import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.DatagramChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.handler.socket.SocketChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.SyslogEvent;
import org.apache.nifi.syslog.parsers.SyslogParser;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

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
                    @WritesAttribute(attribute="syslog.hostname", description="The hostname or IP address of the Syslog message."),
                    @WritesAttribute(attribute="syslog.sender", description="The hostname of the Syslog server that sent the message."),
                    @WritesAttribute(attribute="syslog.body", description="The body of the Syslog message, everything after the hostname."),
                    @WritesAttribute(attribute="syslog.valid", description="An indicator of whether this message matched the expected formats. " +
                            "If this value is false, the other attributes will be empty and only the original message will be available in the content."),
                    @WritesAttribute(attribute="syslog.protocol", description="The protocol over which the Syslog message was received."),
                    @WritesAttribute(attribute="syslog.port", description="The port over which the Syslog message was received."),
                    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the FlowFile which will be text/plain for Syslog messages.")})
@SeeAlso({PutSyslog.class, ParseSyslog.class})
public class ListenSyslog extends AbstractSyslogProcessor {

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
        .name("Max Size of Message Queue")
        .displayName("Max Size of Message Queue")
        .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
                    "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
                    "memory used by the processor.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("10000")
        .required(true)
        .build();

    public static final PropertyDescriptor RECV_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Receive Buffer Size")
        .displayName("Receive Buffer Size")
        .description("The size of each buffer used to receive Syslog messages. Adjust this value appropriately based on the expected size of the " +
                    "incoming Syslog messages. When UDP is selected each buffer will hold one Syslog message. When TCP is selected messages are read " +
                    "from an incoming connection until the buffer is full, or the connection is closed. ")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("65507 B")
        .required(true)
        .build();
    public static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Max Size of Socket Buffer")
        .displayName("Max Size of Socket Buffer")
        .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .required(true)
        .build();
    public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
        .name("Max Number of TCP Connections")
        .displayName("Max Number of TCP Connections")
        .description("The maximum number of concurrent connections to accept Syslog messages in TCP mode.")
        .addValidator(StandardValidators.createLongValidator(1, 65535, true))
        .defaultValue("2")
        .required(true)
        .build();
    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Max Batch Size")
        .displayName("Max Batch Size")
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
        .displayName("Message Delimiter")
        .description("Specifies the delimiter to place between Syslog messages when multiple messages are bundled together (see <Max Batch Size> property).")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("\\n")
        .required(true)
        .build();
    public static final PropertyDescriptor PARSE_MESSAGES = new PropertyDescriptor.Builder()
        .name("Parse Messages")
        .displayName("Parse Messages")
        .description("Indicates if the processor should parse the Syslog messages. If set to false, each outgoing FlowFile will only " +
                    "contain the sender, protocol, and port, and no additional attributes.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .displayName("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, syslog " +
                    "messages will be received over a secure connection.")
        .required(false)
        .identifiesControllerService(RestrictedSSLContextService.class)
        .build();
    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
        .name("Client Auth")
        .displayName("Client Auth")
        .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
        .required(false)
        .allowableValues(SSLContextService.ClientAuth.values())
        .defaultValue(SSLContextService.ClientAuth.REQUIRED.name())
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

    private volatile ChannelDispatcher channelDispatcher;
    private volatile SyslogParser parser;
    private volatile BlockingQueue<ByteBuffer> bufferPool;
    private volatile BlockingQueue<RawSyslogEvent> syslogEvents;
    private volatile BlockingQueue<RawSyslogEvent> errorEvents = new LinkedBlockingQueue<>();
    private volatile byte[] messageDemarcatorBytes; //it is only the array reference that is volatile - not the contents.

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROTOCOL);
        descriptors.add(PORT);
        descriptors.add(NETWORK_INTF_NAME);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(CLIENT_AUTH);
        descriptors.add(RECV_BUFFER_SIZE);
        descriptors.add(MAX_MESSAGE_QUEUE_SIZE);
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

        // Validate CLIENT_AUTH
        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }


        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxChannelBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxMessageQueueSize = context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger();
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final String nicIPAddressStr = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final String charSet = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();
        final String msgDemarcator = context.getProperty(MESSAGE_DELIMITER).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        messageDemarcatorBytes = msgDemarcator.getBytes(Charset.forName(charSet));

        final int maxConnections;
        if (UDP_VALUE.getValue().equals(protocol)) {
            maxConnections = 1;
        } else {
            maxConnections = context.getProperty(MAX_CONNECTIONS).asLong().intValue();
        }

        bufferPool = new LinkedBlockingQueue<>(maxConnections);
        for (int i = 0; i < maxConnections; i++) {
            bufferPool.offer(ByteBuffer.allocate(bufferSize));
        }

        parser = new SyslogParser(Charset.forName(charSet));
        syslogEvents = new LinkedBlockingQueue<>(maxMessageQueueSize);

        InetAddress nicIPAddress = null;
        if (!StringUtils.isEmpty(nicIPAddressStr)) {
            NetworkInterface netIF = NetworkInterface.getByName(nicIPAddressStr);
            nicIPAddress = netIF.getInetAddresses().nextElement();
        }

        // create either a UDP or TCP reader and call open() to bind to the given port
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        channelDispatcher = createChannelReader(context, protocol, bufferPool, syslogEvents, maxConnections, sslContextService, Charset.forName(charSet));
        channelDispatcher.open(nicIPAddress, port, maxChannelBufferSize);

        final Thread readerThread = new Thread(channelDispatcher);
        readerThread.setName("ListenSyslog [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    // visible for testing.
    protected SyslogParser getParser() {
        return parser;
    }

    // visible for testing to be overridden and provide a mock ChannelDispatcher if desired
    protected ChannelDispatcher createChannelReader(final ProcessContext context, final String protocol, final BlockingQueue<ByteBuffer> bufferPool,
                                                    final BlockingQueue<RawSyslogEvent> events, final int maxConnections,
                                                    final SSLContextService sslContextService, final Charset charset) throws IOException {

        final EventFactory<RawSyslogEvent> eventFactory = new RawSyslogEventFactory();

        if (UDP_VALUE.getValue().equals(protocol)) {
            return new DatagramChannelDispatcher(eventFactory, bufferPool, events, getLogger());
        } else {
            // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
            SSLContext sslContext = null;
            SslContextFactory.ClientAuth clientAuth = null;

            if (sslContextService != null) {
                final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
                sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuthValue));
                clientAuth = SslContextFactory.ClientAuth.valueOf(clientAuthValue);
            }

            final ChannelHandlerFactory<RawSyslogEvent<SocketChannel>, AsyncChannelDispatcher> handlerFactory = new SocketChannelHandlerFactory<>();
            return new SocketChannelDispatcher(eventFactory, handlerFactory, bufferPool, events, getLogger(), maxConnections, sslContext, clientAuth, charset);
        }
    }

    // used for testing to access the random port that was selected
    protected int getPort() {
        return channelDispatcher == null ? 0 : channelDispatcher.getPort();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (channelDispatcher != null) {
            channelDispatcher.close();
        }
    }

    protected RawSyslogEvent getMessage(final boolean longPoll, final boolean pollErrorQueue, final ProcessSession session) {
        RawSyslogEvent rawSyslogEvent = null;
        if (pollErrorQueue) {
            rawSyslogEvent = errorEvents.poll();
        }

        if (rawSyslogEvent == null) {
            try {
                if (longPoll) {
                    rawSyslogEvent = syslogEvents.poll(20, TimeUnit.MILLISECONDS);
                } else {
                    rawSyslogEvent = syslogEvents.poll();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        if (rawSyslogEvent != null) {
            session.adjustCounter("Messages Received", 1L, false);
        }

        return rawSyslogEvent;
    }

    protected int getErrorQueueSize() {
        return errorEvents.size();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // poll the queue with a small timeout to avoid unnecessarily yielding below
        RawSyslogEvent rawSyslogEvent = getMessage(true, true, session);

        // if nothing in the queue just return, we don't want to yield here because yielding could adversely
        // impact performance, and we already have a long poll in getMessage so there will be some built in
        // throttling even when no data is available
        if (rawSyslogEvent == null) {
            return;
        }

        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();

        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();
        final String protocol = context.getProperty(PROTOCOL).getValue();

        final Map<String, String> defaultAttributes = new HashMap<>(4);
        defaultAttributes.put(SyslogAttributes.SYSLOG_PROTOCOL.key(), protocol);
        defaultAttributes.put(SyslogAttributes.SYSLOG_PORT.key(), port);
        defaultAttributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");


        final int numAttributes = SyslogAttributes.values().length + 2;
        final boolean shouldParse = context.getProperty(PARSE_MESSAGES).asBoolean();

        final Map<String, FlowFile> flowFilePerSender = new HashMap<>();
        final SyslogParser parser = getParser();

        for (int i = 0; i < maxBatchSize; i++) {
            SyslogEvent event = null;

            // If this is our first iteration, we have already polled our queues. Otherwise, poll on each iteration.
            if (i > 0) {
                rawSyslogEvent = getMessage(true, false, session);

                if (rawSyslogEvent == null) {
                    break;
                }
            }

            final String sender = rawSyslogEvent.getSender();
            FlowFile flowFile = flowFilePerSender.computeIfAbsent(sender, k -> session.create());

            if (shouldParse) {
                boolean valid = true;
                try {
                    event = parser.parseEvent(rawSyslogEvent.getData(), sender);
                } catch (final ProcessException pe) {
                    getLogger().warn("Failed to parse Syslog event; routing to invalid");
                    valid = false;
                }

                // If the event is invalid, route it to 'invalid' and then stop.
                // We create a separate FlowFile for this case instead of using 'flowFile',
                // because the 'flowFile' object may already have data written to it.
                if (!valid || event == null || !event.isValid()) {
                    FlowFile invalidFlowFile = session.create();
                    invalidFlowFile = session.putAllAttributes(invalidFlowFile, defaultAttributes);
                    if (sender != null) {
                        invalidFlowFile = session.putAttribute(invalidFlowFile, SyslogAttributes.SYSLOG_SENDER.key(), sender);
                    }

                    try {
                        final byte[] rawBytes = rawSyslogEvent.getData();
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
                attributes.put(SyslogAttributes.SYSLOG_PRIORITY.key(), event.getPriority());
                attributes.put(SyslogAttributes.SYSLOG_SEVERITY.key(), event.getSeverity());
                attributes.put(SyslogAttributes.SYSLOG_FACILITY.key(), event.getFacility());
                attributes.put(SyslogAttributes.SYSLOG_VERSION.key(), event.getVersion());
                attributes.put(SyslogAttributes.SYSLOG_TIMESTAMP.key(), event.getTimeStamp());
                attributes.put(SyslogAttributes.SYSLOG_HOSTNAME.key(), event.getHostName());
                attributes.put(SyslogAttributes.SYSLOG_BODY.key(), event.getMsgBody());
                attributes.put(SyslogAttributes.SYSLOG_VALID.key(), String.valueOf(event.isValid()));

                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            // figure out if we should write the bytes from the raw event or parsed event
            final boolean writeDemarcator = (i > 0);

            try {
                // write the raw bytes of the message as the FlowFile content
                final byte[] rawMessage = (event == null) ? rawSyslogEvent.getData() : event.getRawMessage();
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
            newAttributes.put(SyslogAttributes.SYSLOG_SENDER.key(), sender);
            flowFile = session.putAllAttributes(flowFile, newAttributes);

            getLogger().debug("Transferring {} to success", new Object[] {flowFile});
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
            final String transitUri = new StringBuilder().append(protocol.toLowerCase()).append("://").append(senderHost).append(":").append(port).toString();
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    /**
     * Wrapper class to pass around the raw message and the host/ip that sent it
     */
    static class RawSyslogEvent<C extends SelectableChannel> implements Event<C> {

        final byte[] rawMessage;
        final String sender;

        public RawSyslogEvent(final byte[] rawMessage, final String sender) {
            this.rawMessage = rawMessage;
            this.sender = sender;
        }

        @Override
        public byte[] getData() {
            return this.rawMessage;
        }

        @Override
        public String getSender() {
            return this.sender;
        }

        @Override
        public ChannelResponder getResponder() {
            return null;
        }
    }

    /**
     * EventFactory implementation for RawSyslogEvent.
     */
    private static class RawSyslogEventFactory implements EventFactory<RawSyslogEvent> {

        @Override
        public RawSyslogEvent create(byte[] data, Map<String, String> metadata, final ChannelResponder responder) {
            String sender = null;
            if (metadata != null && metadata.containsKey(EventFactory.SENDER_KEY)) {
                sender = metadata.get(EventFactory.SENDER_KEY);
            }
            return new RawSyslogEvent(data, sender);
        }
    }

}
