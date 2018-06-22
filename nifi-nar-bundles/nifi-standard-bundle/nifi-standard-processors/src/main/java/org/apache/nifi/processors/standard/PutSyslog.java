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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.processor.util.put.sender.DatagramChannelSender;
import org.apache.nifi.processor.util.put.sender.SSLSocketChannelSender;
import org.apache.nifi.processor.util.put.sender.SocketChannelSender;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.syslog.parsers.SyslogParser;
import org.apache.nifi.util.StopWatch;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerWhenEmpty
@Tags({"syslog", "put", "udp", "tcp", "logs"})
@CapabilityDescription("Sends Syslog messages to a given host and port over TCP or UDP. Messages are constructed from the \"Message ___\" properties of the processor " +
        "which can use expression language to generate messages from incoming FlowFiles. The properties are used to construct messages of the form: " +
        "(<PRIORITY>)(VERSION )(TIMESTAMP) (HOSTNAME) (BODY) where version is optional.  The constructed messages are checked against regular expressions for " +
        "RFC5424 and RFC3164 formatted messages. The timestamp can be an RFC5424 timestamp with a format of \"yyyy-MM-dd'T'HH:mm:ss.SZ\" or \"yyyy-MM-dd'T'HH:mm:ss.S+hh:mm\", " +
        "or it can be an RFC3164 timestamp with a format of \"MMM d HH:mm:ss\". If a message is constructed that does not form a valid Syslog message according to the " +
        "above description, then it is routed to the invalid relationship. Valid messages are sent to the Syslog server and successes are routed to the success relationship, " +
        "failures routed to the failure relationship.")
@SeeAlso({ListenSyslog.class, ParseSyslog.class})
public class PutSyslog extends AbstractSyslogProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The ip address or hostname of the Syslog server. Note that Expression language is not evaluated per FlowFile.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_SOCKET_SEND_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Send Buffer")
            .description("The maximum size of the socket send buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped. Note that Expression language is not evaluated per FlowFile.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size")
            .description("The number of incoming FlowFiles to process in a single execution of this processor. Note that Expression language is not evaluated per FlowFile.")
            .required(true)
            .defaultValue("25")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor IDLE_EXPIRATION = new PropertyDescriptor
            .Builder().name("Idle Connection Expiration")
            .description("The amount of time a connection should be held open without being used before closing the connection. Note that Expression language is not evaluated per FlowFile.")
            .required(true)
            .defaultValue("5 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor MSG_PRIORITY = new PropertyDescriptor
            .Builder().name("Message Priority")
            .description("The priority for the Syslog messages, excluding < >.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_VERSION = new PropertyDescriptor
            .Builder().name("Message Version")
            .description("The version for the Syslog messages.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_TIMESTAMP = new PropertyDescriptor
            .Builder().name("Message Timestamp")
            .description("The timestamp for the Syslog messages. The timestamp can be an RFC5424 timestamp with a format of " +
                    "\"yyyy-MM-dd'T'HH:mm:ss.SZ\" or \"yyyy-MM-dd'T'HH:mm:ss.S+hh:mm\", \" or it can be an RFC3164 timestamp " +
                    "with a format of \"MMM d HH:mm:ss\".")
            .required(true)
            .defaultValue("${now():format('MMM d HH:mm:ss')}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_HOSTNAME = new PropertyDescriptor
            .Builder().name("Message Hostname")
            .description("The hostname for the Syslog messages.")
            .required(true)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_BODY = new PropertyDescriptor
            .Builder().name("Message Body")
            .description("The body for the Syslog messages.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, syslog " +
                    "messages will be sent over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to Syslog are sent out this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to Syslog are sent out this relationship.")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that do not form a valid Syslog message are sent out this relationship.")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private volatile BlockingQueue<ChannelSender> senderPool;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PROTOCOL);
        descriptors.add(PORT);
        descriptors.add(MAX_SOCKET_SEND_BUFFER_SIZE);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(IDLE_EXPIRATION);
        descriptors.add(TIMEOUT);
        descriptors.add(BATCH_SIZE);
        descriptors.add(CHARSET);
        descriptors.add(MSG_PRIORITY);
        descriptors.add(MSG_VERSION);
        descriptors.add(MSG_TIMESTAMP);
        descriptors.add(MSG_HOSTNAME);
        descriptors.add(MSG_BODY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String protocol = context.getProperty(PROTOCOL).getValue();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (UDP_VALUE.getValue().equals(protocol) && sslContextService != null) {
            results.add(new ValidationResult.Builder()
                    .explanation("SSL can not be used with UDP")
                    .valid(false).subject("SSL Context").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        // initialize the queue of senders, one per task, senders will get created on the fly in onTrigger
        this.senderPool = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());
    }

    protected ChannelSender createSender(final ProcessContext context) throws IOException {
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final int maxSendBuffer = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).evaluateAttributeExpressions().asDataSize(DataUnit.B).intValue();
        final int timeout = context.getProperty(TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        return createSender(sslContextService, protocol, host, port, maxSendBuffer, timeout);
    }

    // visible for testing to override and provide a mock sender if desired
    protected ChannelSender createSender(final SSLContextService sslContextService, final String protocol, final String host,
                                         final int port, final int maxSendBufferSize, final int timeout)
            throws IOException {

        ChannelSender sender;
        if (protocol.equals(UDP_VALUE.getValue())) {
            sender = new DatagramChannelSender(host, port, maxSendBufferSize, getLogger());
        } else {
            // if an SSLContextService is provided then we make a secure sender
            if (sslContextService != null) {
                final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
                sender = new SSLSocketChannelSender(host, port, maxSendBufferSize, sslContext, getLogger());
            } else {
                sender = new SocketChannelSender(host, port, maxSendBufferSize, getLogger());
            }
        }
        sender.setTimeout(timeout);
        sender.open();
        return sender;
    }

    @OnStopped
    public void onStopped() {
        if (senderPool != null) {
            ChannelSender sender = senderPool.poll();
            while (sender != null) {
                sender.close();
                sender = senderPool.poll();
            }
        }
    }

    private PruneResult pruneIdleSenders(final long idleThreshold){
        int numClosed = 0;
        int numConsidered = 0;

        long currentTime = System.currentTimeMillis();
        final List<ChannelSender> putBack = new ArrayList<>();

        // if a connection hasn't been used with in the threshold then it gets closed
        ChannelSender sender;
        while ((sender = senderPool.poll()) != null) {
            numConsidered++;
            if (currentTime > (sender.getLastUsed() + idleThreshold)) {
                getLogger().debug("Closing idle connection...");
                sender.close();
                numClosed++;
            } else {
                putBack.add(sender);
            }
        }

        // re-queue senders that weren't idle, but if the queue is full then close the sender
        for (ChannelSender putBackSender : putBack) {
            boolean returned = senderPool.offer(putBackSender);
            if (!returned) {
                putBackSender.close();
            }
        }

        return new PruneResult(numClosed, numConsidered);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.isEmpty()) {
            final PruneResult result = pruneIdleSenders(context.getProperty(IDLE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).longValue());
            // yield if we closed an idle connection, or if there were no connections in the first place
            if (result.getNumClosed() > 0 || (result.getNumClosed() == 0 && result.getNumConsidered() == 0)) {
                context.yield();
            }
            return;
        }

        // get a sender from the pool, or create a new one if the pool is empty
        // if we can't create a new connection then route flow files to failure and yield
        ChannelSender sender = senderPool.poll();
        if (sender == null) {
            try {
                getLogger().debug("No available connections, creating a new one...");
                sender = createSender(context);
            } catch (IOException e) {
                for (final FlowFile flowFile : flowFiles) {
                    getLogger().error("No available connections, and unable to create a new one, transferring {} to failure",
                            new Object[]{flowFile}, e);
                    session.transfer(flowFile, REL_FAILURE);
                }
                context.yield();
                return;
            }
        }

        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String transitUri = new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
        final AtomicReference<IOException> exceptionHolder = new AtomicReference<>(null);
        final Charset charSet = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions().getValue());

        try {
            for (FlowFile flowFile : flowFiles) {
                final StopWatch timer = new StopWatch(true);
                final String priority = context.getProperty(MSG_PRIORITY).evaluateAttributeExpressions(flowFile).getValue();
                final String version = context.getProperty(MSG_VERSION).evaluateAttributeExpressions(flowFile).getValue();
                final String timestamp = context.getProperty(MSG_TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
                final String hostname = context.getProperty(MSG_HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
                final String body = context.getProperty(MSG_BODY).evaluateAttributeExpressions(flowFile).getValue();

                final StringBuilder messageBuilder = new StringBuilder();
                messageBuilder.append("<").append(priority).append(">");
                if (version != null) {
                    messageBuilder.append(version).append(" ");
                }
                messageBuilder.append(timestamp).append(" ").append(hostname).append(" ").append(body);

                final String fullMessage = messageBuilder.toString();
                getLogger().debug(fullMessage);

                if (isValid(fullMessage)) {
                    try {
                        // now that we validated, add a new line if doing TCP
                        if (protocol.equals(TCP_VALUE.getValue())) {
                            messageBuilder.append('\n');
                        }

                        sender.send(messageBuilder.toString(), charSet);
                        timer.stop();

                        final long duration = timer.getDuration(TimeUnit.MILLISECONDS);
                        session.getProvenanceReporter().send(flowFile, transitUri, duration, true);

                        getLogger().info("Transferring {} to success", new Object[]{flowFile});
                        session.transfer(flowFile, REL_SUCCESS);
                    } catch (IOException e) {
                        getLogger().error("Transferring {} to failure", new Object[]{flowFile}, e);
                        session.transfer(flowFile, REL_FAILURE);
                        exceptionHolder.set(e);
                    }
                } else {
                    getLogger().info("Transferring {} to invalid", new Object[]{flowFile});
                    session.transfer(flowFile, REL_INVALID);
                }
            }
        } finally {
            // if the connection is still open and no IO errors happened then try to return, if pool is full then close
            if (sender.isConnected() && exceptionHolder.get() == null) {
                boolean returned = senderPool.offer(sender);
                if (!returned) {
                    sender.close();
                }
            } else {
                // probably already closed here, but quietly close anyway to be safe
                sender.close();
            }

        }

    }

    private boolean isValid(final String message) {
        for (Pattern pattern : SyslogParser.MESSAGE_PATTERNS) {
            Matcher matcher = pattern.matcher(message);
            if (matcher.matches()) {
                return true;
            }
        }
        return false;
    }

    private static class PruneResult {

        private final int numClosed;

        private final int numConsidered;

        public PruneResult(final int numClosed, final int numConsidered) {
            this.numClosed = numClosed;
            this.numConsidered = numConsidered;
        }

        public int getNumClosed() {
            return numClosed;
        }

        public int getNumConsidered() {
            return numConsidered;
        }

    }

}
