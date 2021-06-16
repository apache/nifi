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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.ByteBufferFactory;
import org.apache.nifi.processor.util.listen.dispatcher.ByteBufferPool;
import org.apache.nifi.processor.util.listen.dispatcher.ByteBufferSource;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.event.StandardEventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.handler.socket.SocketChannelHandlerFactory;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "tcp", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection using a line separator " +
        "as the message demarcator. The default behavior is for each message to produce a single FlowFile, however this can " +
        "be controlled by increasing the Batch Size to a larger value for higher throughput. The Receive Buffer Size must be " +
        "set as large as the largest messages expected to be received, meaning if every 100kb there is a line separator, then " +
        "the Receive Buffer Size must be greater than 100kb.")
@WritesAttributes({
        @WritesAttribute(attribute="tcp.sender", description="The sending host of the messages."),
        @WritesAttribute(attribute="tcp.port", description="The sending port the messages were received.")
})
public class ListenTCP extends AbstractListenEventBatchingProcessor<StandardEvent> {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.REQUIRED.name())
            .build();

    public static final PropertyDescriptor MAX_RECV_THREAD_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("max-receiving-threads")
            .displayName("Max Number of Receiving Message Handler Threads")
            .description(
                    "The maximum number of threads might be available for handling receiving messages ready all the time. " +
                    "Cannot be bigger than the \"Max Number of TCP Connections\". " +
                    "If not set, the value of \"Max Number of TCP Connections\" will be used.")
            .addValidator(StandardValidators.createLongValidator(1, 65535, true))
            .required(false)
            .build();

    protected static final PropertyDescriptor POOL_RECV_BUFFERS = new PropertyDescriptor.Builder()
            .name("pool-receive-buffers")
            .displayName("Pool Receive Buffers")
            .description(
                    "When turned on, the processor uses pre-populated pool of buffers when receiving messages. " +
                    "This is prepared during initialisation of the processor. " +
                    "With high value of Max Number of TCP Connections and Receive Buffer Size this strategy might allocate significant amount of memory! " +
                    "When turned off, the byte buffers will be created on demand and be destroyed after use.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                MAX_CONNECTIONS,
                MAX_RECV_THREAD_POOL_SIZE,
                POOL_RECV_BUFFERS,
                SSL_CONTEXT_SERVICE,
                CLIENT_AUTH
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        final int maxConnections = validationContext.getProperty(MAX_CONNECTIONS).asInteger();

        if (validationContext.getProperty(MAX_RECV_THREAD_POOL_SIZE).isSet()) {
            final int maxPoolSize = validationContext.getProperty(MAX_RECV_THREAD_POOL_SIZE).asInteger();

            if (maxPoolSize > maxConnections) {
                results.add(new ValidationResult.Builder()
                        .explanation("\"" + MAX_RECV_THREAD_POOL_SIZE.getDisplayName() + "\" cannot be bigger than \"" + MAX_CONNECTIONS.getDisplayName() + "\"")
                        .valid(false)
                        .subject(MAX_RECV_THREAD_POOL_SIZE.getDisplayName())
                        .build());
            }
        }

        return results;
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<StandardEvent> events)
            throws IOException {

        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final int maxThreadPoolSize = context.getProperty(MAX_RECV_THREAD_POOL_SIZE).isSet()
                ? context.getProperty(MAX_RECV_THREAD_POOL_SIZE).asInteger()
                : maxConnections;

        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final Charset charSet = Charset.forName(context.getProperty(CHARSET).getValue());

        // initialize the buffer pool based on max number of connections and the buffer size
        final ByteBufferSource byteBufferSource = context.getProperty(POOL_RECV_BUFFERS).asBoolean()
                ? new ByteBufferPool(maxConnections, bufferSize)
                : new ByteBufferFactory(bufferSize);

        // if an SSLContextService was provided then create an SSLContext to pass down to the dispatcher
        SSLContext sslContext = null;
        ClientAuth clientAuth = null;

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createContext();
            clientAuth = ClientAuth.valueOf(clientAuthValue);
        }

        final EventFactory<StandardEvent> eventFactory = new StandardEventFactory();
        final ChannelHandlerFactory<StandardEvent<SocketChannel>, AsyncChannelDispatcher> handlerFactory = new SocketChannelHandlerFactory<>();
        return new SocketChannelDispatcher(eventFactory, handlerFactory, byteBufferSource, events, getLogger(), maxConnections,
                maxThreadPoolSize, sslContext, clientAuth, charSet);
    }

    @Override
    protected Map<String, String> getAttributes(final FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final Map<String,String> attributes = new HashMap<>(3);
        attributes.put("tcp.sender", sender);
        attributes.put("tcp.port", String.valueOf(port));
        return attributes;
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("tcp").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

}
