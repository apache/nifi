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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.DatagramChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.event.StandardEventFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

@SupportsBatching
@Tags({"ingest", "udp", "listen", "source"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Listens for Datagram Packets on a given port. The default behavior produces a FlowFile " +
        "per datagram, however for higher throughput the Max Batch Size property may be increased to specify the number of " +
        "datagrams to batch together in a single FlowFile. This processor can be restricted to listening for datagrams from  a " +
        "specific remote host and port by specifying the Sending Host and Sending Host Port properties, otherwise it will listen " +
        "for datagrams from all hosts and ports.")
@WritesAttributes({
        @WritesAttribute(attribute="udp.sender", description="The sending host of the messages."),
        @WritesAttribute(attribute="udp.port", description="The sending port the messages were received.")
})
public class ListenUDP extends AbstractListenEventBatchingProcessor<StandardEvent> {

    public static final PropertyDescriptor SENDING_HOST = new PropertyDescriptor.Builder()
            .name("Sending Host")
            .description("IP, or name, of a remote host. Only Datagrams from the specified Sending Host Port and this host will "
                    + "be accepted. Improves Performance. May be a system property or an environment variable.")
            .addValidator(new HostValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SENDING_HOST_PORT = new PropertyDescriptor.Builder()
            .name("Sending Host Port")
            .description("Port being used by remote host to send Datagrams. Only Datagrams from the specified Sending Host and "
                    + "this port will be accepted. Improves Performance. May be a system property or an environment variable.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final String UDP_PORT_ATTR = "udp.port";
    public static final String UDP_SENDER_ATTR = "udp.sender";

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                SENDING_HOST,
                SENDING_HOST_PORT
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> result = new ArrayList<>();

        final String sendingHost = validationContext.getProperty(SENDING_HOST).getValue();
        final String sendingPort = validationContext.getProperty(SENDING_HOST_PORT).getValue();

        if (StringUtils.isBlank(sendingHost) && StringUtils.isNotBlank(sendingPort)) {
            result.add(
                    new ValidationResult.Builder()
                            .subject(SENDING_HOST.getName())
                            .valid(false)
                            .explanation("Must specify Sending Host when specifying Sending Host Port")
                            .build());
        } else if (StringUtils.isBlank(sendingPort) && StringUtils.isNotBlank(sendingHost)) {
            result.add(
                    new ValidationResult.Builder()
                            .subject(SENDING_HOST_PORT.getName())
                            .valid(false)
                            .explanation("Must specify Sending Host Port when specifying Sending Host")
                            .build());
        }

        return result;
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<StandardEvent> events)
            throws IOException {
        final String sendingHost = context.getProperty(SENDING_HOST).evaluateAttributeExpressions().getValue();
        final Integer sendingHostPort = context.getProperty(SENDING_HOST_PORT).evaluateAttributeExpressions().asInteger();
        final Integer bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(context.getMaxConcurrentTasks(), bufferSize);
        final EventFactory<StandardEvent> eventFactory = new StandardEventFactory();
        return new DatagramChannelDispatcher<>(eventFactory, bufferPool, events, getLogger(), sendingHost, sendingHostPort);
    }

    @Override
    protected Map<String, String> getAttributes(final FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final Map<String,String> attributes = new HashMap<>(3);
        attributes.put(UDP_SENDER_ATTR, sender);
        attributes.put(UDP_PORT_ATTR, String.valueOf(port));
        return attributes;
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("udp").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

    public static class HostValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            try {
                InetAddress.getByName(input);
                return new ValidationResult.Builder().subject(subject).valid(true).input(input).build();
            } catch (final UnknownHostException e) {
                return new ValidationResult.Builder().subject(subject).valid(false).input(input).explanation("Unknown host: " + e).build();
            }
        }

    }

}
