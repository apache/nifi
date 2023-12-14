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
package org.apache.nifi.amqp.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"amqp", "rabbit", "get", "message", "receive", "consume"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes AMQP Messages from an AMQP Broker using the AMQP 0.9.1 protocol. Each message that is received from the AMQP Broker will be "
    + "emitted as its own FlowFile to the 'success' relationship.")
@WritesAttributes({
    @WritesAttribute(attribute = "amqp$appId", description = "The App ID field from the AMQP Message"),
    @WritesAttribute(attribute = "amqp$contentEncoding", description = "The Content Encoding reported by the AMQP Message"),
    @WritesAttribute(attribute = "amqp$contentType", description = "The Content Type reported by the AMQP Message"),
    @WritesAttribute(attribute = "amqp$headers", description = "The headers present on the AMQP Message. Added only if processor is configured to output this attribute."),
    @WritesAttribute(attribute = "<Header Key Prefix>.<attribute>",
        description = "Each message header will be inserted with this attribute name, if processor is configured to output headers as attribute"),
    @WritesAttribute(attribute = "amqp$deliveryMode", description = "The numeric indicator for the Message's Delivery Mode"),
    @WritesAttribute(attribute = "amqp$priority", description = "The Message priority"),
    @WritesAttribute(attribute = "amqp$correlationId", description = "The Message's Correlation ID"),
    @WritesAttribute(attribute = "amqp$replyTo", description = "The value of the Message's Reply-To field"),
    @WritesAttribute(attribute = "amqp$expiration", description = "The Message Expiration"),
    @WritesAttribute(attribute = "amqp$messageId", description = "The unique ID of the Message"),
    @WritesAttribute(attribute = "amqp$timestamp", description = "The timestamp of the Message, as the number of milliseconds since epoch"),
    @WritesAttribute(attribute = "amqp$type", description = "The type of message"),
    @WritesAttribute(attribute = "amqp$userId", description = "The ID of the user"),
    @WritesAttribute(attribute = "amqp$clusterId", description = "The ID of the AMQP Cluster"),
    @WritesAttribute(attribute = "amqp$routingKey", description = "The routingKey of the AMQP Message"),
    @WritesAttribute(attribute = "amqp$exchange", description = "The exchange from which AMQP Message was received")
})
public class ConsumeAMQP extends AbstractAMQPProcessor<AMQPConsumer> {

    private static final String ATTRIBUTES_PREFIX = "amqp$";
    public static final String DEFAULT_HEADERS_KEY_PREFIX = "consume.amqp";

    public static final AllowableValue HEADERS_FORMAT_COMMA_SEPARATED_STRING = new AllowableValue("Comma-Separated String", "Comma-Separated String",
            "Put all headers as a string with the specified separator in the attribute 'amqp$headers'.");
    public static final AllowableValue HEADERS_FORMAT_JSON_STRING = new AllowableValue("JSON String", "JSON String",
            "Format all headers as JSON string and output in the attribute 'amqp$headers'. It will include keys with null value as well.");
    public static final AllowableValue HEADERS_FORMAT_ATTRIBUTES = new AllowableValue("FlowFile Attributes", "FlowFile Attributes",
            "Put each header as attribute of the flow file with a prefix specified in the properties");

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
        .name("Queue")
        .description("The name of the existing AMQP Queue from which messages will be consumed. Usually pre-defined by AMQP administrator. ")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor AUTO_ACKNOWLEDGE = new PropertyDescriptor.Builder()
        .name("auto.acknowledge")
        .displayName("Auto-Acknowledge Messages")
        .description(" If false (Non-Auto-Acknowledge), the messages will be acknowledged by the processor after transferring the FlowFiles to success and committing "
            + "the NiFi session. Non-Auto-Acknowledge mode provides 'at-least-once' delivery semantics. "
            + "If true (Auto-Acknowledge), messages that are delivered to the AMQP Client will be auto-acknowledged by the AMQP Broker just after sending them out. "
            + "This generally will provide better throughput but will also result in messages being lost upon restart/crash of the AMQP Broker, NiFi or the processor. "
            + "Auto-Acknowledge mode provides 'at-most-once' delivery semantics and it is recommended only if loosing messages is acceptable.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("batch.size")
        .displayName("Batch Size")
        .description("The maximum number of messages that should be processed in a single session. Once this many messages have been received (or once no more messages are readily available), "
            + "the messages received will be transferred to the 'success' relationship and the messages will be acknowledged to the AMQP Broker. Setting this value to a larger number "
            + "could result in better performance, particularly for very small messages, but can also result in more messages being duplicated upon sudden restart of NiFi.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("10")
        .required(true)
        .build();
    static final PropertyDescriptor PREFETCH_COUNT = new PropertyDescriptor.Builder()
        .name("prefetch.count")
        .displayName("Prefetch Count")
        .description("The maximum number of unacknowledged messages for the consumer. If consumer has this number of unacknowledged messages, AMQP broker will "
               + "no longer send new messages until consumer acknowledges some of the messages already delivered to it."
               + "Allowed values: from 0 to 65535. 0 means no limit")
        .addValidator(StandardValidators.createLongValidator(0, 65535, true))
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("0")
        .required(true)
        .build();

    public static final PropertyDescriptor HEADER_FORMAT = new PropertyDescriptor.Builder()
        .name("header.format")
        .displayName("Header Output Format")
        .description("Defines how to output headers from the received message")
        .allowableValues(HEADERS_FORMAT_COMMA_SEPARATED_STRING, HEADERS_FORMAT_JSON_STRING, HEADERS_FORMAT_ATTRIBUTES)
        .defaultValue(HEADERS_FORMAT_COMMA_SEPARATED_STRING.getValue())
        .required(true)
        .build();
    public static final PropertyDescriptor HEADER_KEY_PREFIX = new PropertyDescriptor.Builder()
        .name("header.key.prefix")
        .displayName("Header Key Prefix")
        .description("Text to be prefixed to header keys as the are added to the FlowFile attributes. Processor will append '.' to the value of this property")
        .defaultValue(DEFAULT_HEADERS_KEY_PREFIX)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .dependsOn(HEADER_FORMAT, HEADERS_FORMAT_ATTRIBUTES)
        .required(true)
        .build();

    public static final PropertyDescriptor HEADER_SEPARATOR = new PropertyDescriptor.Builder()
        .name("header.separator")
        .displayName("Header Separator")
        .description("The character that is used to separate key-value for header in String. The value must be only one character."
                )
        .addValidator(StandardValidators.SINGLE_CHAR_VALIDATOR)
        .defaultValue(",")
        .dependsOn(HEADER_FORMAT, HEADERS_FORMAT_COMMA_SEPARATED_STRING)
        .required(false)
        .build();
    static final PropertyDescriptor REMOVE_CURLY_BRACES = new PropertyDescriptor.Builder()
        .name("remove.curly.braces")
        .displayName("Remove Curly Braces")
        .description("If true Remove Curly Braces, Curly Braces in the header will be automatically remove.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("False")
        .allowableValues("True", "False")
        .dependsOn(HEADER_FORMAT, HEADERS_FORMAT_COMMA_SEPARATED_STRING)
        .required(false)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are received from the AMQP queue are routed to this relationship")
        .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private static final Set<Relationship> relationships;

    private static final ObjectMapper objectMapper;

    static {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QUEUE);
        properties.add(AUTO_ACKNOWLEDGE);
        properties.add(BATCH_SIZE);
        properties.add(PREFETCH_COUNT);
        properties.add(HEADER_FORMAT);
        properties.add(HEADER_KEY_PREFIX);
        properties.add(HEADER_SEPARATOR);
        properties.add(REMOVE_CURLY_BRACES);
        properties.addAll(getCommonPropertyDescriptors());
        propertyDescriptors = Collections.unmodifiableList(properties);

        relationships = Set.of(REL_SUCCESS);

        objectMapper = new ObjectMapper();
    }

    /**
     * Will construct a {@link FlowFile} containing the body of the consumed AMQP message (if {@link GetResponse} returned by {@link AMQPConsumer} is
     * not null) and AMQP properties that came with message which are added to a {@link FlowFile} as attributes, transferring {@link FlowFile} to
     * 'success' {@link Relationship}.
     */
    @Override
    protected void processResource(final Connection connection, final AMQPConsumer consumer, final ProcessContext context, final ProcessSession session) {
        GetResponse lastReceived = null;

        if (!connection.isOpen() || !consumer.getChannel().isOpen()) {
            throw new AMQPException("AMQP client has lost connection.");
        }

        for (int i = 0; i < context.getProperty(BATCH_SIZE).asInteger(); i++) {
            final GetResponse response = consumer.consume();
            if (response == null) {
                if (lastReceived == null) {
                    // If no messages received, then yield.
                    context.yield();
                }

                break;
            }

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> out.write(response.getBody()));

            final BasicProperties amqpProperties = response.getProps();
            final Envelope envelope = response.getEnvelope();
            final String headerFormat = context.getProperty(HEADER_FORMAT).getValue();
            final String headerKeyPrefix = context.getProperty(HEADER_KEY_PREFIX).getValue();
            final Map<String, String> attributes = buildAttributes(amqpProperties, envelope, headerFormat, headerKeyPrefix,
                    context.getProperty(REMOVE_CURLY_BRACES).asBoolean(), context.getProperty(HEADER_SEPARATOR).toString());
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().receive(flowFile, connection.toString() + "/" + context.getProperty(QUEUE).getValue());
            session.transfer(flowFile, REL_SUCCESS);
            lastReceived = response;
        }

        if (lastReceived != null) {
            final GetResponse finalGetResponse = lastReceived;
            session.commitAsync(() -> consumer.acknowledge(finalGetResponse), null);
        }
    }

    private Map<String, String> buildAttributes(final BasicProperties properties, final Envelope envelope, String headersStringFormat, String headerAttributePrefix, boolean removeCurlyBraces,
                                                String valueSeparatorForHeaders) {
        AllowableValue headerFormat = new AllowableValue(headersStringFormat);
        final Map<String, String> attributes = new HashMap<>();
        addAttribute(attributes, ATTRIBUTES_PREFIX + "appId", properties.getAppId());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "contentEncoding", properties.getContentEncoding());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "contentType", properties.getContentType());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "deliveryMode", properties.getDeliveryMode());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "priority", properties.getPriority());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "correlationId", properties.getCorrelationId());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "replyTo", properties.getReplyTo());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "expiration", properties.getExpiration());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "messageId", properties.getMessageId());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "timestamp", properties.getTimestamp() == null ? null : properties.getTimestamp().getTime());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "type", properties.getType());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "userId", properties.getUserId());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "clusterId", properties.getClusterId());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "routingKey", envelope.getRoutingKey());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "exchange", envelope.getExchange());
        Map<String, Object> headers = properties.getHeaders();
        if (headers != null) {
            if (HEADERS_FORMAT_ATTRIBUTES.equals(headerFormat)) {
                headers.forEach((key, value) -> addAttribute(attributes,
                        String.format("%s.%s", headerAttributePrefix, key), value));
            } else {
                addAttribute(attributes, ATTRIBUTES_PREFIX + "headers",
                        buildHeaders(properties.getHeaders(), headerFormat, removeCurlyBraces,
                                valueSeparatorForHeaders));
            }
        }
        return attributes;
    }

    private void addAttribute(final Map<String, String> attributes, final String attributeName, final Object value) {
        if (value == null) {
            return;
        }

        attributes.put(attributeName, value.toString());
    }

    private String buildHeaders(Map<String, Object> headers, AllowableValue headerFormat, boolean removeCurlyBraces, String valueSeparatorForHeaders) {
        if (headers == null) {
            return null;
        }
        String headerString = null;
        if (headerFormat.equals(HEADERS_FORMAT_COMMA_SEPARATED_STRING)) {
            headerString = convertMapToString(headers, valueSeparatorForHeaders);

            if (!removeCurlyBraces) {
                headerString = "{" + headerString + "}";
            }
        } else if (headerFormat.equals(HEADERS_FORMAT_JSON_STRING)) {
            try {
                headerString = convertMapToJSONString(headers);
            } catch (JsonProcessingException e) {
                getLogger().warn("Header formatting as JSON failed", e);
            }
        }
        return headerString;
    }

    private static String convertMapToString(Map<String, Object> headers, String valueSeparatorForHeaders) {
        return headers.entrySet().stream().map(e -> (e.getValue()!= null) ? e.getKey() + "=" + e.getValue(): e.getKey())
                .collect(Collectors.joining(valueSeparatorForHeaders));
    }

    private static String convertMapToJSONString(Map<String, Object> headers) throws JsonProcessingException {
        return objectMapper.writeValueAsString(headers);
    }

    @Override
    protected synchronized AMQPConsumer createAMQPWorker(final ProcessContext context, final Connection connection) {
        try {
            final String queueName = context.getProperty(QUEUE).getValue();
            final boolean autoAcknowledge = context.getProperty(AUTO_ACKNOWLEDGE).asBoolean();
            final int prefetchCount =  context.getProperty(PREFETCH_COUNT).asInteger();
            final AMQPConsumer amqpConsumer = new AMQPConsumer(connection, queueName, autoAcknowledge, prefetchCount, getLogger());

            return amqpConsumer;
        } catch (final IOException ioe) {
            throw new ProcessException("Failed to connect to AMQP Broker", ioe);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
}
