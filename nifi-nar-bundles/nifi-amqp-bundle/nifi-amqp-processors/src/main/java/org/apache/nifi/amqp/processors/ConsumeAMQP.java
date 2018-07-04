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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

@Tags({"amqp", "rabbit", "get", "message", "receive", "consume"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes AMQP Messages from an AMQP Broker using the AMQP 0.9.1 protocol. Each message that is received from the AMQP Broker will be "
    + "emitted as its own FlowFile to the 'success' relationship.")
@WritesAttributes({
    @WritesAttribute(attribute = "amqp$appId", description = "The App ID field from the AMQP Message"),
    @WritesAttribute(attribute = "amqp$contentEncoding", description = "The Content Encoding reported by the AMQP Message"),
    @WritesAttribute(attribute = "amqp$contentType", description = "The Content Type reported by the AMQP Message"),
    @WritesAttribute(attribute = "amqp$headers", description = "The headers present on the AMQP Message"),
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
})
public class ConsumeAMQP extends AbstractAMQPProcessor<AMQPConsumer> {
    private static final String ATTRIBUTES_PREFIX = "amqp$";

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
        .name("Queue")
        .description("The name of the existing AMQP Queue from which messages will be consumed. Usually pre-defined by AMQP administrator. ")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor AUTO_ACKNOWLEDGE = new PropertyDescriptor.Builder()
        .name("auto.acknowledge")
        .displayName("Auto-Acknowledge messages")
        .description("If true, messages that are received will be auto-acknowledged by the AMQP Broker. "
            + "This generally will provide better throughput but could result in messages being lost upon restart of NiFi")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("batch.size")
        .displayName("Batch Size")
        .description("The maximum number of messages that should be pulled in a single session. Once this many messages have been received (or once no more messages are readily available), "
            + "the messages received will be transferred to the 'success' relationship and the messages will be acknowledged with the AMQP Broker. Setting this value to a larger number "
            + "could result in better performance, particularly for very small messages, but can also result in more messages being duplicated upon sudden restart of NiFi.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("10")
        .required(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are received from the AMQP queue are routed to this relationship")
        .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private static final Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QUEUE);
        properties.add(AUTO_ACKNOWLEDGE);
        properties.add(BATCH_SIZE);
        properties.addAll(getCommonPropertyDescriptors());
        propertyDescriptors = Collections.unmodifiableList(properties);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rels);
    }

    /**
     * Will construct a {@link FlowFile} containing the body of the consumed AMQP message (if {@link GetResponse} returned by {@link AMQPConsumer} is
     * not null) and AMQP properties that came with message which are added to a {@link FlowFile} as attributes, transferring {@link FlowFile} to
     * 'success' {@link Relationship}.
     */
    @Override
    protected void processResource(final Connection connection, final AMQPConsumer consumer, final ProcessContext context, final ProcessSession session) {
        GetResponse lastReceived = null;

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
            final Map<String, String> attributes = buildAttributes(amqpProperties);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().receive(flowFile, connection.toString() + "/" + context.getProperty(QUEUE).getValue());
            session.transfer(flowFile, REL_SUCCESS);
            lastReceived = response;
        }

        session.commit();

        if (lastReceived != null) {
            try {
                consumer.acknowledge(lastReceived);
            } catch (IOException e) {
                throw new ProcessException("Failed to acknowledge message", e);
            }
        }
    }

    private Map<String, String> buildAttributes(final BasicProperties properties) {
        final Map<String, String> attributes = new HashMap<>();
        addAttribute(attributes, ATTRIBUTES_PREFIX + "appId", properties.getAppId());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "contentEncoding", properties.getContentEncoding());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "contentType", properties.getContentType());
        addAttribute(attributes, ATTRIBUTES_PREFIX + "headers", properties.getHeaders());
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
        return attributes;
    }

    private void addAttribute(final Map<String, String> attributes, final String attributeName, final Object value) {
        if (value == null) {
            return;
        }

        attributes.put(attributeName, value.toString());
    }

    @Override
    protected synchronized AMQPConsumer createAMQPWorker(final ProcessContext context, final Connection connection) {
        try {
            final String queueName = context.getProperty(QUEUE).getValue();
            final boolean autoAcknowledge = context.getProperty(AUTO_ACKNOWLEDGE).asBoolean();
            final AMQPConsumer amqpConsumer = new AMQPConsumer(connection, queueName, autoAcknowledge);

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
