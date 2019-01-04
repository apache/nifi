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
package org.apache.nifi.jms.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.processors.JMSConsumer.ConsumerCallback;
import org.apache.nifi.jms.processors.JMSConsumer.JMSResponse;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import javax.jms.Session;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Consuming JMS processor which upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method will construct a
 * {@link FlowFile} containing the body of the consumed JMS message and JMS
 * properties that came with message which are added to a {@link FlowFile} as
 * attributes.
 */
@Tags({ "jms", "get", "message", "receive", "consume" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes JMS Message of type BytesMessage or TextMessage transforming its content to "
        + "a FlowFile and transitioning it to 'success' relationship. JMS attributes such as headers and properties will be copied as FlowFile attributes.")
@WritesAttributes({
        @WritesAttribute(attribute = JmsHeaders.DELIVERY_MODE, description = "The JMSDeliveryMode from the message header."),
        @WritesAttribute(attribute = JmsHeaders.EXPIRATION, description = "The JMSExpiration from the message header."),
        @WritesAttribute(attribute = JmsHeaders.PRIORITY, description = "The JMSPriority from the message header."),
        @WritesAttribute(attribute = JmsHeaders.REDELIVERED, description = "The JMSRedelivered from the message header."),
        @WritesAttribute(attribute = JmsHeaders.TIMESTAMP, description = "The JMSTimestamp from the message header."),
        @WritesAttribute(attribute = JmsHeaders.CORRELATION_ID, description = "The JMSCorrelationID from the message header."),
        @WritesAttribute(attribute = JmsHeaders.MESSAGE_ID, description = "The JMSMessageID from the message header."),
        @WritesAttribute(attribute = JmsHeaders.TYPE, description = "The JMSType from the message header."),
        @WritesAttribute(attribute = JmsHeaders.REPLY_TO, description = "The JMSReplyTo from the message header."),
        @WritesAttribute(attribute = JmsHeaders.DESTINATION, description = "The JMSDestination from the message header."),
        @WritesAttribute(attribute = "other attributes", description = "Each message property is written to an attribute.")
})
@SeeAlso(value = { PublishJMS.class, JMSConnectionFactoryProvider.class })
public class ConsumeJMS extends AbstractJMSProcessor<JMSConsumer> {

    static final AllowableValue AUTO_ACK = new AllowableValue(String.valueOf(Session.AUTO_ACKNOWLEDGE),
            "AUTO_ACKNOWLEDGE (" + String.valueOf(Session.AUTO_ACKNOWLEDGE) + ")",
            "Automatically acknowledges a client's receipt of a message, regardless if NiFi session has been commited. "
                    + "Can result in data loss in the event where NiFi abruptly stopped before session was commited.");

    static final AllowableValue CLIENT_ACK = new AllowableValue(String.valueOf(Session.CLIENT_ACKNOWLEDGE),
            "CLIENT_ACKNOWLEDGE (" + String.valueOf(Session.CLIENT_ACKNOWLEDGE) + ")",
            "(DEFAULT) Manually acknowledges a client's receipt of a message after NiFi Session was commited, thus ensuring no data loss");

    static final AllowableValue DUPS_OK = new AllowableValue(String.valueOf(Session.DUPS_OK_ACKNOWLEDGE),
            "DUPS_OK_ACKNOWLEDGE (" + String.valueOf(Session.DUPS_OK_ACKNOWLEDGE) + ")",
            "This acknowledgment mode instructs the session to lazily acknowledge the delivery of messages. May result in both data "
                    + "duplication and data loss while achieving the best throughput.");

    public static final String JMS_SOURCE_DESTINATION_NAME = "jms.source.destination";

    static final PropertyDescriptor ACKNOWLEDGEMENT_MODE = new PropertyDescriptor.Builder()
            .name("Acknowledgement Mode")
            .description("The JMS Acknowledgement Mode. Using Auto Acknowledge can cause messages to be lost on restart of NiFi but may provide "
                            + "better performance than Client Acknowledge.")
            .required(true)
            .allowableValues(AUTO_ACK, CLIENT_ACK, DUPS_OK)
            .defaultValue(CLIENT_ACK.getValue())
            .build();

    static final PropertyDescriptor DURABLE_SUBSCRIBER = new PropertyDescriptor.Builder()
            .name("Durable subscription")
            .description("If destination is Topic if present then make it the consumer durable. " +
                         "@see https://docs.oracle.com/javaee/7/api/javax/jms/Session.html#createDurableConsumer-javax.jms.Topic-java.lang.String-")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SHARED_SUBSCRIBER = new PropertyDescriptor.Builder()
            .name("Shared subscription")
            .description("If destination is Topic if present then make it the consumer shared. " +
                         "@see https://docs.oracle.com/javaee/7/api/javax/jms/Session.html#createSharedConsumer-javax.jms.Topic-java.lang.String-")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SUBSCRIPTION_NAME = new PropertyDescriptor.Builder()
            .name("Subscription Name")
            .description("The name of the subscription to use if destination is Topic and is shared or durable.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .description("How long to wait to consume a message from the remote broker before giving up.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the JMS Destination are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> thisPropertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(propertyDescriptors);
        _propertyDescriptors.remove(MESSAGE_BODY);

        // change the validator on CHARSET property
        _propertyDescriptors.remove(CHARSET);
        PropertyDescriptor CHARSET_WITH_EL_VALIDATOR_PROPERTY = new PropertyDescriptor.Builder().fromPropertyDescriptor(CHARSET)
                .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR_WITH_EVALUATION)
                .build();
        _propertyDescriptors.add(CHARSET_WITH_EL_VALIDATOR_PROPERTY);

        _propertyDescriptors.add(ACKNOWLEDGEMENT_MODE);
        _propertyDescriptors.add(DURABLE_SUBSCRIBER);
        _propertyDescriptors.add(SHARED_SUBSCRIBER);
        _propertyDescriptors.add(SUBSCRIPTION_NAME);
        _propertyDescriptors.add(TIMEOUT);
        thisPropertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Will construct a {@link FlowFile} containing the body of the consumed JMS
     * message (if {@link JMSResponse} returned by {@link JMSConsumer} is not
     * null) and JMS properties that came with message which are added to a
     * {@link FlowFile} as attributes, transferring {@link FlowFile} to
     * 'success' {@link Relationship}.
     */
    @Override
    protected void rendezvousWithJms(final ProcessContext context, final ProcessSession processSession, final JMSConsumer consumer) throws ProcessException {
        final String destinationName = context.getProperty(DESTINATION).evaluateAttributeExpressions().getValue();
        final Boolean durableBoolean = context.getProperty(DURABLE_SUBSCRIBER).evaluateAttributeExpressions().asBoolean();
        final boolean durable = durableBoolean == null ? false : durableBoolean;
        final Boolean sharedBoolean = context.getProperty(SHARED_SUBSCRIBER).evaluateAttributeExpressions().asBoolean();
        final boolean shared = sharedBoolean == null ? false : sharedBoolean;
        final String subscriptionName = context.getProperty(SUBSCRIPTION_NAME).evaluateAttributeExpressions().getValue();
        final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();

        consumer.consume(destinationName, durable, shared, subscriptionName, charset, new ConsumerCallback() {
            @Override
            public void accept(final JMSResponse response) {
                if (response == null) {
                    return;
                }

                FlowFile flowFile = processSession.create();
                flowFile = processSession.write(flowFile, out -> out.write(response.getMessageBody()));

                final Map<String, String> jmsHeaders = response.getMessageHeaders();
                final Map<String, String> jmsProperties = response.getMessageProperties();

                flowFile = ConsumeJMS.this.updateFlowFileAttributesWithJMSAttributes(jmsHeaders, flowFile, processSession);
                flowFile = ConsumeJMS.this.updateFlowFileAttributesWithJMSAttributes(jmsProperties, flowFile, processSession);
                flowFile = processSession.putAttribute(flowFile, JMS_SOURCE_DESTINATION_NAME, destinationName);

                processSession.getProvenanceReporter().receive(flowFile, destinationName);
                processSession.transfer(flowFile, REL_SUCCESS);
                processSession.commit();
            }
        });
    }

    /**
     * Will create an instance of {@link JMSConsumer}
     */
    @Override
    protected JMSConsumer finishBuildingJmsWorker(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ProcessContext processContext) {
        int ackMode = processContext.getProperty(ACKNOWLEDGEMENT_MODE).asInteger();
        jmsTemplate.setSessionAcknowledgeMode(ackMode);

        long timeout = processContext.getProperty(TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        jmsTemplate.setReceiveTimeout(timeout);

        return new JMSConsumer(connectionFactory, jmsTemplate, this.getLogger());
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return thisPropertyDescriptors;
    }

    /**
     * Copies JMS attributes (i.e., headers and properties) as FF attributes.
     * Given that FF attributes mandate that values are of type String, the
     * copied values of JMS attributes will be "stringified" via
     * String.valueOf(attribute).
     */
    private FlowFile updateFlowFileAttributesWithJMSAttributes(Map<String, String> jmsAttributes, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<>();
        for (Entry<String, String> entry : jmsAttributes.entrySet()) {
            attributes.put(entry.getKey(), entry.getValue());
        }

        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }
}
