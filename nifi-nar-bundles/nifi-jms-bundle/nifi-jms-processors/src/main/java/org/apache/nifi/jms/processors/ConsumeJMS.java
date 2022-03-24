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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import javax.jms.Session;
import java.util.ArrayList;
import java.util.Collection;
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
@CapabilityDescription("Consumes JMS Message of type BytesMessage, TextMessage, ObjectMessage, MapMessage or StreamMessage transforming its content to "
        + "a FlowFile and transitioning it to 'success' relationship. JMS attributes such as headers and properties will be copied as FlowFile attributes. "
        + "MapMessages will be transformed into JSONs and then into byte arrays. The other types will have their raw contents as byte array transferred into the flowfile.")
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
        @WritesAttribute(attribute = ConsumeJMS.JMS_MESSAGETYPE, description = "The JMS message type, can be TextMessage, BytesMessage, ObjectMessage, MapMessage or StreamMessage)."),
        @WritesAttribute(attribute = "other attributes", description = "Each message property is written to an attribute.")
})
@DynamicProperty(name = "The name of a Connection Factory configuration property.", value = "The value of a given Connection Factory configuration property.",
        description = "Additional configuration property for the Connection Factory. It can be used when the Connection Factory is being configured via the 'JNDI *' or the 'JMS *'" +
                "properties of the processor. For more information, see the Additional Details page.",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@SeeAlso(value = { PublishJMS.class, JMSConnectionFactoryProvider.class })
public class ConsumeJMS extends AbstractJMSProcessor<JMSConsumer> {
    public static final String JMS_MESSAGETYPE = "jms.messagetype";

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

    static final PropertyDescriptor MESSAGE_SELECTOR = new PropertyDescriptor.Builder()
            .name("Message Selector")
            .displayName("Message Selector")
            .description("The JMS Message Selector to filter the messages that the processor will receive")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

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
            .displayName("Durable Subscription")
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
            .displayName("Shared Subscription")
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
    static final PropertyDescriptor ERROR_QUEUE = new PropertyDescriptor.Builder()
            .name("Error Queue Name")
            .description("The name of a JMS Queue where - if set - unprocessed messages will be routed. Usually provided by the administrator (e.g., 'queue://myErrorQueue' or 'myErrorQueue')." +
                "Only applicable if 'Destination Type' is set to 'QUEUE'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the JMS Destination are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();

        _propertyDescriptors.add(CF_SERVICE);
        _propertyDescriptors.add(DESTINATION);
        _propertyDescriptors.add(DESTINATION_TYPE);
        _propertyDescriptors.add(MESSAGE_SELECTOR);
        _propertyDescriptors.add(USER);
        _propertyDescriptors.add(PASSWORD);
        _propertyDescriptors.add(CLIENT_ID);
        _propertyDescriptors.add(SESSION_CACHE_SIZE);

        // change the validator on CHARSET property
        PropertyDescriptor charsetWithELValidatorProperty = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(CHARSET)
                .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR_WITH_EVALUATION)
                .build();
        _propertyDescriptors.add(charsetWithELValidatorProperty);

        _propertyDescriptors.add(ACKNOWLEDGEMENT_MODE);
        _propertyDescriptors.add(DURABLE_SUBSCRIBER);
        _propertyDescriptors.add(SHARED_SUBSCRIBER);
        _propertyDescriptors.add(SUBSCRIPTION_NAME);
        _propertyDescriptors.add(TIMEOUT);
        _propertyDescriptors.add(ERROR_QUEUE);

        _propertyDescriptors.addAll(JNDI_JMS_CF_PROPERTIES);
        _propertyDescriptors.addAll(JMS_CF_PROPERTIES);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private static boolean isDurableSubscriber(final ProcessContext context) {
        final Boolean durableBoolean = context.getProperty(DURABLE_SUBSCRIBER).evaluateAttributeExpressions().asBoolean();
        return durableBoolean == null ? false : durableBoolean;
    }

    private static boolean isShared(final ProcessContext context) {
        final Boolean sharedBoolean = context.getProperty(SHARED_SUBSCRIBER).evaluateAttributeExpressions().asBoolean();
        return sharedBoolean == null ? false : sharedBoolean;
    }

    @OnScheduled
    public void onSchedule(ProcessContext context) {
        if (context.getMaxConcurrentTasks() > 1 && isDurableSubscriber(context) && !isShared(context)) {
            throw new ProcessException("Durable non shared subscriptions cannot work on multiple threads. Check javax/jms/Session#createDurableConsumer API doc.");
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        String destinationType = validationContext.getProperty(DESTINATION_TYPE).getValue();
        String errorQueue = validationContext.getProperty(ERROR_QUEUE).getValue();

        if (errorQueue != null && !QUEUE.equals(destinationType)) {
            validationResults.add(new ValidationResult.Builder()
                .valid(false)
                .subject(ERROR_QUEUE.getDisplayName())
                .explanation("'" + ERROR_QUEUE.getDisplayName() + "' is applicable only when " +
                    "'" + DESTINATION_TYPE.getDisplayName() + "'='" + QUEUE + "'")
                .build());
        }
        return validationResults;
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
        final String errorQueueName = context.getProperty(ERROR_QUEUE).evaluateAttributeExpressions().getValue();
        final boolean durable = isDurableSubscriber(context);
        final boolean shared = isShared(context);
        final String subscriptionName = context.getProperty(SUBSCRIPTION_NAME).evaluateAttributeExpressions().getValue();
        final String messageSelector = context.getProperty(MESSAGE_SELECTOR).evaluateAttributeExpressions().getValue();
        final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();

        try {
            consumer.consume(destinationName, errorQueueName, durable, shared, subscriptionName, messageSelector, charset, new ConsumerCallback() {
                @Override
                public void accept(final JMSResponse response) {
                    if (response == null) {
                        return;
                    }

                    try {
                        FlowFile flowFile = processSession.create();
                        flowFile = processSession.write(flowFile, out -> out.write(response.getMessageBody()));

                        final Map<String, String> jmsHeaders = response.getMessageHeaders();
                        final Map<String, String> jmsProperties = response.getMessageProperties();

                        flowFile = ConsumeJMS.this.updateFlowFileAttributesWithJMSAttributes(jmsHeaders, flowFile, processSession);
                        flowFile = ConsumeJMS.this.updateFlowFileAttributesWithJMSAttributes(jmsProperties, flowFile, processSession);
                        flowFile = processSession.putAttribute(flowFile, JMS_SOURCE_DESTINATION_NAME, destinationName);

                        processSession.getProvenanceReporter().receive(flowFile, destinationName);
                        processSession.putAttribute(flowFile, JMS_MESSAGETYPE, response.getMessageType());
                        processSession.transfer(flowFile, REL_SUCCESS);

                        processSession.commitAsync(() -> acknowledge(response), throwable -> response.reject());
                    } catch (final Throwable t) {
                        response.reject();
                        throw t;
                    }
                }
            });
        } catch(Exception e) {
            consumer.setValid(false);
            context.yield();
            throw e; // for backward compatibility with exception handling in flows
        }
    }

    private void acknowledge(final JMSResponse response) {
        try {
            response.acknowledge();
        } catch (final Exception e) {
            getLogger().error("Failed to acknowledge JMS Message that was received", e);
            throw new ProcessException(e);
        }
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
        return propertyDescriptors;
    }

    /**
     * <p>
     * Use provided clientId for non shared durable consumers, if not set
     * always a different value as defined in {@link AbstractJMSProcessor#setClientId(ProcessContext, SingleConnectionFactory)}.
     * </p>
     * See {@link Session#createDurableConsumer(javax.jms.Topic, String, String, boolean)},
     * in special following part: <i>An unshared durable subscription is
     * identified by a name specified by the client and by the client identifier,
     * which must be set. An application which subsequently wishes to create
     * a consumer on that unshared durable subscription must use the same
     * client identifier.</i>
     */
    @Override
    protected void setClientId(ProcessContext context, SingleConnectionFactory cachingFactory) {
        if (isDurableSubscriber(context) && !isShared(context)) {
            cachingFactory.setClientId(getClientId(context));
        } else {
            super.setClientId(context, cachingFactory);
        }
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
