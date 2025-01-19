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
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.processors.JMSConsumer.JMSResponse;
import org.apache.nifi.jms.processors.ioconcept.writer.FlowFileWriter;
import org.apache.nifi.jms.processors.ioconcept.writer.FlowFileWriterCallback;
import org.apache.nifi.jms.processors.ioconcept.writer.record.OutputStrategy;
import org.apache.nifi.jms.processors.ioconcept.writer.record.RecordWriter;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import jakarta.jms.Session;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
@SeeAlso(value = { PublishJMS.class, JMSConnectionFactoryProvider.class })
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Client Library Location can reference resources over HTTP"
                )
        }
)
public class ConsumeJMS extends AbstractJMSProcessor<JMSConsumer> {

    public static final String JMS_MESSAGETYPE = "jms.messagetype";

    private final static String COUNTER_PARSE_FAILURES = "Parse Failures";
    private final static String COUNTER_RECORDS_RECEIVED = "Records Received";
    private final static String COUNTER_RECORDS_PROCESSED = "Records Processed";

    static final AllowableValue AUTO_ACK = new AllowableValue(String.valueOf(Session.AUTO_ACKNOWLEDGE),
            "AUTO_ACKNOWLEDGE (" + Session.AUTO_ACKNOWLEDGE + ")",
            "Automatically acknowledges a client's receipt of a message, regardless if NiFi session has been commited. "
                    + "Can result in data loss in the event where NiFi abruptly stopped before session was commited.");

    static final AllowableValue CLIENT_ACK = new AllowableValue(String.valueOf(Session.CLIENT_ACKNOWLEDGE),
            "CLIENT_ACKNOWLEDGE (" + Session.CLIENT_ACKNOWLEDGE + ")",
            "(DEFAULT) Manually acknowledges a client's receipt of a message after NiFi Session was commited, thus ensuring no data loss");

    static final AllowableValue DUPS_OK = new AllowableValue(String.valueOf(Session.DUPS_OK_ACKNOWLEDGE),
            "DUPS_OK_ACKNOWLEDGE (" + Session.DUPS_OK_ACKNOWLEDGE + ")",
            "This acknowledgment mode instructs the session to lazily acknowledge the delivery of messages. May result in both data "
                    + "duplication and data loss while achieving the best throughput.");

    public static final String JMS_SOURCE_DESTINATION_NAME = "jms.source.destination";

    static final PropertyDescriptor MESSAGE_SELECTOR = new PropertyDescriptor.Builder()
            .name("Message Selector")
            .displayName("Message Selector")
            .description("The JMS Message Selector to filter the messages that the processor will receive")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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
                         "@see https://jakarta.ee/specifications/platform/9/apidocs/jakarta/jms/session#createDurableConsumer-jakarta.jms.Topic-java.lang.String-")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SHARED_SUBSCRIBER = new PropertyDescriptor.Builder()
            .name("Shared subscription")
            .displayName("Shared Subscription")
            .description("If destination is Topic if present then make it the consumer shared. " +
                         "@see https://jakarta.ee/specifications/platform/9/apidocs/jakarta/jms/session#createSharedConsumer-jakarta.jms.Topic-java.lang.String-")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SUBSCRIPTION_NAME = new PropertyDescriptor.Builder()
            .name("Subscription Name")
            .description("The name of the subscription to use if destination is Topic and is shared or durable.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .description("How long to wait to consume a message from the remote broker before giving up.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor ERROR_QUEUE = new PropertyDescriptor.Builder()
            .name("Error Queue Name")
            .description("The name of a JMS Queue where - if set - unprocessed messages will be routed. Usually provided by the administrator (e.g., 'queue://myErrorQueue' or 'myErrorQueue')." +
                "Only applicable if 'Destination Type' is set to 'QUEUE'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_READER)
            .description("The Record Reader to use for parsing received JMS Messages into Records.")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_WRITER)
            .description("The Record Writer to use for serializing Records before writing them to a FlowFile.")
            .build();

    static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("output-strategy")
            .displayName("Output Strategy")
            .description("The format used to output the JMS message into a FlowFile record.")
            .dependsOn(RECORD_READER)
            .required(true)
            .defaultValue(OutputStrategy.USE_VALUE.getValue())
            .allowableValues(OutputStrategy.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the JMS Destination are routed to this relationship")
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message cannot be parsed using the configured Record Reader, the contents of the "
                    + "message will be routed to this Relationship as its own individual FlowFile.")
            .autoTerminateDefault(true) // to make sure flow are still valid after upgrades
            .build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_PARSE_FAILURE
    );

    private static final PropertyDescriptor CHARSET_WITH_EL_VALIDATOR_PROPERTY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CHARSET)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR_WITH_EVALUATION)
            .build();

    private static final List<PropertyDescriptor> OTHER_PROPERTIES = Stream.concat(
            JNDI_JMS_CF_PROPERTIES.stream(),
            JMS_CF_PROPERTIES.stream()
    ).toList();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            Stream.of(
                    CF_SERVICE,
                    DESTINATION,
                    DESTINATION_TYPE,
                    MESSAGE_SELECTOR,
                    USER,
                    PASSWORD,
                    CLIENT_ID,
                    CHARSET_WITH_EL_VALIDATOR_PROPERTY,
                    ACKNOWLEDGEMENT_MODE,
                    DURABLE_SUBSCRIBER,
                    SHARED_SUBSCRIBER,
                    SUBSCRIPTION_NAME,
                    TIMEOUT,
                    MAX_BATCH_SIZE,
                    ERROR_QUEUE,
                    RECORD_READER,
                    RECORD_WRITER,
                    OUTPUT_STRATEGY
            ),
            OTHER_PROPERTIES.stream()
    ).toList();

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);

        if (!config.hasProperty(MAX_BATCH_SIZE)) {
            if (config.isPropertySet(BASE_RECORD_READER)) {
                config.setProperty(MAX_BATCH_SIZE, "10000");
            }
        }
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
            if (context.getProperty(RECORD_READER).isSet()) {
                processMessagesAsRecords(context, processSession, consumer, destinationName, errorQueueName, durable, shared, subscriptionName, messageSelector, charset);
            } else {
                processMessages(context, processSession, consumer, destinationName, errorQueueName, durable, shared, subscriptionName, messageSelector, charset);
            }
        } catch (Exception e) {
            getLogger().error("Error while trying to process JMS message", e);
            consumer.setValid(false);
            context.yield();
            throw e;
        }
    }

    private void processMessages(ProcessContext context, ProcessSession processSession, JMSConsumer consumer, String destinationName, String errorQueueName,
                                 boolean durable, boolean shared, String subscriptionName, String messageSelector, String charset) {

        int batchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        consumer.consumeMessageSet(destinationName, errorQueueName, durable, shared, subscriptionName, messageSelector, charset, batchSize, jmsResponses -> {
            jmsResponses.forEach(response -> {
                try {
                    final FlowFile flowFile = createFlowFileFromMessage(processSession, destinationName, response);

                    processSession.getProvenanceReporter().receive(flowFile, destinationName);
                    processSession.transfer(flowFile, REL_SUCCESS);
                    processSession.commitAsync(
                            () -> withLog(() -> acknowledge(response)),
                            __ -> withLog(() -> response.reject()));
                } catch (final Throwable t) {
                    response.reject();
                    throw t;
                }
            });
        });
    }

    private FlowFile createFlowFileFromMessage(ProcessSession processSession, String destinationName, JMSResponse response) {
        FlowFile flowFile = processSession.create();
        flowFile = processSession.write(flowFile, out -> out.write(response.getMessageBody()));

        final Map<String, String> jmsHeaders = response.getMessageHeaders();
        final Map<String, String> jmsProperties = response.getMessageProperties();
        Map<String, String> attributes = mergeJmsAttributes(jmsHeaders, jmsProperties);
        attributes.put(JMS_SOURCE_DESTINATION_NAME, destinationName);
        attributes.put(JMS_MESSAGETYPE, response.getMessageType());
        return processSession.putAllAttributes(flowFile, attributes);
    }

    private void processMessagesAsRecords(ProcessContext context, ProcessSession session, JMSConsumer consumer, String destinationName, String errorQueueName,
                                          boolean durable, boolean shared, String subscriptionName, String messageSelector, String charset) {

        int batchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final OutputStrategy outputStrategy = OutputStrategy.valueOf(context.getProperty(OUTPUT_STRATEGY).getValue());

        final FlowFileWriter<JMSResponse> flowFileWriter = new RecordWriter<>(
                readerFactory,
                writerFactory,
                message -> message.getMessageBody() == null ? new byte[0] : message.getMessageBody(),
                message -> mergeJmsAttributes(message.getMessageHeaders(), message.getMessageProperties()),
                outputStrategy,
                getLogger()
        );

        consumer.consumeMessageSet(destinationName, errorQueueName, durable, shared, subscriptionName, messageSelector, charset, batchSize, jmsResponses -> {
            flowFileWriter.write(session, jmsResponses, new FlowFileWriterCallback<>() {
                @Override
                public void onSuccess(FlowFile flowFile, List<JMSResponse> processedMessages, List<JMSResponse> failedMessages) {
                    session.getProvenanceReporter().receive(flowFile, destinationName);
                    session.adjustCounter(COUNTER_RECORDS_RECEIVED, processedMessages.size() + failedMessages.size(), false);
                    session.adjustCounter(COUNTER_RECORDS_PROCESSED, processedMessages.size(), false);

                    session.transfer(flowFile, REL_SUCCESS);
                    session.commitAsync(
                            () -> withLog(() -> acknowledge(processedMessages, failedMessages)),
                            __ -> withLog(() -> reject(processedMessages, failedMessages))
                    );
                }

                @Override
                public void onParseFailure(FlowFile flowFile, JMSResponse message, Exception e) {
                    session.adjustCounter(COUNTER_PARSE_FAILURES, 1, false);

                    final FlowFile failedMessage = createFlowFileFromMessage(session, destinationName, message);
                    session.transfer(failedMessage, REL_PARSE_FAILURE);
                }

                @Override
                public void onFailure(FlowFile flowFile, List<JMSResponse> processedMessages, List<JMSResponse> failedMessages, Exception e) {
                    reject(processedMessages, failedMessages);
                    // It would be nicer to call rollback and yield here, but we are rethrowing the exception to have the same error handling with processSingleMessage.
                    throw new ProcessException(e);
                }
            });
        });
    }

    private void acknowledge(final JMSResponse response) {
        try {
            response.acknowledge();
        } catch (final Exception e) {
            getLogger().error("Failed to acknowledge JMS Message that was received", e);
            throw new ProcessException(e);
        }
    }

    private void acknowledge(final List<JMSResponse> processedMessages, final List<JMSResponse> failedMessages) {
        acknowledge(findLastBatchedJmsResponse(processedMessages, failedMessages));
    }

    private void reject(final List<JMSResponse> processedMessages, final List<JMSResponse> failedMessages) {
        findLastBatchedJmsResponse(processedMessages, failedMessages).reject();
    }

    private void withLog(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            getLogger().error("An error happened during commitAsync callback", e);
            throw e;
        }
    }

    private JMSResponse findLastBatchedJmsResponse(List<JMSResponse> processedMessages, List<JMSResponse> failedMessages) {
        return Stream.of(processedMessages, failedMessages).flatMap(Collection::stream).max(Comparator.comparing(JMSResponse::getBatchOrder)).get();
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
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * <p>
     * Use provided clientId for non shared durable consumers, if not set
     * always a different value as defined in {@link AbstractJMSProcessor#setClientId(ProcessContext, SingleConnectionFactory)}.
     * </p>
     * See {@link Session#createDurableConsumer(jakarta.jms.Topic, String, String, boolean)},
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

    private Map<String, String> mergeJmsAttributes(Map<String, String> headers, Map<String, String> properties) {
        final Map<String, String> jmsAttributes = new HashMap<>(headers);
        properties.forEach((key, value) -> {
            if (jmsAttributes.containsKey(key)) {
                getLogger().warn("JMS Header and Property name collides as an attribute. JMS Property will override the JMS Header attribute. attributeName=[{}]", key);
            }
            jmsAttributes.put(key, value);
        });

        return jmsAttributes;
    }
}
