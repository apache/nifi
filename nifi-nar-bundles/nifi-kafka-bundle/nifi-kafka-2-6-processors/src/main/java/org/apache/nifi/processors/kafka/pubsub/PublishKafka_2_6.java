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

package org.apache.nifi.processors.kafka.pubsub;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.attribute.StandardTransitUriProvider;
import org.apache.nifi.kafka.shared.component.KafkaPublishComponent;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.kafka.shared.property.provider.KafkaPropertyProvider;
import org.apache.nifi.kafka.shared.property.provider.StandardKafkaPropertyProvider;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.kafka.shared.validation.DynamicPropertyValidator;
import org.apache.nifi.kafka.shared.validation.KafkaClientCustomValidationFunction;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED;

@Tags({"Apache", "Kafka", "Put", "Send", "Message", "PubSub", "2.6"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka using the Kafka 2.6 Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line. "
    + "The complementary NiFi processor for fetching messages is ConsumeKafka_2_6.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
    description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
@ReadsAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TOMBSTONE, description = "If this attribute is set to 'true', if the processor is not configured "
        + "with a demarcator and if the FlowFile's content is null, then a tombstone message with zero bytes will be sent to Kafka.")
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Kafka for this FlowFile. This attribute is added only to "
    + "FlowFiles that are routed to success. If the <Message Demarcator> Property is not set, this will always be 1, but if the Property is set, it may "
    + "be greater than 1.")
public class PublishKafka_2_6 extends AbstractProcessor implements KafkaPublishComponent, VerifiableProcessor {
    protected static final String MSG_COUNT = "msg.count";

    static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("all", "Guarantee Replicated Delivery",
        "FlowFile will be routed to failure unless the message is replicated to the appropriate "
            + "number of Kafka Nodes according to the Topic configuration");
    static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery",
        "FlowFile will be routed to success if the message is received by a single Kafka node, "
            + "whether or not it is replicated. This is faster than <Guarantee Replicated Delivery> "
            + "but can result in data loss if a Kafka node crashes");
    static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort",
        "FlowFile will be routed to success after successfully sending the content to a Kafka node, "
            + "without waiting for any acknowledgment from the node at all. This provides the best performance but may result in data loss.");

    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue(Partitioners.RoundRobinPartitioner.class.getName(),
        Partitioners.RoundRobinPartitioner.class.getSimpleName(),
        "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
            + "the next Partition to Partition 2, and so on, wrapping as necessary.");
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("org.apache.kafka.clients.producer.internals.DefaultPartitioner",
        "DefaultPartitioner", "The default partitioning strategy will choose the sticky partition that changes when the batch is full "
                + "(See KIP-480 for details about sticky partitioning).");
    static final AllowableValue EXPRESSION_LANGUAGE_PARTITIONING = new AllowableValue(Partitioners.ExpressionLanguagePartitioner.class.getName(), "Expression Language Partitioner",
        "Interprets the <Partition> property as Expression Language that will be evaluated against each FlowFile. This Expression will be evaluated once against the FlowFile, " +
            "so all Records in a given FlowFile will go to the same partition.");

    static final AllowableValue UTF8_ENCODING = new AllowableValue("utf-8", "UTF-8 Encoded", "The key is interpreted as a UTF-8 Encoded string.");
    static final AllowableValue HEX_ENCODING = new AllowableValue("hex", "Hex Encoded",
        "The key is interpreted as arbitrary binary data that is encoded using hexadecimal characters with uppercase letters.");

    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
        .name("topic")
        .displayName("Topic Name")
        .description("The name of the Kafka Topic to publish to.")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
        .name(ProducerConfig.ACKS_CONFIG)
        .displayName("Delivery Guarantee")
        .description("Specifies the requirement for guaranteeing that a message is sent to Kafka. Corresponds to Kafka's 'acks' property.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
        .defaultValue(DELIVERY_REPLICATED)
        .build();

    static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
        .name(ProducerConfig.MAX_BLOCK_MS_CONFIG)
        .displayName("Max Metadata Wait Time")
        .description("The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the "
            + "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .defaultValue("5 sec")
        .build();

    static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
        .name("ack.wait.time")
        .displayName("Acknowledgment Wait Time")
        .description("After sending a message to Kafka, this indicates the amount of time that we are willing to wait for a response from Kafka. "
            + "If Kafka does not acknowledge the message within this time period, the FlowFile will be routed to 'failure'.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .defaultValue("5 secs")
        .build();

    static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder()
        .name("max.request.size")
        .displayName("Max Request Size")
        .description("The maximum size of a request in bytes. Corresponds to Kafka's 'max.request.size' property and defaults to 1 MB (1048576).")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .build();

    static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
        .name("kafka-key")
        .displayName("Kafka Key")
        .description("The Key to use for the Message. "
            + "If not specified, the flow file attribute 'kafka.key' is used as the message key, if it is present."
            + "Beware that setting Kafka key and demarcating at the same time may potentially lead to many Kafka messages with the same key."
            + "Normally this is not a problem as Kafka does not enforce or assume message and key uniqueness. Still, setting the demarcator and Kafka key at the same time poses a risk of "
            + "data loss on Kafka. During a topic compaction on Kafka, messages will be deduplicated based on this key.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
        .name("key-attribute-encoding")
        .displayName("Key Attribute Encoding")
        .description("FlowFiles that are emitted have an attribute named '" + KafkaFlowFileAttribute.KAFKA_KEY + "'. This property dictates how the value of the attribute should be encoded.")
        .required(true)
        .defaultValue(UTF8_ENCODING)
        .allowableValues(UTF8_ENCODING, HEX_ENCODING)
        .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
        .name("message-demarcator")
        .displayName("Message Demarcator")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
            + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
            + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka message. "
            + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
        .build();

    static final PropertyDescriptor PARTITION_CLASS = new PropertyDescriptor.Builder()
        .name(ProducerConfig.PARTITIONER_CLASS_CONFIG)
        .displayName("Partitioner class")
        .description("Specifies which class to use to compute a partition id for a message. Corresponds to Kafka's 'partitioner.class' property.")
        .allowableValues(ROUND_ROBIN_PARTITIONING, RANDOM_PARTITIONING, EXPRESSION_LANGUAGE_PARTITIONING)
        .defaultValue(RANDOM_PARTITIONING)
        .required(false)
        .build();

    static final PropertyDescriptor PARTITION = new PropertyDescriptor.Builder()
        .name("partition")
        .displayName("Partition")
        .description("Specifies which Partition Records will go to.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
        .name(ProducerConfig.COMPRESSION_TYPE_CONFIG)
        .displayName("Compression Type")
        .description("This parameter allows you to specify the compression codec for all data generated by this producer.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues("none", "gzip", "snappy", "lz4")
        .defaultValue("none")
        .build();

    static final PropertyDescriptor ATTRIBUTE_NAME_REGEX = new PropertyDescriptor.Builder()
        .name("attribute-name-regex")
        .displayName("Attributes to Send as Headers (Regex)")
        .description("A Regular Expression that is matched against all FlowFile attribute names. "
            + "Any attribute whose name matches the regex will be added to the Kafka messages as a Header. "
            + "If not specified, no FlowFile attributes will be added as headers.")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .build();
    static final PropertyDescriptor USE_TRANSACTIONS = new PropertyDescriptor.Builder()
        .name("use-transactions")
        .displayName("Use Transactions")
        .description("Specifies whether or not NiFi should provide Transactional guarantees when communicating with Kafka. If there is a problem sending data to Kafka, "
            + "and this property is set to false, then the messages that have already been sent to Kafka will continue on and be delivered to consumers. "
            + "If this is set to true, then the Kafka transaction will be rolled back so that those messages are not available to consumers. Setting this to true "
            + "requires that the <Delivery Guarantee> property be set to \"Guarantee Replicated Delivery.\"")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    static final PropertyDescriptor TRANSACTIONAL_ID_PREFIX = new PropertyDescriptor.Builder()
        .name("transactional-id-prefix")
        .displayName("Transactional Id Prefix")
        .description("When Use Transaction is set to true, KafkaProducer config 'transactional.id' will be a generated UUID and will be prefixed with this string.")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .dependsOn(USE_TRANSACTIONS, "true")
        .required(false)
        .build();
    static final PropertyDescriptor MESSAGE_HEADER_ENCODING = new PropertyDescriptor.Builder()
        .name("message-header-encoding")
        .displayName("Message Header Encoding")
        .description("For any attribute that is added as a message header, as configured via the <Attributes to Send as Headers> property, "
            + "this property indicates the Character Encoding to use for serializing the headers.")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .required(false)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles for which all content was sent to Kafka.")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            BOOTSTRAP_SERVERS,
            TOPIC,
            USE_TRANSACTIONS,
            TRANSACTIONAL_ID_PREFIX,
            MESSAGE_DEMARCATOR,
            FAILURE_STRATEGY,
            DELIVERY_GUARANTEE,
            ATTRIBUTE_NAME_REGEX,
            MESSAGE_HEADER_ENCODING,
            SECURITY_PROTOCOL,
            SASL_MECHANISM,
            SELF_CONTAINED_KERBEROS_USER_SERVICE,
            KERBEROS_SERVICE_NAME,
            SASL_USERNAME,
            SASL_PASSWORD,
            AWS_PROFILE_NAME,
            TOKEN_AUTHENTICATION,
            SSL_CONTEXT_SERVICE,
            KEY,
            KEY_ATTRIBUTE_ENCODING,
            MAX_REQUEST_SIZE,
            ACK_WAIT_TIME,
            METADATA_WAIT_TIME,
            PARTITION_CLASS,
            PARTITION,
            COMPRESSION_CODEC
    );
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    private volatile PublisherPool publisherPool = null;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
            .name(propertyDescriptorName)
            .addValidator(new DynamicPropertyValidator(ProducerConfig.class))
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final KafkaClientCustomValidationFunction validationFunction = new KafkaClientCustomValidationFunction();
        final Collection<ValidationResult> results = validationFunction.apply(validationContext);

        final boolean useTransactions = validationContext.getProperty(USE_TRANSACTIONS).asBoolean();
        if (useTransactions) {
            final String deliveryGuarantee = validationContext.getProperty(DELIVERY_GUARANTEE).getValue();
            if (!DELIVERY_REPLICATED.getValue().equals(deliveryGuarantee)) {
                results.add(new ValidationResult.Builder()
                    .subject("Delivery Guarantee")
                    .valid(false)
                    .explanation("In order to use Transactions, the Delivery Guarantee must be \"Guarantee Replicated Delivery.\" "
                        + "Either change the <Use Transactions> property or the <Delivery Guarantee> property.")
                    .build());
            }
        }

        final String partitionClass = validationContext.getProperty(PARTITION_CLASS).getValue();
        if (EXPRESSION_LANGUAGE_PARTITIONING.getValue().equals(partitionClass)) {
            final String rawRecordPath = validationContext.getProperty(PARTITION).getValue();
            if (rawRecordPath == null) {
                results.add(new ValidationResult.Builder()
                    .subject("Partition")
                    .valid(false)
                    .explanation("The <Partition> property must be specified if using the Expression Language Partitioning class")
                    .build());
            }
        }

        return results;
    }

    private synchronized PublisherPool getPublisherPool(final ProcessContext context) {
        PublisherPool pool = publisherPool;
        if (pool != null) {
            return pool;
        }

        return publisherPool = createPublisherPool(context);
    }

    protected PublisherPool createPublisherPool(final ProcessContext context) {
        final int maxMessageSize = context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue();
        final long maxAckWaitMillis = context.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        final String attributeNameRegex = context.getProperty(ATTRIBUTE_NAME_REGEX).getValue();
        final Pattern attributeNamePattern = attributeNameRegex == null ? null : Pattern.compile(attributeNameRegex);
        final boolean useTransactions = context.getProperty(USE_TRANSACTIONS).asBoolean();
        final String transactionalIdPrefix = context.getProperty(TRANSACTIONAL_ID_PREFIX).evaluateAttributeExpressions().getValue();
        Supplier<String> transactionalIdSupplier = new TransactionIdSupplier(transactionalIdPrefix);

        final String charsetName = context.getProperty(MESSAGE_HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);

        final KafkaPropertyProvider propertyProvider = new StandardKafkaPropertyProvider(ProducerConfig.class);
        final Map<String, Object> kafkaProperties = propertyProvider.getProperties(context);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("max.request.size", String.valueOf(maxMessageSize));

        return new PublisherPool(kafkaProperties, getLogger(), maxMessageSize, maxAckWaitMillis,
                useTransactions, transactionalIdSupplier, attributeNamePattern, charset, null, null);
    }

    @OnStopped
    public void closePool() {
        if (publisherPool != null) {
            publisherPool.close();
        }

        publisherPool = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final boolean useDemarcator = context.getProperty(MESSAGE_DEMARCATOR).isSet();

        final List<FlowFile> flowFiles = PublishKafkaUtil.pollFlowFiles(session);
        if (flowFiles.isEmpty()) {
            return;
        }

        final PublisherPool pool = getPublisherPool(context);
        if (pool == null) {
            context.yield();
            return;
        }

        final String securityProtocol = context.getProperty(SECURITY_PROTOCOL).getValue();
        final String bootstrapServers = context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        final boolean useTransactions = context.getProperty(USE_TRANSACTIONS).asBoolean();
        final PublishFailureStrategy failureStrategy = getFailureStrategy(context);

        final long startTime = System.nanoTime();
        try (final PublisherLease lease = obtainPublisher(context, pool)) {
            try {
                if (useTransactions) {
                    lease.beginTransaction();
                }

                // Send each FlowFile to Kafka asynchronously.
                for (final FlowFile flowFile : flowFiles) {
                    if (!isScheduled()) {
                        // If stopped, re-queue FlowFile instead of sending it
                        if (useTransactions) {
                            session.rollback();
                            lease.rollback();
                            return;
                        }

                        session.transfer(flowFile);
                        continue;
                    }

                    final byte[] messageKey = getMessageKey(flowFile, context);
                    final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
                    final byte[] demarcatorBytes;
                    if (useDemarcator) {
                        demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8);
                    } else {
                        demarcatorBytes = null;
                    }

                    final Integer partition = getPartition(context, flowFile);
                    session.read(flowFile, rawIn -> {
                        try (final InputStream in = new BufferedInputStream(rawIn)) {
                            lease.publish(flowFile, in, messageKey, demarcatorBytes, topic, partition);
                        }
                    });

                    // If consumer offsets haven't been committed, add them to the transaction.
                    if (useTransactions && "false".equals(flowFile.getAttribute(KAFKA_CONSUMER_OFFSETS_COMMITTED))) {
                        PublishKafkaUtil.addConsumerOffsets(lease, flowFile, getLogger());
                    }
                }

                // Complete the send
                final PublishResult publishResult = lease.complete();

                if (publishResult.isFailure()) {
                    getLogger().info("Failed to send FlowFile to kafka; transferring to specified failure strategy");
                    failureStrategy.routeFlowFiles(session, flowFiles);
                    return;
                }

                // Transfer any successful FlowFiles.
                final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                for (FlowFile success : flowFiles) {
                    final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(success).getValue();

                    final int msgCount = publishResult.getSuccessfulMessageCount(success);
                    success = session.putAttribute(success, MSG_COUNT, String.valueOf(msgCount));
                    session.adjustCounter("Messages Sent", msgCount, true);

                    final String transitUri = StandardTransitUriProvider.getTransitUri(securityProtocol, bootstrapServers, topic);
                    session.getProvenanceReporter().send(success, transitUri, "Sent " + msgCount + " messages", transmissionMillis);
                    session.transfer(success, REL_SUCCESS);
                }
            } catch (final ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                lease.poison();
                getLogger().error("Failed to send messages to Kafka; will yield Processor and transfer FlowFiles to specified failure strategy");
                failureStrategy.routeFlowFiles(session, flowFiles);
                context.yield();
            }
        }
    }

    private PublisherLease obtainPublisher(final ProcessContext context, final PublisherPool pool) {
        try {
            return pool.obtainPublisher();
        } catch (final KafkaException e) {
            getLogger().error("Failed to obtain Kafka Producer", e);
            context.yield();
            throw e;
        }
    }

    private PublishFailureStrategy getFailureStrategy(final ProcessContext context) {
        return switch (context.getProperty(FAILURE_STRATEGY).asAllowableValue(FailureStrategy.class)) {
            case ROUTE_TO_FAILURE -> (session, flowFiles) -> session.transfer(flowFiles, REL_FAILURE);
            case ROLLBACK -> (session, flowFiles) -> session.rollback();
        };
    }

    private byte[] getMessageKey(final FlowFile flowFile, final ProcessContext context) {

        final String uninterpretedKey;
        if (context.getProperty(KEY).isSet()) {
            uninterpretedKey = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            uninterpretedKey = flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_KEY);
        }

        if (uninterpretedKey == null) {
            return null;
        }

        final String keyEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).getValue();
        if (UTF8_ENCODING.getValue().equals(keyEncoding)) {
            return uninterpretedKey.getBytes(StandardCharsets.UTF_8);
        }

        try {
            return Hex.decodeHex(uninterpretedKey);
        } catch (final DecoderException e) {
            throw new RuntimeException("Hexadecimal decoding failed", e);
        }
    }

    private Integer getPartition(final ProcessContext context, final FlowFile flowFile) {
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();
        if (EXPRESSION_LANGUAGE_PARTITIONING.getValue().equals(partitionClass)) {
            final String partition = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();
            return Objects.hashCode(partition);
        }

        return null;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(attributes).getValue();
        try (final PublisherPool pool = createPublisherPool(context)) {
            return pool.verifyConfiguration(topic);
        }
    }
}
