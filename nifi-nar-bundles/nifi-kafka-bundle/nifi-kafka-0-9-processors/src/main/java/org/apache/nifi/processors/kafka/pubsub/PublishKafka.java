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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"Apache", "Kafka", "Put", "Send", "Message", "PubSub", "0.9.x"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka using the Kafka 0.9.x Producer. "
    + "The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line. "
    + " Please note there are cases where the publisher can get into an indefinite stuck state.  We are closely monitoring"
    + " how this evolves in the Kafka community and will take advantage of those fixes as soon as we can.  In the mean time"
    + " it is possible to enter states where the only resolution will be to restart the JVM NiFi runs on. The complementary NiFi processor for fetching messages is ConsumeKafka.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
    description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ")
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Kafka for this FlowFile. This attribute is added only to "
    + "FlowFiles that are routed to success. If the <Message Demarcator> Property is not set, this will always be 1, but if the Property is set, it may "
    + "be greater than 1.")
public class PublishKafka extends AbstractProcessor {
    protected static final String MSG_COUNT = "msg.count";

    static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("all", "Guarantee Replicated Delivery",
        "FlowFile will be routed to failure unless the message is replicated to the appropriate "
            + "number of Kafka Nodes according to the Topic configuration");
    static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery",
        "FlowFile will be routed to success if the message is received by a single Kafka node, "
            + "whether or not it is replicated. This is faster than <Guarantee Replicated Delivery> "
            + "but can result in data loss if a Kafka node crashes");
    static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort",
        "FlowFile will be routed to success after successfully writing the content to a Kafka node, "
            + "without waiting for a response. This provides the best performance but may result in data loss.");

    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue(Partitioners.RoundRobinPartitioner.class.getName(),
        Partitioners.RoundRobinPartitioner.class.getSimpleName(),
        "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
            + "the next Partition to Partition 2, and so on, wrapping as necessary.");
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("org.apache.kafka.clients.producer.internals.DefaultPartitioner",
        "DefaultPartitioner", "Messages will be assigned to random partitions.");

    static final AllowableValue UTF8_ENCODING = new AllowableValue("utf-8", "UTF-8 Encoded", "The key is interpreted as a UTF-8 Encoded string.");
    static final AllowableValue HEX_ENCODING = new AllowableValue("hex", "Hex Encoded",
        "The key is interpreted as arbitrary binary data that is encoded using hexadecimal characters with uppercase letters.");

    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
        .name("topic")
        .displayName("Topic Name")
        .description("The name of the Kafka Topic to publish to.")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
        .name(ProducerConfig.ACKS_CONFIG)
        .displayName("Delivery Guarantee")
        .description("Specifies the requirement for guaranteeing that a message is sent to Kafka. Corresponds to Kafka's 'acks' property.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
        .defaultValue(DELIVERY_BEST_EFFORT.getValue())
        .build();

    static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
        .name(ProducerConfig.MAX_BLOCK_MS_CONFIG)
        .displayName("Max Metadata Wait Time")
        .description("The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the "
            + "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
        .name("key-attribute-encoding")
        .displayName("Key Attribute Encoding")
        .description("FlowFiles that are emitted have an attribute named '" + KafkaProcessorUtils.KAFKA_KEY + "'. This property dictates how the value of the attribute should be encoded.")
        .required(true)
        .defaultValue(UTF8_ENCODING.getValue())
        .allowableValues(UTF8_ENCODING, HEX_ENCODING)
        .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
        .name("message-demarcator")
        .displayName("Message Demarcator")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
            + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
            + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka message. "
            + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
        .build();

    static final PropertyDescriptor PARTITION_CLASS = new PropertyDescriptor.Builder()
        .name(ProducerConfig.PARTITIONER_CLASS_CONFIG)
        .displayName("Partitioner class")
        .description("Specifies which class to use to compute a partition id for a message. Corresponds to Kafka's 'partitioner.class' property.")
        .allowableValues(ROUND_ROBIN_PARTITIONING, RANDOM_PARTITIONING)
        .defaultValue(RANDOM_PARTITIONING.getValue())
        .required(false)
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

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles for which all content was sent to Kafka.")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
        .build();

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    private volatile PublisherPool publisherPool = null;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.addAll(KafkaProcessorUtils.getCommonPropertyDescriptors());
        properties.add(TOPIC);
        properties.add(DELIVERY_GUARANTEE);
        properties.add(KEY);
        properties.add(KEY_ATTRIBUTE_ENCODING);
        properties.add(MESSAGE_DEMARCATOR);
        properties.add(MAX_REQUEST_SIZE);
        properties.add(ACK_WAIT_TIME);
        properties.add(METADATA_WAIT_TIME);
        properties.add(PARTITION_CLASS);
        properties.add(COMPRESSION_CODEC);

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

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
            .addValidator(new KafkaProcessorUtils.KafkaConfigValidator(ProducerConfig.class))
            .dynamic(true)
            .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return KafkaProcessorUtils.validateCommonProperties(validationContext);
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
        final long maxAckWaitMillis = context.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS).longValue();

        final Map<String, Object> kafkaProperties = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ProducerConfig.class, kafkaProperties);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("max.request.size", String.valueOf(maxMessageSize));

        return new PublisherPool(kafkaProperties, getLogger(), maxMessageSize, maxAckWaitMillis);
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

        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(250, DataUnit.KB, 500));
        if (flowFiles.isEmpty()) {
            return;
        }

        final PublisherPool pool = getPublisherPool(context);
        if (pool == null) {
            context.yield();
            return;
        }

        final String securityProtocol = context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL).getValue();
        final String bootstrapServers = context.getProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();

        final long startTime = System.nanoTime();
        try (final PublisherLease lease = pool.obtainPublisher()) {
            // Send each FlowFile to Kafka asynchronously.
            for (final FlowFile flowFile : flowFiles) {
                if (!isScheduled()) {
                    // If stopped, re-queue FlowFile instead of sending it
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

                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream rawIn) throws IOException {
                        try (final InputStream in = new BufferedInputStream(rawIn)) {
                            lease.publish(flowFile, in, messageKey, demarcatorBytes, topic);
                        }
                    }
                });
            }

            // Complete the send
            final PublishResult publishResult = lease.complete();

            // Transfer any successful FlowFiles.
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            for (FlowFile success : publishResult.getSuccessfulFlowFiles()) {
                final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(success).getValue();

                final int msgCount = publishResult.getSuccessfulMessageCount(success);
                success = session.putAttribute(success, MSG_COUNT, String.valueOf(msgCount));
                session.adjustCounter("Messages Sent", msgCount, true);

                final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers, topic);
                session.getProvenanceReporter().send(success, transitUri, "Sent " + msgCount + " messages", transmissionMillis);
                session.transfer(success, REL_SUCCESS);
            }

            // Transfer any failures.
            for (final FlowFile failure : publishResult.getFailedFlowFiles()) {
                final int successCount = publishResult.getSuccessfulMessageCount(failure);
                if (successCount > 0) {
                    getLogger().error("Failed to send some messages for {} to Kafka, but {} messages were acknowledged by Kafka. Routing to failure due to {}",
                        new Object[] {failure, successCount, publishResult.getReasonForFailure(failure)});
                } else {
                    getLogger().error("Failed to send all message for {} to Kafka; routing to failure due to {}",
                        new Object[] {failure, publishResult.getReasonForFailure(failure)});
                }

                session.transfer(failure, REL_FAILURE);
            }
        }
    }


    private byte[] getMessageKey(final FlowFile flowFile, final ProcessContext context) {

        final String uninterpretedKey;
        if (context.getProperty(KEY).isSet()) {
            uninterpretedKey = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            uninterpretedKey = flowFile.getAttribute(KafkaProcessorUtils.KAFKA_KEY);
        }

        if (uninterpretedKey == null) {
            return null;
        }

        final String keyEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).getValue();
        if (UTF8_ENCODING.getValue().equals(keyEncoding)) {
            return uninterpretedKey.getBytes(StandardCharsets.UTF_8);
        }

        return DatatypeConverter.parseHexBinary(uninterpretedKey);
    }
}
