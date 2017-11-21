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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

@Tags({"Apache", "Kafka", "Record", "csv", "json", "avro", "logs", "Put", "Send", "Message", "PubSub", "0.11.x"})
@CapabilityDescription("Sends the contents of a FlowFile as individual records to Apache Kafka using the Kafka 0.11.x Producer API. "
    + "The contents of the FlowFile are expected to be record-oriented data that can be read by the configured Record Reader. "
    + " Please note there are cases where the publisher can get into an indefinite stuck state.  We are closely monitoring"
    + " how this evolves in the Kafka community and will take advantage of those fixes as soon as we can.  In the meantime"
    + " it is possible to enter states where the only resolution will be to restart the JVM NiFi runs on. The complementary NiFi "
    + "processor for fetching messages is ConsumeKafka_0_11_Record.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
    description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
    + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
    + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ")
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Kafka for this FlowFile. This attribute is added only to "
    + "FlowFiles that are routed to success.")
@SeeAlso({PublishKafka_0_11.class, ConsumeKafka_0_11.class, ConsumeKafkaRecord_0_11.class})
public class PublishKafkaRecord_0_11 extends AbstractProcessor {
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
        .expressionLanguageSupported(true)
        .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("The Record Reader to use for incoming FlowFiles")
        .identifiesControllerService(RecordReaderFactory.class)
        .expressionLanguageSupported(false)
        .required(true)
        .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("The Record Writer to use in order to serialize the data before sending to Kafka")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .expressionLanguageSupported(false)
        .required(true)
        .build();

    static final PropertyDescriptor MESSAGE_KEY_FIELD = new PropertyDescriptor.Builder()
        .name("message-key-field")
        .displayName("Message Key Field")
        .description("The name of a field in the Input Records that should be used as the Key for the Kafka message.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(false)
        .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
        .name("acks")
        .displayName("Delivery Guarantee")
        .description("Specifies the requirement for guaranteeing that a message is sent to Kafka. Corresponds to Kafka's 'acks' property.")
        .required(true)
        .expressionLanguageSupported(false)
        .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
        .defaultValue(DELIVERY_BEST_EFFORT.getValue())
        .build();

    static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
        .name("max.block.ms")
        .displayName("Max Metadata Wait Time")
        .description("The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the "
            + "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue("5 sec")
        .build();

    static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
        .name("ack.wait.time")
        .displayName("Acknowledgment Wait Time")
        .description("After sending a message to Kafka, this indicates the amount of time that we are willing to wait for a response from Kafka. "
            + "If Kafka does not acknowledge the message within this time period, the FlowFile will be routed to 'failure'.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(false)
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

    static final PropertyDescriptor PARTITION_CLASS = new PropertyDescriptor.Builder()
        .name("partitioner.class")
        .displayName("Partitioner class")
        .description("Specifies which class to use to compute a partition id for a message. Corresponds to Kafka's 'partitioner.class' property.")
        .allowableValues(ROUND_ROBIN_PARTITIONING, RANDOM_PARTITIONING)
        .defaultValue(RANDOM_PARTITIONING.getValue())
        .required(false)
        .build();

    static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
        .name("compression.type")
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
        .expressionLanguageSupported(false)
        .required(false)
        .build();
    static final PropertyDescriptor USE_TRANSACTIONS = new PropertyDescriptor.Builder()
        .name("use-transactions")
        .displayName("Use Transactions")
        .description("Specifies whether or not NiFi should provide Transactional guarantees when communicating with Kafka. If there is a problem sending data to Kafka, "
            + "and this property is set to false, then the messages that have already been sent to Kafka will continue on and be delivered to consumers. "
            + "If this is set to true, then the Kafka transaction will be rolled back so that those messages are not available to consumers. Setting this to true "
            + "requires that the <Delivery Guarantee> property be set to \"Guarantee Replicated Delivery.\"")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
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

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    private volatile PublisherPool publisherPool = null;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KafkaProcessorUtils.BOOTSTRAP_SERVERS);
        properties.add(TOPIC);
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(USE_TRANSACTIONS);
        properties.add(DELIVERY_GUARANTEE);
        properties.add(ATTRIBUTE_NAME_REGEX);
        properties.add(MESSAGE_HEADER_ENCODING);
        properties.add(KafkaProcessorUtils.SECURITY_PROTOCOL);
        properties.add(KafkaProcessorUtils.KERBEROS_PRINCIPLE);
        properties.add(KafkaProcessorUtils.USER_PRINCIPAL);
        properties.add(KafkaProcessorUtils.USER_KEYTAB);
        properties.add(KafkaProcessorUtils.SSL_CONTEXT_SERVICE);
        properties.add(MESSAGE_KEY_FIELD);
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
        final List<ValidationResult> results = new ArrayList<>();
        results.addAll(KafkaProcessorUtils.validateCommonProperties(validationContext));

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
        final long maxAckWaitMillis = context.getProperty(ACK_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS).longValue();

        final String attributeNameRegex = context.getProperty(ATTRIBUTE_NAME_REGEX).getValue();
        final Pattern attributeNamePattern = attributeNameRegex == null ? null : Pattern.compile(attributeNameRegex);
        final boolean useTransactions = context.getProperty(USE_TRANSACTIONS).asBoolean();

        final String charsetName = context.getProperty(MESSAGE_HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);

        final Map<String, Object> kafkaProperties = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ProducerConfig.class, kafkaProperties);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("max.request.size", String.valueOf(maxMessageSize));

        return new PublisherPool(kafkaProperties, getLogger(), maxMessageSize, maxAckWaitMillis, useTransactions, attributeNamePattern, charset);
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
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 500));
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
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final boolean useTransactions = context.getProperty(USE_TRANSACTIONS).asBoolean();

        final long startTime = System.nanoTime();
        try (final PublisherLease lease = pool.obtainPublisher()) {
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

                final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
                final String messageKeyField = context.getProperty(MESSAGE_KEY_FIELD).evaluateAttributeExpressions(flowFile).getValue();

                try {
                    session.read(flowFile, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream rawIn) throws IOException {
                            try (final InputStream in = new BufferedInputStream(rawIn)) {
                                final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
                                final RecordSet recordSet = reader.createRecordSet();

                                final RecordSchema schema = writerFactory.getSchema(flowFile.getAttributes(), recordSet.getSchema());
                                lease.publish(flowFile, recordSet, writerFactory, schema, messageKeyField, topic);
                            } catch (final SchemaNotFoundException | MalformedRecordException e) {
                                throw new ProcessException(e);
                            }
                        }
                    });
                } catch (final Exception e) {
                    // The FlowFile will be obtained and the error logged below, when calling publishResult.getFailedFlowFiles()
                    lease.fail(flowFile, e);
                    continue;
                }
            }

            // Complete the send
            final PublishResult publishResult = lease.complete();

            if (publishResult.isFailure()) {
                getLogger().info("Failed to send FlowFile to kafka; transferring to failure");
                session.transfer(flowFiles, REL_FAILURE);
                return;
            }

            // Transfer any successful FlowFiles.
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            for (FlowFile success : flowFiles) {
                final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(success).getValue();

                final int msgCount = publishResult.getSuccessfulMessageCount(success);
                success = session.putAttribute(success, MSG_COUNT, String.valueOf(msgCount));
                session.adjustCounter("Messages Sent", msgCount, true);

                final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers, topic);
                session.getProvenanceReporter().send(success, transitUri, "Sent " + msgCount + " messages", transmissionMillis);
                session.transfer(success, REL_SUCCESS);
            }
        }
    }
}
