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
package org.apache.nifi.processors.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.kafka.KafkaPublisher.KafkaPublisherResult;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "Apache", "Kafka", "Put", "Send", "Message", "PubSub", "0.8.x"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka, specifically for 0.8.x versions. The messages to send may be individual FlowFiles or may be delimited, using a "
        + "user-specified delimiter, such as a new-line. The complementary NiFi processor for fetching messages is GetKafka.")
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
                 description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set as part of the static properties, its value wil be"
        + " overriden with warning message describing the override."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration.")
public class PutKafka extends AbstractKafkaProcessor<KafkaPublisher> {

    public static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("all", "Guarantee Replicated Delivery",
            "FlowFile will be routed to"
                    + " failure unless the message is replicated to the appropriate number of Kafka Nodes according to the Topic configuration");
    public static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery",
            "FlowFile will be routed"
                    + " to success if the message is received by a single Kafka node, whether or not it is replicated. This is faster than"
                    + " <Guarantee Replicated Delivery> but can result in data loss if a Kafka node crashes");
    public static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort",
            "FlowFile will be routed to success after"
                    + " successfully writing the content to a Kafka node, without waiting for a response. This provides the best performance but may result"
                    + " in data loss.");

    /**
     * AllowableValue for sending messages to Kafka without compression
     */
    public static final AllowableValue COMPRESSION_CODEC_NONE = new AllowableValue("none", "None",
            "Compression will not be used for any topic.");

    /**
     * AllowableValue for sending messages to Kafka with GZIP compression
     */
    public static final AllowableValue COMPRESSION_CODEC_GZIP = new AllowableValue("gzip", "GZIP",
            "Compress messages using GZIP");

    /**
     * AllowableValue for sending messages to Kafka with Snappy compression
     */
    public static final AllowableValue COMPRESSION_CODEC_SNAPPY = new AllowableValue("snappy", "Snappy",
            "Compress messages using Snappy");

    /**
     * @deprecated Kafka 0.8.x producer doesn't use 'partitioner.class' property.
     */
    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue("Round Robin", "Round Robin",
            "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
                    + "the next Partition to Partition 2, and so on, wrapping as necessary.");
    /**
     * @deprecated Kafka 0.8.x producer doesn't use 'partitioner.class' property.
     */
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("Random Robin", "Random",
            "Messages will be assigned to random partitions.");
    /**
     * @deprecated Kafka 0.8.x producer doesn't use 'partitioner.class' property. To specify partition, simply configure the 'partition' property.
     */
    static final AllowableValue USER_DEFINED_PARTITIONING = new AllowableValue("User-Defined", "User-Defined",
            "The <Partition> property will be used to determine the partition. All messages within the same FlowFile will be "
                    + "assigned to the same partition.");

    public static final PropertyDescriptor SEED_BROKERS = new PropertyDescriptor.Builder()
            .name("Known Brokers")
            .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The Kafka Topic of interest")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    /**
     * @deprecated Kafka 0.8.x producer doesn't use 'partitioner.class' property.
     * This property is still valid as a dynamic property, so that existing processor configuration can stay valid.
     */
    static final PropertyDescriptor PARTITION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Partition Strategy")
            .description("Deprecated. Used to specify how messages should be partitioned when sent to Kafka, but it's no longer used.")
            .allowableValues(ROUND_ROBIN_PARTITIONING, RANDOM_PARTITIONING, USER_DEFINED_PARTITIONING)
            .dynamic(true)
            .build();
    public static final PropertyDescriptor PARTITION = new PropertyDescriptor.Builder()
            .name("Partition")
            .description("Specifies which Kafka Partition to add the message to. If using a message delimiter, all messages "
                            + "in the same FlowFile will be sent to the same partition. If a partition is specified but is not valid, "
                            + "then the FlowFile will be routed to failure relationship.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();
    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("Kafka Key")
            .description("The Key to use for the Message")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name("Delivery Guarantee")
            .description("Specifies the requirement for guaranteeing that a message is sent to Kafka").required(true)
            .expressionLanguageSupported(false)
            .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
            .defaultValue(DELIVERY_BEST_EFFORT.getValue())
            .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
            .name("Message Delimiter")
            .description("Specifies the delimiter (interpreted in its UTF-8 byte representation) to use for splitting apart multiple messages within a single FlowFile. "
                            + "If not specified, the entire content of the FlowFile will be used as a single message. If specified, "
                            + "the contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka "
                            + "message. Note that if messages are delimited and some messages for a given FlowFile are transferred "
                            + "successfully while others are not, the messages will be split into individual FlowFiles, such that those "
                            + "messages that were successfully sent are routed to the 'success' relationship while other messages are "
                            + "sent to the 'failure' relationship.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Buffer Size")
            .description("The maximum amount of data to buffer in memory before sending to Kafka")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("5 MB")
            .build();
    static final PropertyDescriptor MAX_RECORD_SIZE = new PropertyDescriptor.Builder()
            .name("Max Record Size")
            .description("The maximum size that any individual record can be.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .required(true)
            .defaultValue("1 MB")
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("The amount of time to wait for a response from Kafka before determining that there is a communications error")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("30 secs").build();
    public static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
            .name("Client Name")
            .description("Client Name to use when communicating with Kafka")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor BATCH_NUM_MESSAGES = new PropertyDescriptor.Builder()
            .name("Async Batch Size")
            .displayName("Batch Size")
            .description("This configuration controls the default batch size in bytes.The producer will attempt to batch records together into "
                    + "fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client "
                    + "and the server.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("16384") // Kafka default
            .build();
    public static final PropertyDescriptor QUEUE_BUFFERING_MAX = new PropertyDescriptor.Builder()
            .name("Queue Buffering Max Time")
            .description("Maximum time to buffer data before sending to Kafka. For example a setting of 100 ms"
                    + " will try to batch together 100 milliseconds' worth of messages to send at once. This will improve"
                    + " throughput but adds message delivery latency due to the buffering.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("Compression Codec")
            .description("This parameter allows you to specify the compression codec for all"
                    + " data generated by this producer.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(COMPRESSION_CODEC_NONE, COMPRESSION_CODEC_GZIP, COMPRESSION_CODEC_SNAPPY)
            .defaultValue(COMPRESSION_CODEC_NONE.getValue())
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully sent to Kafka will be routed to this Relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
            .build();

    protected static final String FAILED_PROC_ID_ATTR = "failed.proc.id";

    protected static final String FAILED_LAST_ACK_IDX = "failed.last.idx";

    protected static final String FAILED_TOPIC_ATTR = "failed.topic";

    protected static final String FAILED_KEY_ATTR = "failed.key";

    protected static final String FAILED_DELIMITER_ATTR = "failed.delimiter";

    private static final List<PropertyDescriptor> propertyDescriptors;

    private static final Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SEED_BROKERS);
        _propertyDescriptors.add(TOPIC);
        _propertyDescriptors.add(PARTITION);
        _propertyDescriptors.add(KEY);
        _propertyDescriptors.add(DELIVERY_GUARANTEE);
        _propertyDescriptors.add(MESSAGE_DELIMITER);
        _propertyDescriptors.add(MAX_BUFFER_SIZE);
        _propertyDescriptors.add(MAX_RECORD_SIZE);
        _propertyDescriptors.add(TIMEOUT);
        _propertyDescriptors.add(BATCH_NUM_MESSAGES);
        _propertyDescriptors.add(QUEUE_BUFFERING_MAX);
        _propertyDescriptors.add(COMPRESSION_CODEC);
        _propertyDescriptors.add(CLIENT_NAME);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }


    /**
     * Will rendezvous with Kafka if {@link ProcessSession} contains {@link FlowFile}
     * producing a result {@link FlowFile}.
     * <br>
     * The result {@link FlowFile} that is successful is then transferred to {@link #REL_SUCCESS}
     * <br>
     * The result {@link FlowFile} that is failed is then transferred to {@link #REL_FAILURE}
     *
     */
    @Override
    protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession session) throws ProcessException {
        boolean processed = false;
        FlowFile flowFile = session.get();
        if (flowFile != null) {
            flowFile = this.doRendezvousWithKafka(flowFile, context, session);
            if (!this.isFailedFlowFile(flowFile)) {
                session.getProvenanceReporter().send(flowFile,
                        context.getProperty(SEED_BROKERS).evaluateAttributeExpressions(flowFile).getValue() + "/"
                        + context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue());
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }
            processed = true;
        }
        return processed;
    }

    /**
     * Will rendezvous with {@link KafkaPublisher} after building
     * {@link PublishingContext} and will produce the resulting {@link FlowFile}.
     * The resulting FlowFile contains all required information to determine
     * if message publishing originated from the provided FlowFile has actually
     * succeeded fully, partially or failed completely (see
     * {@link #isFailedFlowFile(FlowFile)}.
     */
    private FlowFile doRendezvousWithKafka(final FlowFile flowFile, final ProcessContext context, final ProcessSession session) {
        final AtomicReference<KafkaPublisherResult> publishResultRef = new AtomicReference<>();
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream contentStream) throws IOException {
                PublishingContext publishingContext = PutKafka.this.buildPublishingContext(flowFile, context, contentStream);
                KafkaPublisherResult result = null;
                try {
                    result = PutKafka.this.kafkaResource.publish(publishingContext);
                } catch (final IllegalArgumentException e) {
                    getLogger().error("Failed to publish {}, due to {}", new Object[]{flowFile, e}, e);
                    result = new KafkaPublisherResult(0, -1);

                }
                publishResultRef.set(result);
            }
        });

        FlowFile resultFile = publishResultRef.get().isAllAcked()
                ? this.cleanUpFlowFileIfNecessary(flowFile, session)
                : session.putAllAttributes(flowFile, this.buildFailedFlowFileAttributes(publishResultRef.get().getLastMessageAcked(), flowFile, context));

        return resultFile;
    }

    /**
     * Builds {@link PublishingContext} for message(s) to be sent to Kafka.
     * {@link PublishingContext} contains all contextual information required by
     * {@link KafkaPublisher} to publish to Kafka. Such information contains
     * things like topic name, content stream, delimiter, key and last ACKed
     * message for cases where provided FlowFile is being retried (failed in the
     * past). <br>
     * For the clean FlowFile (file that has been sent for the first time),
     * PublishingContext will be built form {@link ProcessContext} associated
     * with this invocation. <br>
     * For the failed FlowFile, {@link PublishingContext} will be built from
     * attributes of that FlowFile which by then will already contain required
     * information (e.g., topic, key, delimiter etc.). This is required to
     * ensure the affinity of the retry in the even where processor
     * configuration has changed. However keep in mind that failed FlowFile is
     * only considered a failed FlowFile if it is being re-processed by the same
     * processor (determined via {@link #FAILED_PROC_ID_ATTR}, see
     * {@link #isFailedFlowFile(FlowFile)}). If failed FlowFile is being sent to
     * another PublishKafka processor it is treated as a fresh FlowFile
     * regardless if it has #FAILED* attributes set.
     */
    private PublishingContext buildPublishingContext(FlowFile flowFile, ProcessContext context,
            InputStream contentStream) {
        String topicName;
        byte[] keyBytes;
        byte[] delimiterBytes = null;
        int lastAckedMessageIndex = -1;
        if (this.isFailedFlowFile(flowFile)) {
            lastAckedMessageIndex = Integer.valueOf(flowFile.getAttribute(FAILED_LAST_ACK_IDX));
            topicName = flowFile.getAttribute(FAILED_TOPIC_ATTR);
            keyBytes = flowFile.getAttribute(FAILED_KEY_ATTR) != null
                    ? flowFile.getAttribute(FAILED_KEY_ATTR).getBytes(StandardCharsets.UTF_8) : null;
            delimiterBytes = flowFile.getAttribute(FAILED_DELIMITER_ATTR) != null
                    ? flowFile.getAttribute(FAILED_DELIMITER_ATTR).getBytes(StandardCharsets.UTF_8) : null;

        } else {
            topicName = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
            String _key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
            keyBytes = _key == null ? null : _key.getBytes(StandardCharsets.UTF_8);
            delimiterBytes = context.getProperty(MESSAGE_DELIMITER).isSet() ? context.getProperty(MESSAGE_DELIMITER)
                    .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;
        }

        PublishingContext publishingContext = new PublishingContext(contentStream, topicName, lastAckedMessageIndex);
        publishingContext.setKeyBytes(keyBytes);
        publishingContext.setDelimiterBytes(delimiterBytes);
        publishingContext.setPartitionId(this.determinePartition(context, flowFile));
        return publishingContext;
    }

    /**
     * Returns 'true' if provided FlowFile is a failed FlowFile. A failed
     * FlowFile contains {@link #FAILED_PROC_ID_ATTR}.
     */
    private boolean isFailedFlowFile(FlowFile flowFile) {
        return this.getIdentifier().equals(flowFile.getAttribute(FAILED_PROC_ID_ATTR));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected KafkaPublisher buildKafkaResource(ProcessContext context, ProcessSession session)
            throws ProcessException {
        KafkaPublisher kafkaPublisher = new KafkaPublisher(this.buildKafkaConfigProperties(context), this.getLogger());
        return kafkaPublisher;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (PARTITION_STRATEGY.getName().equals(propertyDescriptorName)) {
            return PARTITION_STRATEGY;
        }

        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     *
     */
    private FlowFile cleanUpFlowFileIfNecessary(FlowFile flowFile, ProcessSession session) {
        if (this.isFailedFlowFile(flowFile)) {
            Set<String> keysToRemove = new HashSet<>();
            keysToRemove.add(FAILED_DELIMITER_ATTR);
            keysToRemove.add(FAILED_KEY_ATTR);
            keysToRemove.add(FAILED_TOPIC_ATTR);
            keysToRemove.add(FAILED_PROC_ID_ATTR);
            keysToRemove.add(FAILED_LAST_ACK_IDX);
            flowFile = session.removeAllAttributes(flowFile, keysToRemove);
        }
        return flowFile;
    }

    /**
     *
     */
    private Integer determinePartition(ProcessContext context, FlowFile flowFile) {
        String pv = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();
        if (pv != null){
            return Integer.parseInt(context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue());
        }
        return null;
    }

    /**
     * Builds a {@link Map} of FAILED_* attributes
     *
     * @see #FAILED_PROC_ID_ATTR
     * @see #FAILED_LAST_ACK_IDX
     * @see #FAILED_TOPIC_ATTR
     * @see #FAILED_KEY_ATTR
     * @see #FAILED_DELIMITER_ATTR
     */
    private Map<String, String> buildFailedFlowFileAttributes(int lastAckedMessageIndex, FlowFile sourceFlowFile,
            ProcessContext context) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(FAILED_PROC_ID_ATTR, this.getIdentifier());
        attributes.put(FAILED_LAST_ACK_IDX, String.valueOf(lastAckedMessageIndex));
        attributes.put(FAILED_TOPIC_ATTR, context.getProperty(TOPIC).evaluateAttributeExpressions(sourceFlowFile).getValue());
        attributes.put(FAILED_KEY_ATTR, context.getProperty(KEY).evaluateAttributeExpressions(sourceFlowFile).getValue());
        attributes.put(FAILED_DELIMITER_ATTR, context.getProperty(MESSAGE_DELIMITER).isSet()
                ? context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions(sourceFlowFile).getValue()
                : null);
        return attributes;
    }

    /**
     *
     */
    private Properties buildKafkaConfigProperties(final ProcessContext context) {
        Properties properties = new Properties();
        String timeout = String.valueOf(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
        properties.setProperty("bootstrap.servers", context.getProperty(SEED_BROKERS).evaluateAttributeExpressions().getValue());
        properties.setProperty("acks", context.getProperty(DELIVERY_GUARANTEE).getValue());
        properties.setProperty("buffer.memory", String.valueOf(context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).longValue()));
        properties.setProperty("compression.type", context.getProperty(COMPRESSION_CODEC).getValue());
        properties.setProperty("batch.size", context.getProperty(BATCH_NUM_MESSAGES).getValue());

        properties.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());
        Long queueBufferingMillis = context.getProperty(QUEUE_BUFFERING_MAX).asTimePeriod(TimeUnit.MILLISECONDS);
        if (queueBufferingMillis != null) {
            properties.setProperty("linger.ms", String.valueOf(queueBufferingMillis));
        }
        properties.setProperty("max.request.size", String.valueOf(context.getProperty(MAX_RECORD_SIZE).asDataSize(DataUnit.B).longValue()));
        properties.setProperty("timeout.ms", timeout);
        properties.setProperty("metadata.fetch.timeout.ms", timeout);

        // Set Dynamic Properties
        for (final Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                if (PARTITION_STRATEGY.equals(descriptor)) {
                    continue;
                }
                if (properties.containsKey(descriptor.getName())) {
                    this.getLogger().warn("Overriding existing property '" + descriptor.getName() + "' which had value of '"
                                    + properties.getProperty(descriptor.getName()) + "' with dynamically set value '"
                                    + entry.getValue() + "'.");
                }
                properties.setProperty(descriptor.getName(), entry.getValue());
            }
        }
        return properties;
    }
}
