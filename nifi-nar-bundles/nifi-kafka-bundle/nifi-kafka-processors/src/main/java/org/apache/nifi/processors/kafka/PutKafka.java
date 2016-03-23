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
import java.util.BitSet;
import java.util.Collection;
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
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.processor.util.StandardValidators;

import kafka.producer.DefaultPartitioner;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "Apache", "Kafka", "Put", "Send", "Message", "PubSub" })
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka. The messages to send may be individual FlowFiles or may be delimited, using a "
        + "user-specified delimiter, such as a new-line.")
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
                 description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set as part of the static properties, its value wil be"
        + " overriden with warning message describing the override."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration.")
public class PutKafka extends AbstractProcessor {

    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";

    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";

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

    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue("Round Robin", "Round Robin",
            "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
                    + "the next Partition to Partition 2, and so on, wrapping as necessary.");
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("Random Robin", "Random",
            "Messages will be assigned to random partitions.");
    static final AllowableValue USER_DEFINED_PARTITIONING = new AllowableValue("User-Defined", "User-Defined",
            "The <Partition> property will be used to determine the partition. All messages within the same FlowFile will be "
                    + "assigned to the same partition.");

    public static final PropertyDescriptor SEED_BROKERS = new PropertyDescriptor.Builder()
            .name("Known Brokers")
            .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(BROKER_REGEX)))
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The Kafka Topic of interest")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor PARTITION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Partition Strategy")
            .description("Specifies how messages should be partitioned when sent to Kafka")
            .allowableValues(ROUND_ROBIN_PARTITIONING, RANDOM_PARTITIONING, USER_DEFINED_PARTITIONING)
            .defaultValue(ROUND_ROBIN_PARTITIONING.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor PARTITION = new PropertyDescriptor.Builder()
            .name("Partition")
            .description("Specifies which Kafka Partition to add the message to. If using a message delimiter, all messages "
                            + "in the same FlowFile will be sent to the same partition. If a partition is specified but is not valid, "
                            + "then all messages within the same FlowFile will use the same partition but it remains undefined which partition is used.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
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
            .description("Specifies the delimiter to use for splitting apart multiple messages within a single FlowFile. "
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
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR).required(true)
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
            .name("Async Batch Size").displayName("Batch Size")
            .description("The number of messages to send in one batch. The producer will wait until either this number of messages are ready "
                            + "to send or \"Queue Buffering Max Time\" is reached. NOTE: This property will be ignored unless the 'Message Delimiter' "
                            + "property is specified.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("200")
            .build();
    public static final PropertyDescriptor QUEUE_BUFFERING_MAX = new PropertyDescriptor.Builder()
            .name("Queue Buffering Max Time")
            .description("Maximum time to buffer data before sending to Kafka. For example a setting of 100 ms"
                    + " will try to batch together 100 milliseconds' worth of messages to send at once. This will improve"
                    + " throughput but adds message delivery latency due to the buffering.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 secs")
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

    protected static final String ATTR_PROC_ID = "PROC_ID";

    protected static final String ATTR_FAILED_SEGMENTS = "FS";

    protected static final String ATTR_TOPIC = "TOPIC";

    protected static final String ATTR_KEY = "KEY";

    protected static final String ATTR_DELIMITER = "DELIMITER";

    private volatile KafkaPublisher kafkaPublisher;

    private static final List<PropertyDescriptor> propertyDescriptors;

    private static final Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SEED_BROKERS);
        _propertyDescriptors.add(TOPIC);
        _propertyDescriptors.add(PARTITION_STRATEGY);
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
     *
     */
    @OnScheduled
    public void createKafkaPublisher(ProcessContext context) {
        this.kafkaPublisher = new KafkaPublisher(this.buildKafkaConfigProperties(context));
        this.kafkaPublisher.setProcessLog(this.getLogger());
    }

    /**
     *
     */
    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile != null) {
            final SplittableMessageContext messageContext = this.buildMessageContext(flowFile, context, session);
            final Integer partitionKey = this.determinePartition(messageContext, context, flowFile);
            final AtomicReference<BitSet> failedSegmentsRef = new AtomicReference<BitSet>();
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream contentStream) throws IOException {
                    failedSegmentsRef.set(kafkaPublisher.publish(messageContext, contentStream, partitionKey));
                }
            });

            if (failedSegmentsRef.get().isEmpty()) {
                session.getProvenanceReporter().send(flowFile, context.getProperty(SEED_BROKERS).getValue() + "/" + messageContext.getTopicName());
                flowFile = this.cleanUpFlowFileIfNecessary(flowFile, session);
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                flowFile = session.putAllAttributes(flowFile, this.buildFailedFlowFileAttributes(failedSegmentsRef.get(), messageContext));
                session.transfer(flowFile, REL_FAILURE);
            }

        } else {
            context.yield();
        }
    }

    @OnStopped
    public void cleanup() {
        try {
            this.kafkaPublisher.close();
        } catch (Exception e) {
            getLogger().warn("Failed while closing KafkaPublisher", e);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String partitionStrategy = validationContext.getProperty(PARTITION_STRATEGY).getValue();
        if (partitionStrategy.equalsIgnoreCase(USER_DEFINED_PARTITIONING.getValue())
                && !validationContext.getProperty(PARTITION).isSet()) {
            results.add(new ValidationResult.Builder().subject("Partition").valid(false)
                    .explanation("The <Partition> property must be set when configured to use the User-Defined Partitioning Strategy")
                    .build());
        }
        return results;
    }

    /**
     *
     */
    private FlowFile cleanUpFlowFileIfNecessary(FlowFile flowFile, ProcessSession session) {
        if (flowFile.getAttribute(ATTR_FAILED_SEGMENTS) != null) {
            flowFile = session.removeAttribute(flowFile, ATTR_FAILED_SEGMENTS);
            flowFile = session.removeAttribute(flowFile, ATTR_KEY);
            flowFile = session.removeAttribute(flowFile, ATTR_TOPIC);
            flowFile = session.removeAttribute(flowFile, ATTR_DELIMITER);
            flowFile = session.removeAttribute(flowFile, ATTR_PROC_ID);
        }
        return flowFile;
    }

    /**
     *
     */
    private Integer determinePartition(SplittableMessageContext messageContext, ProcessContext context, FlowFile flowFile) {
        String partitionStrategy = context.getProperty(PARTITION_STRATEGY).getValue();
        Integer partitionValue = null;
        if (partitionStrategy.equalsIgnoreCase(USER_DEFINED_PARTITIONING.getValue())) {
            String pv = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();
            if (pv != null){
                partitionValue = Integer.parseInt(context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue());
            }
        }
        return partitionValue;
    }

    /**
     *
     */
    private Map<String, String> buildFailedFlowFileAttributes(BitSet failedSegments, SplittableMessageContext messageContext) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_PROC_ID, this.getIdentifier());
        attributes.put(ATTR_FAILED_SEGMENTS, new String(failedSegments.toByteArray(), StandardCharsets.UTF_8));
        attributes.put(ATTR_TOPIC, messageContext.getTopicName());
        attributes.put(ATTR_KEY, messageContext.getKeyBytesAsString());
        attributes.put(ATTR_DELIMITER, messageContext.getDelimiterPattern());
        return attributes;
    }

    /**
     *
     */
    private SplittableMessageContext buildMessageContext(FlowFile flowFile, ProcessContext context, ProcessSession session) {
        String topicName;
        byte[] key;
        String delimiterPattern;

        String failedSegmentsString = flowFile.getAttribute(ATTR_FAILED_SEGMENTS);
        if (flowFile.getAttribute(ATTR_PROC_ID) != null && flowFile.getAttribute(ATTR_PROC_ID).equals(this.getIdentifier()) && failedSegmentsString != null) {
            topicName = flowFile.getAttribute(ATTR_TOPIC);
            key = flowFile.getAttribute(ATTR_KEY).getBytes();
            delimiterPattern = flowFile.getAttribute(ATTR_DELIMITER);
        } else {
            failedSegmentsString = null;
            topicName = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
            String _key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
            key = _key == null ? null : _key.getBytes(StandardCharsets.UTF_8);
            delimiterPattern = context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
        }
        SplittableMessageContext messageContext = new SplittableMessageContext(topicName, key, delimiterPattern);
        if (failedSegmentsString != null) {
            messageContext.setFailedSegmentsAsByteArray(failedSegmentsString.getBytes());
        }
        return messageContext;
    }

    /**
     *
     */
    private Properties buildKafkaConfigProperties(final ProcessContext context) {
        Properties properties = new Properties();
        String timeout = String.valueOf(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
        properties.setProperty("bootstrap.servers", context.getProperty(SEED_BROKERS).getValue());
        properties.setProperty("acks", context.getProperty(DELIVERY_GUARANTEE).getValue());
        properties.setProperty("buffer.memory", String.valueOf(context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).longValue()));
        properties.setProperty("compression.type", context.getProperty(COMPRESSION_CODEC).getValue());
        if (context.getProperty(MESSAGE_DELIMITER).isSet()) {
            properties.setProperty("batch.size", context.getProperty(BATCH_NUM_MESSAGES).getValue());
        } else {
            properties.setProperty("batch.size", "1");
        }

        properties.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());
        Long queueBufferingMillis = context.getProperty(QUEUE_BUFFERING_MAX).asTimePeriod(TimeUnit.MILLISECONDS);
        if (queueBufferingMillis != null) {
            properties.setProperty("linger.ms", String.valueOf(queueBufferingMillis));
        }
        properties.setProperty("max.request.size", String.valueOf(context.getProperty(MAX_RECORD_SIZE).asDataSize(DataUnit.B).longValue()));
        properties.setProperty("timeout.ms", timeout);
        properties.setProperty("metadata.fetch.timeout.ms", timeout);

        String partitionStrategy = context.getProperty(PARTITION_STRATEGY).getValue();
        String partitionerClass = null;
        if (partitionStrategy.equalsIgnoreCase(ROUND_ROBIN_PARTITIONING.getValue())) {
            partitionerClass = Partitioners.RoundRobinPartitioner.class.getName();
        } else if (partitionStrategy.equalsIgnoreCase(RANDOM_PARTITIONING.getValue())) {
            partitionerClass = DefaultPartitioner.class.getName();
        }
        properties.setProperty("partitioner.class", partitionerClass);

        // Set Dynamic Properties
        for (final Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
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
