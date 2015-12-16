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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.NonThreadSafeCircularBuffer;
import org.apache.nifi.util.LongHolder;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Apache", "Kafka", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka. The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line.")
@TriggerWhenEmpty // because we have a queue of sessions that are ready to be committed
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
            description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set as part of the static properties, its value wil be"
        + " overriden with warning message describing the override."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration.")
public class PutKafka extends AbstractSessionFactoryProcessor {

    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";
    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";

    public static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("all", "Guarantee Replicated Delivery", "FlowFile will be routed to"
        + " failure unless the message is replicated to the appropriate number of Kafka Nodes according to the Topic configuration");
    public static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery", "FlowFile will be routed"
        + " to success if the message is received by a single Kafka node, whether or not it is replicated. This is faster than"
        + " <Guarantee Replicated Delivery> but can result in data loss if a Kafka node crashes");
    public static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort", "FlowFile will be routed to success after"
        + " successfully writing the content to a Kafka node, without waiting for a response. This provides the best performance but may result"
        + " in data loss.");


    /**
     * AllowableValue for sending messages to Kafka without compression
     */
    public static final AllowableValue COMPRESSION_CODEC_NONE = new AllowableValue("none", "None", "Compression will not be used for any topic.");

    /**
     * AllowableValue for sending messages to Kafka with GZIP compression
     */
    public static final AllowableValue COMPRESSION_CODEC_GZIP = new AllowableValue("gzip", "GZIP", "Compress messages using GZIP");

    /**
     * AllowableValue for sending messages to Kafka with Snappy compression
     */
    public static final AllowableValue COMPRESSION_CODEC_SNAPPY = new AllowableValue("snappy", "Snappy", "Compress messages using Snappy");

    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue("Round Robin", "Round Robin",
        "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, the next Partition to Partition 2, and so on, wrapping as necessary.");
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("Random Robin", "Random",
        "Messages will be assigned to random partitions.");
    static final AllowableValue USER_DEFINED_PARTITIONING = new AllowableValue("User-Defined", "User-Defined",
        "The <Partition> property will be used to determine the partition. All messages within the same FlowFile will be assigned to the same partition.");


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
        .description("Specifies which Kafka Partition to add the message to. If using a message delimiter, all messages in the same FlowFile will be sent to the same partition. "
            + "If a partition is specified but is not valid, then all messages within the same FlowFile will use the same partition but it remains undefined which partition is used.")
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
        .description("Specifies the requirement for guaranteeing that a message is sent to Kafka")
        .required(true)
        .expressionLanguageSupported(false)
        .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
        .defaultValue(DELIVERY_BEST_EFFORT.getValue())
        .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
        .name("Message Delimiter")
        .description("Specifies the delimiter to use for splitting apart multiple messages within a single FlowFile. "
            + "If not specified, the entire content of the FlowFile will be used as a single message. "
            + "If specified, the contents of the FlowFile will be split on this delimiter and each section "
            + "sent as a separate Kafka message. Note that if messages are delimited and some messages for a given FlowFile "
            + "are transferred successfully while others are not, the messages will be split into individual FlowFiles, such that those "
            + "messages that were successfully sent are routed to the 'success' relationship while other messages are sent to the 'failure' "
            + "relationship.")
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
        .defaultValue("30 secs")
        .build();
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
        .description("The number of messages to send in one batch. The producer will wait until either this number of messages are ready"
            + " to send or \"Queue Buffering Max Time\" is reached.")
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully sent to Kafka will be routed to this Relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
        .build();

    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");
    private final BlockingQueue<FlowFileMessageBatch> completeBatches = new LinkedBlockingQueue<>();
    private final Set<FlowFileMessageBatch> activeBatches = Collections.synchronizedSet(new HashSet<FlowFileMessageBatch>());

    private final ConcurrentMap<String, AtomicLong> partitionIndexMap = new ConcurrentHashMap<>();

    private volatile Producer<byte[], byte[]> producer;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor clientName = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CLIENT_NAME)
            .defaultValue("NiFi-" + getIdentifier())
            .build();

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SEED_BROKERS);
        props.add(TOPIC);
        props.add(PARTITION_STRATEGY);
        props.add(PARTITION);
        props.add(KEY);
        props.add(DELIVERY_GUARANTEE);
        props.add(MESSAGE_DELIMITER);
        props.add(MAX_BUFFER_SIZE);
        props.add(MAX_RECORD_SIZE);
        props.add(TIMEOUT);
        props.add(BATCH_NUM_MESSAGES);
        props.add(QUEUE_BUFFERING_MAX);
        props.add(COMPRESSION_CODEC);
        props.add(clientName);
        return props;
    }


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String partitionStrategy = validationContext.getProperty(PARTITION_STRATEGY).getValue();
        if (partitionStrategy.equalsIgnoreCase(USER_DEFINED_PARTITIONING.getValue()) && !validationContext.getProperty(PARTITION).isSet()) {
            results.add(new ValidationResult.Builder().subject("Partition").valid(false).explanation(
                "The <Partition> property must be set when configured to use the User-Defined Partitioning Strategy").build());
        }

        return results;
    }

    protected Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    @OnStopped
    public void cleanup() {
        final Producer<byte[], byte[]> producer = getProducer();
        if (producer != null) {
            producer.close();
        }

        for (final FlowFileMessageBatch batch : activeBatches) {
            batch.cancelOrComplete();
        }
    }

    @OnScheduled
    public void createProducer(final ProcessContext context) {
        producer = new KafkaProducer<byte[], byte[]>(createConfig(context), new ByteArraySerializer(), new ByteArraySerializer());
    }

    protected int getActiveMessageBatchCount() {
        return activeBatches.size();
    }

    protected int getCompleteMessageBatchCount() {
        return completeBatches.size();
    }

    protected Properties createConfig(final ProcessContext context) {
        final String brokers = context.getProperty(SEED_BROKERS).getValue();

        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("acks", context.getProperty(DELIVERY_GUARANTEE).getValue());
        properties.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());

        final String timeout = String.valueOf(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
        properties.setProperty("timeout.ms", timeout);
        properties.setProperty("metadata.fetch.timeout.ms", timeout);

        properties.setProperty("batch.size", context.getProperty(BATCH_NUM_MESSAGES).getValue());
        properties.setProperty("max.request.size", String.valueOf(context.getProperty(MAX_RECORD_SIZE).asDataSize(DataUnit.B).longValue()));

        final long maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).longValue();
        properties.setProperty("buffer.memory", String.valueOf(maxBufferSize));

        final String compressionCodec = context.getProperty(COMPRESSION_CODEC).getValue();
        properties.setProperty("compression.type", compressionCodec);

        final Long queueBufferingMillis = context.getProperty(QUEUE_BUFFERING_MAX).asTimePeriod(TimeUnit.MILLISECONDS);
        if (queueBufferingMillis != null) {
            properties.setProperty("linger.ms", String.valueOf(queueBufferingMillis));
        }

        properties.setProperty("retries", "0");
        properties.setProperty("block.on.buffer.full", "false");

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

    private Integer getPartition(final ProcessContext context, final FlowFile flowFile, final String topic) {
        final long unnormalizedIndex;

        final String partitionStrategy = context.getProperty(PARTITION_STRATEGY).getValue();
        if (partitionStrategy.equalsIgnoreCase(ROUND_ROBIN_PARTITIONING.getValue())) {
            AtomicLong partitionIndex = partitionIndexMap.get(topic);
            if (partitionIndex == null) {
                partitionIndex = new AtomicLong(0L);
                final AtomicLong existing = partitionIndexMap.putIfAbsent(topic, partitionIndex);
                if (existing != null) {
                    partitionIndex = existing;
                }
            }

            unnormalizedIndex = partitionIndex.getAndIncrement();
        } else if (partitionStrategy.equalsIgnoreCase(RANDOM_PARTITIONING.getValue())) {
            return null;
        } else {
            if (context.getProperty(PARTITION).isSet()) {
                final String partitionValue = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();

                if (NUMBER_PATTERN.matcher(partitionValue).matches()) {
                    // Subtract 1 because if the partition is "3" then we want to get index 2 into the List of partitions.
                    unnormalizedIndex = Long.parseLong(partitionValue) - 1;
                } else {
                    unnormalizedIndex = partitionValue.hashCode();
                }
            } else {
                return null;
            }
        }

        final Producer<byte[], byte[]> producer = getProducer();
        final List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
        final int partitionIdx = (int) (unnormalizedIndex % partitionInfos.size());
        return partitionInfos.get(partitionIdx).partition();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        FlowFileMessageBatch batch;
        while ((batch = completeBatches.poll()) != null) {
            batch.completeSession();
        }

        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
        String delimiter = context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
        if (delimiter != null) {
            delimiter = delimiter.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }

        final Producer<byte[], byte[]> producer = getProducer();

        if (delimiter == null) {
            // Send the entire FlowFile as a single message.
            final byte[] value = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, value);
                }
            });

            final Integer partition;
            try {
                partition = getPartition(context, flowFile, topic);
            } catch (final Exception e) {
                getLogger().error("Failed to obtain a partition for {} due to {}", new Object[] {flowFile, e});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                session.commit();
                return;
            }

            final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, partition, keyBytes, value);

            final FlowFileMessageBatch messageBatch = new FlowFileMessageBatch(session, flowFile, topic);
            messageBatch.setNumMessages(1);
            activeBatches.add(messageBatch);

            try {
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                        if (exception == null) {
                            // record was successfully sent.
                            messageBatch.addSuccessfulRange(0L, flowFile.getSize(), metadata.offset());
                        } else {
                            messageBatch.addFailedRange(0L, flowFile.getSize(), exception);
                        }
                    }
                });
            } catch (final BufferExhaustedException bee) {
                messageBatch.addFailedRange(0L, flowFile.getSize(), bee);
                context.yield();
                return;
            }
        } else {
            final byte[] delimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);

            // The NonThreadSafeCircularBuffer allows us to add a byte from the stream one at a time and see
            // if it matches some pattern. We can use this to search for the delimiter as we read through
            // the stream of bytes in the FlowFile
            final NonThreadSafeCircularBuffer buffer = new NonThreadSafeCircularBuffer(delimiterBytes);

            final LongHolder messagesSent = new LongHolder(0L);
            final FlowFileMessageBatch messageBatch = new FlowFileMessageBatch(session, flowFile, topic);
            activeBatches.add(messageBatch);

            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream rawIn) throws IOException {
                        byte[] data = null; // contents of a single message

                        boolean streamFinished = false;

                        int nextByte;
                        try (final InputStream bufferedIn = new BufferedInputStream(rawIn);
                            final ByteCountingInputStream in = new ByteCountingInputStream(bufferedIn)) {

                            long messageStartOffset = in.getBytesConsumed();

                            // read until we're out of data.
                            while (!streamFinished) {
                                nextByte = in.read();

                                if (nextByte > -1) {
                                    baos.write(nextByte);
                                }

                                if (nextByte == -1) {
                                    // we ran out of data. This message is complete.
                                    data = baos.toByteArray();
                                    streamFinished = true;
                                } else if (buffer.addAndCompare((byte) nextByte)) {
                                    // we matched our delimiter. This message is complete. We want all of the bytes from the
                                    // underlying BAOS exception for the last 'delimiterBytes.length' bytes because we don't want
                                    // the delimiter itself to be sent.
                                    data = Arrays.copyOfRange(baos.getUnderlyingBuffer(), 0, baos.size() - delimiterBytes.length);
                                }

                                if (data != null) {
                                    final long messageEndOffset = in.getBytesConsumed();

                                    // If the message has no data, ignore it.
                                    if (data.length != 0) {
                                        final Integer partition;
                                        try {
                                            partition = getPartition(context, flowFile, topic);
                                        } catch (final Exception e) {
                                            messageBatch.addFailedRange(messageStartOffset, messageEndOffset, e);
                                            getLogger().error("Failed to obtain a partition for {} due to {}", new Object[] {flowFile, e});
                                            continue;
                                        }


                                        final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, partition, keyBytes, data);
                                        final long rangeStart = messageStartOffset;

                                        try {
                                            producer.send(producerRecord, new Callback() {
                                                @Override
                                                public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                                                    if (exception == null) {
                                                        // record was successfully sent.
                                                        messageBatch.addSuccessfulRange(rangeStart, messageEndOffset, metadata.offset());
                                                    } else {
                                                        messageBatch.addFailedRange(rangeStart, messageEndOffset, exception);
                                                    }
                                                }
                                            });

                                            messagesSent.incrementAndGet();
                                        } catch (final BufferExhaustedException bee) {
                                            // Not enough room in the buffer. Add from the beginning of this message to end of FlowFile as a failed range
                                            messageBatch.addFailedRange(messageStartOffset, flowFile.getSize(), bee);
                                            context.yield();
                                            return;
                                        }

                                    }

                                    // reset BAOS so that we can start a new message.
                                    baos.reset();
                                    data = null;
                                    messageStartOffset = in.getBytesConsumed();
                                }
                            }
                        }
                    }
                });

                messageBatch.setNumMessages(messagesSent.get());
            }
        }
    }


    private static class Range {
        private final long start;
        private final long end;
        private final Long kafkaOffset;

        public Range(final long start, final long end, final Long kafkaOffset) {
            this.start = start;
            this.end = end;
            this.kafkaOffset = kafkaOffset;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public Long getKafkaOffset() {
            return kafkaOffset;
        }

        @Override
        public String toString() {
            return "Range[" + start + "-" + end + "]";
        }
    }

    private class FlowFileMessageBatch {
        private final ProcessSession session;
        private final FlowFile flowFile;
        private final String topic;
        private final long startTime = System.nanoTime();

        private final List<Range> successfulRanges = new ArrayList<>();
        private final List<Range> failedRanges = new ArrayList<>();

        private Exception lastFailureReason;
        private long numMessages = -1L;
        private long completeTime = 0L;
        private boolean canceled = false;

        public FlowFileMessageBatch(final ProcessSession session, final FlowFile flowFile, final String topic) {
            this.session = session;
            this.flowFile = flowFile;
            this.topic = topic;
        }

        public synchronized void cancelOrComplete() {
            if (isComplete()) {
                completeSession();
                return;
            }

            this.canceled = true;

            session.rollback();
            successfulRanges.clear();
            failedRanges.clear();
        }

        public synchronized void addSuccessfulRange(final long start, final long end, final long kafkaOffset) {
            if (canceled) {
                return;
            }

            successfulRanges.add(new Range(start, end, kafkaOffset));

            if (isComplete()) {
                activeBatches.remove(this);
                completeBatches.add(this);
                completeTime = System.nanoTime();
            }
        }

        public synchronized void addFailedRange(final long start, final long end, final Exception e) {
            if (canceled) {
                return;
            }

            failedRanges.add(new Range(start, end, null));
            lastFailureReason = e;

            if (isComplete()) {
                activeBatches.remove(this);
                completeBatches.add(this);
                completeTime = System.nanoTime();
            }
        }

        private boolean isComplete() {
            return !canceled && (numMessages > -1) && (successfulRanges.size() + failedRanges.size() >= numMessages);
        }

        public synchronized void setNumMessages(final long msgCount) {
            this.numMessages = msgCount;

            if (isComplete()) {
                activeBatches.remove(this);
                completeBatches.add(this);
                completeTime = System.nanoTime();
            }
        }

        private Long getMin(final Long a, final Long b) {
            if (a == null && b == null) {
                return null;
            }

            if (a == null) {
                return b;
            }

            if (b == null) {
                return a;
            }

            return Math.min(a, b);
        }

        private Long getMax(final Long a, final Long b) {
            if (a == null && b == null) {
                return null;
            }

            if (a == null) {
                return b;
            }

            if (b == null) {
                return a;
            }

            return Math.max(a, b);
        }

        private void transferRanges(final List<Range> ranges, final Relationship relationship) {
            Collections.sort(ranges, new Comparator<Range>() {
                @Override
                public int compare(final Range o1, final Range o2) {
                    return Long.compare(o1.getStart(), o2.getStart());
                }
            });

            for (int i = 0; i < ranges.size(); i++) {
                Range range = ranges.get(i);
                int count = 1;
                Long smallestKafkaOffset = range.getKafkaOffset();
                Long largestKafkaOffset = range.getKafkaOffset();

                while (i + 1 < ranges.size()) {
                    // Check if the next range in the List continues where this one left off.
                    final Range nextRange = ranges.get(i + 1);

                    if (nextRange.getStart() == range.getEnd()) {
                        // We have two ranges in a row that are contiguous; combine them into a single Range.
                        range = new Range(range.getStart(), nextRange.getEnd(), null);

                        smallestKafkaOffset = getMin(smallestKafkaOffset, nextRange.getKafkaOffset());
                        largestKafkaOffset = getMax(largestKafkaOffset, nextRange.getKafkaOffset());
                        count++;
                        i++;
                    } else {
                        break;
                    }
                }

                // Create a FlowFile for this range.
                FlowFile child = session.clone(flowFile, range.getStart(), range.getEnd() - range.getStart());
                if (relationship == REL_SUCCESS) {
                    session.getProvenanceReporter().send(child, getTransitUri(), "Sent " + count + " messages; Kafka offsets range from " + smallestKafkaOffset + " to " + largestKafkaOffset);
                    session.transfer(child, relationship);
                } else {
                    session.transfer(session.penalize(child), relationship);
                }
            }
        }

        private String getTransitUri() {
            final List<PartitionInfo> partitions = getProducer().partitionsFor(topic);
            if (partitions.isEmpty()) {
                return "kafka://unknown-host" + "/topics/" + topic;
            }

            final PartitionInfo info = partitions.get(0);
            final Node leader = info.leader();
            final String host = leader.host();
            final int port = leader.port();

            return "kafka://" + host + ":" + port + "/topics/" + topic;
        }

        public synchronized void completeSession() {
            if (canceled) {
                return;
            }

            if (successfulRanges.isEmpty() && failedRanges.isEmpty()) {
                getLogger().info("Completed processing {} but sent 0 FlowFiles to Kafka", new Object[] {flowFile});
                session.transfer(flowFile, REL_SUCCESS);
                session.commit();
                return;
            }

            if (successfulRanges.isEmpty()) {
                getLogger().error("Failed to send {} to Kafka; routing to 'failure'; last failure reason reported was {};", new Object[] {flowFile, lastFailureReason});
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                session.commit();
                return;
            }

            if (failedRanges.isEmpty()) {
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(completeTime - startTime);

                if (successfulRanges.size() == 1) {
                    final Long kafkaOffset = successfulRanges.get(0).getKafkaOffset();
                    final String msg = "Sent 1 message" + ((kafkaOffset == null) ? "" : ("; Kafka offset = " + kafkaOffset));
                    session.getProvenanceReporter().send(flowFile, getTransitUri(), msg);
                } else {
                    long smallestKafkaOffset = successfulRanges.get(0).getKafkaOffset();
                    long largestKafkaOffset = successfulRanges.get(0).getKafkaOffset();

                    for (final Range range : successfulRanges) {
                        smallestKafkaOffset = Math.min(smallestKafkaOffset, range.getKafkaOffset());
                        largestKafkaOffset = Math.max(largestKafkaOffset, range.getKafkaOffset());
                    }

                    session.getProvenanceReporter().send(flowFile, getTransitUri(),
                        "Sent " + successfulRanges.size() + " messages; Kafka offsets range from " + smallestKafkaOffset + " to " + largestKafkaOffset);
                }

                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("Successfully sent {} messages to Kafka for {} in {} millis", new Object[] {successfulRanges.size(), flowFile, transferMillis});
                session.commit();
                return;
            }

            // At this point, the successful ranges is not empty and the failed ranges is not empty. This indicates that some messages made their way to Kafka
            // successfully and some failed. We will address this by splitting apart the source FlowFile into children and sending the successful messages to 'success'
            // and the failed messages to 'failure'.
            transferRanges(successfulRanges, REL_SUCCESS);
            transferRanges(failedRanges, REL_FAILURE);
            session.remove(flowFile);
            getLogger().error("Successfully sent {} messages to Kafka but failed to send {} messages; the last error received was {}",
                new Object[] {successfulRanges.size(), failedRanges.size(), lastFailureReason});
            session.commit();
        }
    }
}
