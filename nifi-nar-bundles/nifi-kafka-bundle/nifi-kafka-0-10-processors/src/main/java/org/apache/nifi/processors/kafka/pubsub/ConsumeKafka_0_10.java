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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.DatatypeConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.SECURITY_PROTOCOL;

@CapabilityDescription("Consumes messages from Apache Kafka specifically built against the Kafka 0.10 Consumer API. "
        + " Please note there are cases where the publisher can get into an indefinite stuck state.  We are closely monitoring"
        + " how this evolves in the Kafka community and will take advantage of those fixes as soon as we can.  In the mean time"
        + " it is possible to enter states where the only resolution will be to restart the JVM NiFi runs on.")
@Tags({"Kafka", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume", "0.10"})
@WritesAttributes({
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_COUNT, description = "The number of messages written if more than one"),
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_KEY, description = "The key of message if present and if single message. "
        + "How the key is encoded depends on the value of the 'Key Attribute Encoding' property."),
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_OFFSET, description = "The offset of the message in the partition of the topic."),
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_PARTITION, description = "The partition of the topic the message or message bundle is from"),
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_TOPIC, description = "The topic the message or message bundle is from")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
        description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ")
public class ConsumeKafka_0_10 extends AbstractProcessor {

    private static final long FIVE_MB = 5L * 1024L * 1024L;

    static final AllowableValue OFFSET_EARLIEST = new AllowableValue("earliest", "earliest", "Automatically reset the offset to the earliest offset");

    static final AllowableValue OFFSET_LATEST = new AllowableValue("latest", "latest", "Automatically reset the offset to the latest offset");

    static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none", "Throw exception to the consumer if no previous offset is found for the consumer's group");

    static final AllowableValue UTF8_ENCODING = new AllowableValue("utf-8", "UTF-8 Encoded", "The key is interpreted as a UTF-8 Encoded string.");
    static final AllowableValue HEX_ENCODING = new AllowableValue("hex", "Hex Encoded",
        "The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters");

    static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name(s)")
            .description("The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma seperated.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.GROUP_ID_CONFIG)
            .displayName("Group ID")
            .description("A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
            .displayName("Offset Reset")
            .description("Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any "
                    + "more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.")
            .required(true)
            .allowableValues(OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_NONE)
            .defaultValue(OFFSET_LATEST.getValue())
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
            .expressionLanguageSupported(true)
            .description("Since KafkaConsumer receives messages in batches, you have an option to output FlowFiles which contains "
                    + "all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use "
                    + "for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received "
                    + "will result in a single FlowFile which  "
                    + "time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
            .build();
    static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder()
            .name("max.poll.records")
            .displayName("Max Poll Records")
            .description("Specifies the maximum number of records Kafka should return in a single poll.")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.")
            .build();

    static final List<PropertyDescriptor> DESCRIPTORS;
    static final Set<Relationship> RELATIONSHIPS;

    private volatile byte[] demarcatorBytes = null;
    private volatile ConsumerPool consumerPool = null;

    static {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.addAll(KafkaProcessorUtils.getCommonPropertyDescriptors());
        descriptors.add(TOPICS);
        descriptors.add(GROUP_ID);
        descriptors.add(AUTO_OFFSET_RESET);
        descriptors.add(KEY_ATTRIBUTE_ENCODING);
        descriptors.add(MESSAGE_DEMARCATOR);
        descriptors.add(MAX_POLL_RECORDS);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
        RELATIONSHIPS = Collections.singleton(REL_SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void prepareProcessing(final ProcessContext context) {
        this.demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet()
                ? context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)
                : null;
    }

    @OnStopped
    public void close() {
        demarcatorBytes = null;
        final ConsumerPool pool = consumerPool;
        consumerPool = null;
        if (pool != null) {
            pool.close();
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(new KafkaProcessorUtils.KafkaConfigValidator(ConsumerConfig.class)).dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return KafkaProcessorUtils.validateCommonProperties(validationContext);
    }

    private synchronized ConsumerPool getConsumerPool(final ProcessContext context) {
        ConsumerPool pool = consumerPool;
        if (pool != null) {
            return pool;
        }

        final Map<String, String> props = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ConsumerConfig.class, props);
        final String topicListing = context.getProperty(TOPICS).evaluateAttributeExpressions().getValue();
        final List<String> topics = new ArrayList<>();
        for (final String topic : topicListing.split(",", 100)) {
            final String trimmedName = topic.trim();
            if (!trimmedName.isEmpty()) {
                topics.add(trimmedName);
            }
        }
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return consumerPool = createConsumerPool(context.getMaxConcurrentTasks(), topics, props, getLogger());
    }

    protected ConsumerPool createConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> props, final ComponentLog log) {
        return new ConsumerPool(maxLeases, topics, props, log);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final long startTimeNanos = System.nanoTime();
        final ConsumerPool pool = getConsumerPool(context);
        if (pool == null) {
            context.yield();
            return;
        }
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecordMap = new HashMap<>();

        try (final ConsumerLease lease = pool.obtainConsumer()) {
            try {
                if (lease == null) {
                    context.yield();
                    return;
                }

                final boolean foundData = gatherDataFromKafka(lease, partitionRecordMap, context);
                if (!foundData) {
                    session.rollback();
                    return;
                }

                writeSessionData(context, session, partitionRecordMap, startTimeNanos);
                //At-least once commit handling (if order is reversed it is at-most once)
                session.commit();
                commitOffsets(lease, partitionRecordMap);
            } catch (final KafkaException ke) {
                lease.poison();
                getLogger().error("Problem while accessing kafka consumer " + ke, ke);
                context.yield();
                session.rollback();
            }
        }
    }

    private void commitOffsets(final ConsumerLease lease, final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecordMap) {
        final Map<TopicPartition, OffsetAndMetadata> partOffsetMap = new HashMap<>();
        partitionRecordMap.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .forEach((entry) -> {
                    long maxOffset = entry.getValue().stream()
                            .mapToLong(record -> record.offset())
                            .max()
                            .getAsLong();
                    partOffsetMap.put(entry.getKey(), new OffsetAndMetadata(maxOffset + 1L));
                });
        lease.commitOffsets(partOffsetMap);
    }

    private void writeSessionData(
            final ProcessContext context, final ProcessSession session,
            final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecordMap,
            final long startTimeNanos) {
        if (demarcatorBytes != null) {
            partitionRecordMap.entrySet().stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .forEach(entry -> {
                        writeData(context, session, entry.getValue(), startTimeNanos);
                    });
        } else {
            partitionRecordMap.entrySet().stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .flatMap(entry -> entry.getValue().stream())
                    .forEach(record -> {
                        writeData(context, session, Collections.singletonList(record), startTimeNanos);
                    });
        }
    }

    private String encodeKafkaKey(final byte[] key, final String encoding) {
        if (key == null) {
            return null;
        }

        if (HEX_ENCODING.getValue().equals(encoding)) {
            return DatatypeConverter.printHexBinary(key);
        } else if (UTF8_ENCODING.getValue().equals(encoding)) {
            return new String(key, StandardCharsets.UTF_8);
        } else {
            return null;    // won't happen because it is guaranteed by the Allowable Values
        }
    }

    private void writeData(final ProcessContext context, final ProcessSession session, final List<ConsumerRecord<byte[], byte[]>> records, final long startTimeNanos) {
        final ConsumerRecord<byte[], byte[]> firstRecord = records.get(0);
        final String offset = String.valueOf(firstRecord.offset());
        final String keyValue = encodeKafkaKey(firstRecord.key(), context.getProperty(KEY_ATTRIBUTE_ENCODING).getValue());
        final String topic = firstRecord.topic();
        final String partition = String.valueOf(firstRecord.partition());
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, out -> {
            boolean useDemarcator = false;
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                if (useDemarcator) {
                    out.write(demarcatorBytes);
                }
                out.write(record.value());
                useDemarcator = true;
            }
        });
        final Map<String, String> kafkaAttrs = new HashMap<>();
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_OFFSET, offset);
        if (keyValue != null && records.size() == 1) {
            kafkaAttrs.put(KafkaProcessorUtils.KAFKA_KEY, keyValue);
        }
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_PARTITION, partition);
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_TOPIC, topic);
        if (records.size() > 1) {
            kafkaAttrs.put(KafkaProcessorUtils.KAFKA_COUNT, String.valueOf(records.size()));
        }
        flowFile = session.putAllAttributes(flowFile, kafkaAttrs);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        final String transitUri = KafkaProcessorUtils.buildTransitURI(
                context.getProperty(SECURITY_PROTOCOL).getValue(),
                context.getProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS).getValue(),
                topic);
        session.getProvenanceReporter().receive(flowFile, transitUri, executionDurationMillis);
        this.getLogger().debug("Created {} containing {} messages from Kafka topic {}, partition {}, starting offset {} in {} millis",
                new Object[]{flowFile, records.size(), topic, partition, offset, executionDurationMillis});
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * Populates the given partitionRecordMap with new records until we poll
     * that returns no records or until we have enough data. It is important to
     * ensure we keep items grouped by their topic and partition so that when we
     * bundle them we bundle them intelligently and so that we can set offsets
     * properly even across multiple poll calls.
     */
    private boolean gatherDataFromKafka(final ConsumerLease lease, final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecordMap, ProcessContext context) {
        final long startNanos = System.nanoTime();
        boolean foundData = false;
        ConsumerRecords<byte[], byte[]> records;
        final int maxRecords = context.getProperty(MAX_POLL_RECORDS).asInteger();
        do {
            records = lease.poll();

            for (final TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> currList = partitionRecordMap.get(partition);
                if (currList == null) {
                    currList = new ArrayList<>();
                    partitionRecordMap.put(partition, currList);
                }
                currList.addAll(records.records(partition));
                if (currList.size() > 0) {
                    foundData = true;
                }
            }
            //If we received data and we still want to get more
        } while (!records.isEmpty() && !checkIfGatheredEnoughData(partitionRecordMap, maxRecords, startNanos));
        return foundData;
    }

    /**
     * Determines if we have enough data as-is and should move on.
     *
     * @return true if we've been gathering for more than 500 ms or if we're
     * demarcating and have more than 50 flowfiles worth or if we're per message
     * and have more than 2000 flowfiles or if totalMessageSize is greater than
     * two megabytes; false otherwise
     *
     * Implementation note: 500 millis and 5 MB are magic numbers. These may
     * need to be tuned. They get at how often offsets will get committed to
     * kafka relative to how many records will get buffered into memory in a
     * poll call before writing to repos.
     */
    private boolean checkIfGatheredEnoughData(final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecordMap, final int maxRecords, final long startTimeNanos) {

        final long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);

        if (durationMillis > 500) {
            return true;
        }

        int topicPartitionsFilled = 0;
        int totalRecords = 0;
        long totalRecordSize = 0;

        for (final List<ConsumerRecord<byte[], byte[]>> recordList : partitionRecordMap.values()) {
            if (!recordList.isEmpty()) {
                topicPartitionsFilled++;
            }
            totalRecords += recordList.size();
            for (final ConsumerRecord<byte[], byte[]> rec : recordList) {
                totalRecordSize += rec.value().length;
            }
        }

        if (demarcatorBytes != null && demarcatorBytes.length > 0) {
            return topicPartitionsFilled > 50;
        } else if (totalRecordSize > FIVE_MB) {
            return true;
        } else {
            return totalRecords > maxRecords;
        }
    }

}
