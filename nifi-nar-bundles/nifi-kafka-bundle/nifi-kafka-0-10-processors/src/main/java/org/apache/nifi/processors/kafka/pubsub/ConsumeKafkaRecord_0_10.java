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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

@CapabilityDescription("Consumes messages from Apache Kafka specifically built against the Kafka 0.10.x Consumer API. "
    + "The complementary NiFi processor for sending messages is PublishKafkaRecord_0_10. Please note that, at this time, the Processor assumes that "
    + "all records that are retrieved from a given partition have the same schema. If any of the Kafka messages are pulled but cannot be parsed or written with the "
    + "configured Record Reader or Record Writer, the contents of the message will be written to a separate FlowFile, and that FlowFile will be transferred to the "
    + "'parse.failure' relationship. Otherwise, each FlowFile is sent to the 'success' relationship and may contain many individual messages within the single FlowFile. "
    + "A 'record.count' attribute is added to indicate how many records are contained in the FlowFile.")
@Tags({"Kafka", "Get", "Record", "csv", "avro", "json", "Ingest", "Ingress", "Topic", "PubSub", "Consume", "0.10.x"})
@WritesAttributes({
    @WritesAttribute(attribute = "record.count", description = "The number of records received"),
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type that is provided by the configured Record Writer"),
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_PARTITION, description = "The partition of the topic the records are from"),
    @WritesAttribute(attribute = KafkaProcessorUtils.KAFKA_TOPIC, description = "The topic records are from")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
        description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ")
@SeeAlso({ConsumeKafka_0_10.class, PublishKafka_0_10.class, PublishKafkaRecord_0_10.class})
public class ConsumeKafkaRecord_0_10 extends AbstractProcessor {

    static final AllowableValue OFFSET_EARLIEST = new AllowableValue("earliest", "earliest", "Automatically reset the offset to the earliest offset");
    static final AllowableValue OFFSET_LATEST = new AllowableValue("latest", "latest", "Automatically reset the offset to the latest offset");
    static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none", "Throw exception to the consumer if no previous offset is found for the consumer's group");
    static final AllowableValue TOPIC_NAME = new AllowableValue("names", "names", "Topic is a full topic name or comma separated list of names");
    static final AllowableValue TOPIC_PATTERN = new AllowableValue("pattern", "pattern", "Topic is a regex using the Java Pattern syntax");

    static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name(s)")
            .description("The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor TOPIC_TYPE = new PropertyDescriptor.Builder()
            .name("topic_type")
            .displayName("Topic Name Format")
            .description("Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression")
            .required(true)
            .allowableValues(TOPIC_NAME, TOPIC_PATTERN)
            .defaultValue(TOPIC_NAME.getValue())
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("The Record Reader to use for incoming FlowFiles")
        .identifiesControllerService(RecordReaderFactory.class)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("The Record Writer to use in order to serialize the data before sending to Kafka")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
        .name("group.id")
            .displayName("Group ID")
            .description("A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
        .name("auto.offset.reset")
            .displayName("Offset Reset")
            .description("Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any "
                    + "more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.")
            .required(true)
            .allowableValues(OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_NONE)
            .defaultValue(OFFSET_LATEST.getValue())
            .build();

    static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder()
            .name("max.poll.records")
            .displayName("Max Poll Records")
            .description("Specifies the maximum number of records Kafka should return in a single poll.")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_UNCOMMITTED_TIME = new PropertyDescriptor.Builder()
            .name("max-uncommit-offset-wait")
            .displayName("Max Uncommitted Time")
            .description("Specifies the maximum amount of time allowed to pass before offsets must be committed. "
                    + "This value impacts how often offsets will be committed.  Committing offsets less often increases "
                    + "throughput but also increases the window of potential data duplication in the event of a rebalance "
                    + "or JVM restart between commits.  This value is also related to maximum poll records and the use "
                    + "of a message demarcator.  When using a message demarcator we can have far more uncommitted messages "
                    + "than when we're not as there is much less for us to keep track of in memory.")
            .required(false)
            .defaultValue("1 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.")
            .build();
    static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the "
                + "message will be routed to this Relationship as its own individual FlowFile.")
            .build();

    static final List<PropertyDescriptor> DESCRIPTORS;
    static final Set<Relationship> RELATIONSHIPS;

    private volatile ConsumerPool consumerPool = null;
    private final Set<ConsumerLease> activeLeases = Collections.synchronizedSet(new HashSet<>());

    static {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KafkaProcessorUtils.BOOTSTRAP_SERVERS);
        descriptors.add(TOPICS);
        descriptors.add(TOPIC_TYPE);
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        descriptors.add(KafkaProcessorUtils.SECURITY_PROTOCOL);
        descriptors.add(KafkaProcessorUtils.KERBEROS_CREDENTIALS_SERVICE);
        descriptors.add(KafkaProcessorUtils.JAAS_SERVICE_NAME);
        descriptors.add(KafkaProcessorUtils.USER_PRINCIPAL);
        descriptors.add(KafkaProcessorUtils.USER_KEYTAB);
        descriptors.add(KafkaProcessorUtils.SSL_CONTEXT_SERVICE);
        descriptors.add(GROUP_ID);
        descriptors.add(AUTO_OFFSET_RESET);
        descriptors.add(MAX_POLL_RECORDS);
        descriptors.add(MAX_UNCOMMITTED_TIME);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_PARSE_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnStopped
    public void close() {
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

        return consumerPool = createConsumerPool(context, getLogger());
    }

    protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
        final int maxLeases = context.getMaxConcurrentTasks();
        final long maxUncommittedTime = context.getProperty(MAX_UNCOMMITTED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        final Map<String, Object> props = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ConsumerConfig.class, props);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        final String topicListing = context.getProperty(ConsumeKafkaRecord_0_10.TOPICS).evaluateAttributeExpressions().getValue();
        final String topicType = context.getProperty(ConsumeKafkaRecord_0_10.TOPIC_TYPE).evaluateAttributeExpressions().getValue();
        final List<String> topics = new ArrayList<>();
        final String securityProtocol = context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL).getValue();
        final String bootstrapServers = context.getProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        if (topicType.equals(TOPIC_NAME.getValue())) {
          for (final String topic : topicListing.split(",", 100)) {
              final String trimmedName = topic.trim();
              if (!trimmedName.isEmpty()) {
                  topics.add(trimmedName);
              }
          }

            return new ConsumerPool(maxLeases, readerFactory, writerFactory, props, topics, maxUncommittedTime, securityProtocol, bootstrapServers, log);
        } else if (topicType.equals(TOPIC_PATTERN.getValue())) {
          final Pattern topicPattern = Pattern.compile(topicListing.trim());
            return new ConsumerPool(maxLeases, readerFactory, writerFactory, props, topicPattern, maxUncommittedTime, securityProtocol, bootstrapServers, log);
        } else {
          getLogger().error("Subscription type has an unknown value {}", new Object[] {topicType});
          return null;
        }
    }

    @OnUnscheduled
    public void interruptActiveThreads() {
        // There are known issues with the Kafka client library that result in the client code hanging
        // indefinitely when unable to communicate with the broker. In order to address this, we will wait
        // up to 30 seconds for the Threads to finish and then will call Consumer.wakeup() to trigger the
        // thread to wakeup when it is blocked, waiting on a response.
        final long nanosToWait = TimeUnit.SECONDS.toNanos(5L);
        final long start = System.nanoTime();
        while (System.nanoTime() - start < nanosToWait && !activeLeases.isEmpty()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (!activeLeases.isEmpty()) {
            int count = 0;
            for (final ConsumerLease lease : activeLeases) {
                getLogger().info("Consumer {} has not finished after waiting 30 seconds; will attempt to wake-up the lease", new Object[] {lease});
                lease.wakeup();
                count++;
            }

            getLogger().info("Woke up {} consumers", new Object[] {count});
        }

        activeLeases.clear();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ConsumerPool pool = getConsumerPool(context);
        if (pool == null) {
            context.yield();
            return;
        }

        try (final ConsumerLease lease = pool.obtainConsumer(session, context)) {
            if (lease == null) {
                context.yield();
                return;
            }

            activeLeases.add(lease);
            try {
                while (this.isScheduled() && lease.continuePolling()) {
                    lease.poll();
                }
                if (this.isScheduled() && !lease.commit()) {
                    context.yield();
                }
            } catch (final WakeupException we) {
                getLogger().warn("Was interrupted while trying to communicate with Kafka with lease {}. "
                    + "Will roll back session and discard any partially received data.", new Object[] {lease});
            } catch (final KafkaException kex) {
                getLogger().error("Exception while interacting with Kafka so will close the lease {} due to {}",
                        new Object[]{lease, kex}, kex);
            } catch (final Throwable t) {
                getLogger().error("Exception while processing data from kafka so will close the lease {} due to {}",
                        new Object[]{lease, t}, t);
            } finally {
                activeLeases.remove(lease);
            }
        }
    }
}
