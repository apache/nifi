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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.Charset;
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
import java.util.regex.Pattern;

import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.HEX_ENCODING;
import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.UTF8_ENCODING;

@CapabilityDescription("Consumes messages from Apache Kafka specifically built against the Kafka 2.0 Consumer API. "
    + "The complementary NiFi processor for sending messages is PublishKafka_2_0.")
@Tags({"Kafka", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume", "2.0"})
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
public class ConsumeKafka_2_0 extends AbstractProcessor {

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

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.GROUP_ID_CONFIG)
            .displayName("Group ID")
            .description("A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .description("Since KafkaConsumer receives messages in batches, you have an option to output FlowFiles which contains "
                    + "all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use "
                    + "for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received "
                    + "will result in a single FlowFile which  "
                    + "time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
            .build();
    static final PropertyDescriptor HEADER_NAME_REGEX = new PropertyDescriptor.Builder()
        .name("header-name-regex")
        .displayName("Headers to Add as Attributes (Regex)")
        .description("A Regular Expression that is matched against all message headers. "
            + "Any message header whose name matches the regex will be added to the FlowFile as an Attribute. "
            + "If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by "
            + "the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like "
            + "\".*\" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling "
            + "the messages together efficiently.")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
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
    static final PropertyDescriptor COMMS_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Communications Timeout")
        .displayName("Communications Timeout")
        .description("Specifies the timeout that the consumer should use when communicating with the Kafka Broker")
        .required(true)
        .defaultValue("60 secs")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    static final PropertyDescriptor HONOR_TRANSACTIONS = new PropertyDescriptor.Builder()
        .name("honor-transactions")
        .displayName("Honor Transactions")
        .description("Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an \"isolation level\" of "
            + "read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If "
            + "this value is true, NiFi will not receive any messages for which the producer's transaction was canceled, but this can result in some latency since the consumer must wait "
            + "for the producer to finish its entire transaction instead of pulling as the messages become available.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    static final PropertyDescriptor MESSAGE_HEADER_ENCODING = new PropertyDescriptor.Builder()
        .name("message-header-encoding")
        .displayName("Message Header Encoding")
        .description("Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. "
            + "This property indicates the Character Encoding to use for deserializing the headers.")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .required(false)
        .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles received from Kafka. Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.")
        .build();

    static final List<PropertyDescriptor> DESCRIPTORS;
    static final Set<Relationship> RELATIONSHIPS;

    private volatile ConsumerPool consumerPool = null;
    private final Set<ConsumerLease> activeLeases = Collections.synchronizedSet(new HashSet<>());

    static {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.addAll(KafkaProcessorUtils.getCommonPropertyDescriptors());
        descriptors.add(TOPICS);
        descriptors.add(TOPIC_TYPE);
        descriptors.add(HONOR_TRANSACTIONS);
        descriptors.add(GROUP_ID);
        descriptors.add(AUTO_OFFSET_RESET);
        descriptors.add(KEY_ATTRIBUTE_ENCODING);
        descriptors.add(MESSAGE_DEMARCATOR);
        descriptors.add(MESSAGE_HEADER_ENCODING);
        descriptors.add(HEADER_NAME_REGEX);
        descriptors.add(MAX_POLL_RECORDS);
        descriptors.add(MAX_UNCOMMITTED_TIME);
        descriptors.add(COMMS_TIMEOUT);
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
        final byte[] demarcator = context.getProperty(ConsumeKafka_2_0.MESSAGE_DEMARCATOR).isSet()
                ? context.getProperty(ConsumeKafka_2_0.MESSAGE_DEMARCATOR).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)
                : null;
        final Map<String, Object> props = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ConsumerConfig.class, props);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        final String topicListing = context.getProperty(ConsumeKafka_2_0.TOPICS).evaluateAttributeExpressions().getValue();
        final String topicType = context.getProperty(ConsumeKafka_2_0.TOPIC_TYPE).evaluateAttributeExpressions().getValue();
        final List<String> topics = new ArrayList<>();
        final String keyEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).getValue();
        final String securityProtocol = context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL).getValue();
        final String bootstrapServers = context.getProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        final boolean honorTransactions = context.getProperty(HONOR_TRANSACTIONS).asBoolean();
        final int commsTimeoutMillis = context.getProperty(COMMS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, commsTimeoutMillis);

        final String charsetName = context.getProperty(MESSAGE_HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(charsetName);

        final String headerNameRegex = context.getProperty(HEADER_NAME_REGEX).getValue();
        final Pattern headerNamePattern = headerNameRegex == null ? null : Pattern.compile(headerNameRegex);

        if (topicType.equals(TOPIC_NAME.getValue())) {
            for (final String topic : topicListing.split(",", 100)) {
                final String trimmedName = topic.trim();
                if (!trimmedName.isEmpty()) {
                    topics.add(trimmedName);
                }
            }

            return new ConsumerPool(maxLeases, demarcator, props, topics, maxUncommittedTime, keyEncoding, securityProtocol,
                bootstrapServers, log, honorTransactions, charset, headerNamePattern);
        } else if (topicType.equals(TOPIC_PATTERN.getValue())) {
            final Pattern topicPattern = Pattern.compile(topicListing.trim());
            return new ConsumerPool(maxLeases, demarcator, props, topicPattern, maxUncommittedTime, keyEncoding, securityProtocol,
                bootstrapServers, log, honorTransactions, charset, headerNamePattern);
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
