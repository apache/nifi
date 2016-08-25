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

import java.io.Closeable;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.bind.DatatypeConverter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"Apache", "Kafka", "Put", "Send", "Message", "PubSub", "0.9.x"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka using the Kafka 0.9 producer. "
        + "The messages to send may be individual FlowFiles or may be delimited, using a "
        + "user-specified delimiter, such as a new-line. "
        + " Please note there are cases where the publisher can get into an indefinite stuck state.  We are closely monitoring"
        + " how this evolves in the Kafka community and will take advantage of those fixes as soon as we can.  In the mean time"
        + " it is possible to enter states where the only resolution will be to restart the JVM NiFi runs on.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
        description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration. ")
public class PublishKafka extends AbstractSessionFactoryProcessor {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected static final String FAILED_PROC_ID_ATTR = "failed.proc.id";

    protected static final String FAILED_LAST_ACK_IDX = "failed.last.idx";

    protected static final String FAILED_TOPIC_ATTR = "failed.topic";

    protected static final String FAILED_KEY_ATTR = "failed.key";

    protected static final String FAILED_DELIMITER_ATTR = "failed.delimiter";

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

    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("The name of the Kafka Topic to publish to.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name(ProducerConfig.ACKS_CONFIG)
            .displayName("Delivery Guarantee")
            .description("Specifies the requirement for guaranteeing that a message is sent to Kafka. Corresponds to Kafka's 'acks' property.")
            .required(true)
            .expressionLanguageSupported(false)
            .allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
            .defaultValue(DELIVERY_BEST_EFFORT.getValue())
            .build();

    static final PropertyDescriptor META_WAIT_TIME = new PropertyDescriptor.Builder()
            .name(ProducerConfig.MAX_BLOCK_MS_CONFIG)
            .displayName("Meta Data Wait Time")
            .description("The amount of time KafkaConsumer will wait to obtain metadata during the 'send' call before failing the "
                    + "entire 'send' call. Corresponds to Kafka's 'max.block.ms' property")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("30 sec")
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
            .description("The Key to use for the Message.  It will be serialized as UTF-8 bytes. "
                    + "If not specified then the flow file attribute kafka.key.hex is used if present "
                    + "and we're not demarcating. In that case the hex string is coverted to its byte"
                    + "form and written as a byte[] key.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("message-demarcator")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                    + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                    + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka message. "
                    + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on your OS.")
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

    static final List<PropertyDescriptor> DESCRIPTORS;

    static final Set<Relationship> RELATIONSHIPS;

    private volatile String brokers;

    private final AtomicInteger taskCounter = new AtomicInteger();

    private volatile boolean acceptTask = true;

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    static {
        final List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.addAll(KafkaProcessorUtils.getCommonPropertyDescriptors());
        _descriptors.add(TOPIC);
        _descriptors.add(DELIVERY_GUARANTEE);
        _descriptors.add(KEY);
        _descriptors.add(MESSAGE_DEMARCATOR);
        _descriptors.add(MAX_REQUEST_SIZE);
        _descriptors.add(META_WAIT_TIME);
        _descriptors.add(PARTITION_CLASS);
        _descriptors.add(COMPRESSION_CODEC);

        DESCRIPTORS = Collections.unmodifiableList(_descriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(new KafkaProcessorUtils.KafkaConfigValidator(ProducerConfig.class)).dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        return KafkaProcessorUtils.validateCommonProperties(validationContext);
    }

    volatile KafkaPublisher kafkaPublisher;

    /**
     * This thread-safe operation will delegate to
     * {@link #rendezvousWithKafka(ProcessContext, ProcessSession)} after first
     * checking and creating (if necessary) Kafka resource which could be either
     * {@link KafkaPublisher} or {@link KafkaConsumer}. It will also close and
     * destroy the underlying Kafka resource upon catching an {@link Exception}
     * raised by {@link #rendezvousWithKafka(ProcessContext, ProcessSession)}.
     * After Kafka resource is destroyed it will be re-created upon the next
     * invocation of this operation essentially providing a self healing
     * mechanism to deal with potentially corrupted resource.
     * <p>
     * Keep in mind that upon catching an exception the state of this processor
     * will be set to no longer accept any more tasks, until Kafka resource is
     * reset. This means that in a multi-threaded situation currently executing
     * tasks will be given a chance to complete while no new tasks will be
     * accepted.
     *
     * @param context context
     * @param sessionFactory factory
     */
    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.acceptTask) { // acts as a circuit breaker to allow existing tasks to wind down so 'kafkaPublisher' can be reset before new tasks are accepted.
            this.taskCounter.incrementAndGet();
            final ProcessSession session = sessionFactory.createSession();
            try {
                /*
                 * We can't be doing double null check here since as a pattern
                 * it only works for lazy init but not reset, which is what we
                 * are doing here. In fact the first null check is dangerous
                 * since 'kafkaPublisher' can become null right after its null
                 * check passed causing subsequent NPE.
                 */
                synchronized (this) {
                    if (this.kafkaPublisher == null) {
                        this.kafkaPublisher = this.buildKafkaResource(context, session);
                    }
                }

                /*
                 * The 'processed' boolean flag does not imply any failure or success. It simply states that:
                 * - ConsumeKafka - some messages were received form Kafka and 1_ FlowFile were generated
                 * - PublishKafka0_10 - some messages were sent to Kafka based on existence of the input FlowFile
                 */
                boolean processed = this.rendezvousWithKafka(context, session);
                session.commit();
                if (!processed) {
                    context.yield();
                }
            } catch (Throwable e) {
                this.acceptTask = false;
                session.rollback(true);
                this.getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, e});
            } finally {
                synchronized (this) {
                    if (this.taskCounter.decrementAndGet() == 0 && !this.acceptTask) {
                        this.close();
                        this.acceptTask = true;
                    }
                }
            }
        } else {
            this.logger.debug("Task was not accepted due to the processor being in 'reset' state. It will be re-submitted upon completion of the reset.");
            this.getLogger().debug("Task was not accepted due to the processor being in 'reset' state. It will be re-submitted upon completion of the reset.");
            context.yield();
        }
    }

    /**
     * Will call {@link Closeable#close()} on the target resource after which
     * the target resource will be set to null. Should only be called when there
     * are no more threads being executed on this processor or when it has been
     * verified that only a single thread remains.
     *
     * @see KafkaPublisher
     * @see KafkaConsumer
     */
    @OnStopped
    public void close() {
        try {
            if (this.kafkaPublisher != null) {
                try {
                    this.kafkaPublisher.close();
                } catch (Exception e) {
                    this.getLogger().warn("Failed while closing " + this.kafkaPublisher, e);
                }
            }
        } finally {
            this.kafkaPublisher = null;
        }
    }

    /**
     * Will rendezvous with Kafka if {@link ProcessSession} contains
     * {@link FlowFile} producing a result {@link FlowFile}.
     * <br>
     * The result {@link FlowFile} that is successful is then transfered to
     * {@link #REL_SUCCESS}
     * <br>
     * The result {@link FlowFile} that is failed is then transfered to
     * {@link #REL_FAILURE}
     *
     */
    protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile != null) {
            long start = System.nanoTime();
            flowFile = this.doRendezvousWithKafka(flowFile, context, session);
            Relationship relationship = REL_SUCCESS;
            if (!this.isFailedFlowFile(flowFile)) {
                String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
                long executionDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                String transitUri = KafkaProcessorUtils.buildTransitURI(context.getProperty(KafkaProcessorUtils.SECURITY_PROTOCOL).getValue(), this.brokers, topic);
                session.getProvenanceReporter().send(flowFile, transitUri, "Sent " + flowFile.getAttribute(MSG_COUNT) + " Kafka messages", executionDuration);
                this.getLogger().debug("Successfully sent {} to Kafka as {} message(s) in {} millis",
                        new Object[]{flowFile, flowFile.getAttribute(MSG_COUNT), executionDuration});
            } else {
                relationship = REL_FAILURE;
                flowFile = session.penalize(flowFile);
            }
            session.transfer(flowFile, relationship);
        }
        return flowFile != null;
    }

    /**
     * Builds and instance of {@link KafkaPublisher}.
     */
    protected KafkaPublisher buildKafkaResource(ProcessContext context, ProcessSession session) {
        final Map<String, String> kafkaProps = new HashMap<>();
        KafkaProcessorUtils.buildCommonKafkaProperties(context, ProducerConfig.class, kafkaProps);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put("max.request.size", String.valueOf(context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue()));
        this.brokers = context.getProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        final Properties props = new Properties();
        props.putAll(kafkaProps);
        KafkaPublisher publisher = new KafkaPublisher(props, this.getLogger());
        return publisher;
    }

    /**
     * Will rendezvous with {@link KafkaPublisher} after building
     * {@link PublishingContext} and will produce the resulting
     * {@link FlowFile}. The resulting FlowFile contains all required
     * information to determine if message publishing originated from the
     * provided FlowFile has actually succeeded fully, partially or failed
     * completely (see {@link #isFailedFlowFile(FlowFile)}.
     */
    private FlowFile doRendezvousWithKafka(final FlowFile flowFile, final ProcessContext context, final ProcessSession session) {
        final AtomicReference<KafkaPublisher.KafkaPublisherResult> publishResultRef = new AtomicReference<>();
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream contentStream) throws IOException {
                PublishingContext publishingContext = PublishKafka.this.buildPublishingContext(flowFile, context, contentStream);
                KafkaPublisher.KafkaPublisherResult result = PublishKafka.this.kafkaPublisher.publish(publishingContext);
                publishResultRef.set(result);
            }
        });

        FlowFile resultFile = publishResultRef.get().isAllAcked()
                ? this.cleanUpFlowFileIfNecessary(flowFile, session)
                : session.putAllAttributes(flowFile, this.buildFailedFlowFileAttributes(publishResultRef.get().getLastMessageAcked(), flowFile, context));

        if (!this.isFailedFlowFile(resultFile)) {
            resultFile = session.putAttribute(resultFile, MSG_COUNT, String.valueOf(publishResultRef.get().getMessagesSent()));
        }
        return resultFile;
    }

    /**
     * Builds {@link PublishingContext} for message(s) to be sent to Kafka.
     * {@link PublishingContext} contains all contextual information required by
     * {@link KafkaPublisher} to publish to Kafka. Such information contains
     * things like topic name, content stream, delimiter, key and last ACKed
     * message for cases where provided FlowFile is being retried (failed in the
     * past).
     * <br>
     * For the clean FlowFile (file that has been sent for the first time),
     * PublishingContext will be built form {@link ProcessContext} associated
     * with this invocation.
     * <br>
     * For the failed FlowFile, {@link PublishingContext} will be built from
     * attributes of that FlowFile which by then will already contain required
     * information (e.g., topic, key, delimiter etc.). This is required to
     * ensure the affinity of the retry in the even where processor
     * configuration has changed. However keep in mind that failed FlowFile is
     * only considered a failed FlowFile if it is being re-processed by the same
     * processor (determined via {@link #FAILED_PROC_ID_ATTR}, see
     * {@link #isFailedFlowFile(FlowFile)}). If failed FlowFile is being sent to
     * another PublishKafka0_10 processor it is treated as a fresh FlowFile
     * regardless if it has #FAILED* attributes set.
     */
    private PublishingContext buildPublishingContext(FlowFile flowFile, ProcessContext context, InputStream contentStream) {
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
            String keyHex = flowFile.getAttribute(KafkaProcessorUtils.KAFKA_KEY_HEX);
            if (_key == null && keyHex != null && KafkaProcessorUtils.HEX_KEY_PATTERN.matcher(keyHex).matches()) {
                keyBytes = DatatypeConverter.parseHexBinary(keyHex);
            }
            delimiterBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;
        }

        PublishingContext publishingContext = new PublishingContext(contentStream, topicName, lastAckedMessageIndex,
                context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue());
        publishingContext.setKeyBytes(keyBytes);
        publishingContext.setDelimiterBytes(delimiterBytes);
        return publishingContext;
    }

    /**
     * Will remove FAILED_* attributes if FlowFile is no longer considered a
     * failed FlowFile
     *
     * @see #isFailedFlowFile(FlowFile)
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
     * Builds a {@link Map} of FAILED_* attributes
     *
     * @see #FAILED_PROC_ID_ATTR
     * @see #FAILED_LAST_ACK_IDX
     * @see #FAILED_TOPIC_ATTR
     * @see #FAILED_KEY_ATTR
     * @see #FAILED_DELIMITER_ATTR
     */
    private Map<String, String> buildFailedFlowFileAttributes(int lastAckedMessageIndex, FlowFile sourceFlowFile, ProcessContext context) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(FAILED_PROC_ID_ATTR, this.getIdentifier());
        attributes.put(FAILED_LAST_ACK_IDX, String.valueOf(lastAckedMessageIndex));
        attributes.put(FAILED_TOPIC_ATTR, context.getProperty(TOPIC).evaluateAttributeExpressions(sourceFlowFile).getValue());
        attributes.put(FAILED_KEY_ATTR, context.getProperty(KEY).evaluateAttributeExpressions(sourceFlowFile).getValue());
        attributes.put(FAILED_DELIMITER_ATTR, context.getProperty(MESSAGE_DEMARCATOR).isSet()
                ? context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions(sourceFlowFile).getValue() : null);
        return attributes;
    }

    /**
     * Returns 'true' if provided FlowFile is a failed FlowFile. A failed
     * FlowFile contains {@link #FAILED_PROC_ID_ATTR}.
     */
    private boolean isFailedFlowFile(FlowFile flowFile) {
        return this.getIdentifier().equals(flowFile.getAttribute(FAILED_PROC_ID_ATTR));
    }
}
