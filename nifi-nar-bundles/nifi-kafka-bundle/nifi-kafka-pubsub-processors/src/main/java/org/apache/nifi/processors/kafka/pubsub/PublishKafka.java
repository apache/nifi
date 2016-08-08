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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.kafka.pubsub.KafkaPublisher.KafkaPublisherResult;
import org.apache.nifi.processors.kafka.pubsub.Partitioners.RoundRobinPartitioner;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "Apache", "Kafka", "Put", "Send", "Message", "PubSub" })
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka. The messages to send may be individual FlowFiles or may be delimited, using a "
        + "user-specified delimiter, such as a new-line.")
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
                 description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration.")
public class PublishKafka extends AbstractKafkaProcessor<KafkaPublisher> {

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

    static final AllowableValue ROUND_ROBIN_PARTITIONING = new AllowableValue(RoundRobinPartitioner.class.getName(),
            RoundRobinPartitioner.class.getSimpleName(),
            "Messages will be assigned partitions in a round-robin fashion, sending the first message to Partition 1, "
                    + "the next Partition to Partition 2, and so on, wrapping as necessary.");
    static final AllowableValue RANDOM_PARTITIONING = new AllowableValue("org.apache.kafka.clients.producer.internals.DefaultPartitioner",
            "DefaultPartitioner", "Messages will be assigned to random partitions.");

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
    static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("kafka-key")
            .displayName("Kafka Key")
            .description("The Key to use for the Message")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor MESSAGE_DEMARCATOR = MESSAGE_DEMARCATOR_BUILDER
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating apart multiple messages within "
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

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
            .build();

    static final List<PropertyDescriptor> DESCRIPTORS;

    static final Set<Relationship> RELATIONSHIPS;

    private volatile String brokers;

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    static {
        List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.addAll(SHARED_DESCRIPTORS);
        _descriptors.add(DELIVERY_GUARANTEE);
        _descriptors.add(KEY);
        _descriptors.add(MESSAGE_DEMARCATOR);
        _descriptors.add(META_WAIT_TIME);
        _descriptors.add(PARTITION_CLASS);
        _descriptors.add(COMPRESSION_CODEC);

        DESCRIPTORS = Collections.unmodifiableList(_descriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.addAll(SHARED_RELATIONSHIPS);
        _relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(_relationships);
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     * Will rendezvous with Kafka if {@link ProcessSession} contains {@link FlowFile}
     * producing a result {@link FlowFile}.
     * <br>
     * The result {@link FlowFile} that is successful is then transfered to {@link #REL_SUCCESS}
     * <br>
     * The result {@link FlowFile} that is failed is then transfered to {@link #REL_FAILURE}
     *
     */
    @Override
    protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession session){
        FlowFile flowFile = session.get();
        if (flowFile != null) {
            long start = System.nanoTime();
            flowFile = this.doRendezvousWithKafka(flowFile, context, session);
            Relationship relationship = REL_SUCCESS;
            if (!this.isFailedFlowFile(flowFile)) {
                String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
                long executionDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                String transitUri = this.buildTransitURI(context.getProperty(SECURITY_PROTOCOL).getValue(), this.brokers, topic);
                session.getProvenanceReporter().send(flowFile, transitUri, "Sent " + flowFile.getAttribute(MSG_COUNT) + " Kafka messages", executionDuration);
                this.getLogger().info("Successfully sent {} to Kafka as {} message(s) in {} millis", new Object[] { flowFile, flowFile.getAttribute(MSG_COUNT), executionDuration });
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
    @Override
    protected KafkaPublisher buildKafkaResource(ProcessContext context, ProcessSession session) {
        Properties kafkaProperties = this.buildKafkaProperties(context);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.brokers = context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        KafkaPublisher publisher = new KafkaPublisher(kafkaProperties, this.getLogger());
        return publisher;
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
                PublishingContext publishingContext = PublishKafka.this.buildPublishingContext(flowFile, context, contentStream);
                KafkaPublisherResult result = PublishKafka.this.kafkaResource.publish(publishingContext);
                publishResultRef.set(result);
            }
        });

        FlowFile resultFile = publishResultRef.get().isAllAcked()
                ? this.cleanUpFlowFileIfNecessary(flowFile, session)
                : session.putAllAttributes(flowFile, this.buildFailedFlowFileAttributes(publishResultRef.get().getLastMessageAcked(), flowFile, context));

        if (!this.isFailedFlowFile(resultFile)) {
            resultFile = session.putAttribute(resultFile, MSG_COUNT,  String.valueOf(publishResultRef.get().getMessagesSent()));
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
     * another PublishKafka processor it is treated as a fresh FlowFile
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
            delimiterBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8) : null;
        }

        PublishingContext publishingContext = new PublishingContext(contentStream, topicName, lastAckedMessageIndex);
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
