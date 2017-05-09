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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Fetches messages from Apache Kafka, specifically for 0.8.x versions. The complementary NiFi processor for sending messages is PutKafka.")
@Tags({"Kafka", "Apache", "Get", "Ingest", "Ingress", "Topic", "PubSub", "0.8.x"})
@WritesAttributes({
        @WritesAttribute(attribute = "kafka.topic", description = "The name of the Kafka Topic from which the message was received"),
        @WritesAttribute(attribute = "kafka.key", description = "The key of the Kafka message, if it exists and batch size is 1. If"
                + " the message does not have a key, or if the batch size is greater than 1, this attribute will not be added"),
        @WritesAttribute(attribute = "kafka.partition", description = "The partition of the Kafka Topic from which the message was received. This attribute is added only if the batch size is 1"),
        @WritesAttribute(attribute = "kafka.offset", description = "The offset of the message within the Kafka partition. This attribute is added only if the batch size is 1")})
@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
            description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
        + " In the event a dynamic property represents a property that was already set as part of the static properties, its value wil be"
        + " overridden with warning message describing the override."
        + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration.")
public class GetKafka extends AbstractProcessor {

    public static final String SMALLEST = "smallest";
    public static final String LARGEST = "largest";

    public static final PropertyDescriptor ZOOKEEPER_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("ZooKeeper Connection String")
            .description("The Connection String to use in order to connect to ZooKeeper. This is often a comma-separated list of <host>:<port>"
                    + " combinations. For example, host1:2181,host2:2181,host3:2188")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The Kafka Topic to pull messages from")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor ZOOKEEPER_COMMIT_DELAY = new PropertyDescriptor.Builder()
            .name("Zookeeper Commit Frequency")
            .description("Specifies how often to communicate with ZooKeeper to indicate which messages have been pulled. A longer time period will"
                    + " result in better overall performance but can result in more data duplication if a NiFi node is lost")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("60 secs")
            .build();
    public static final PropertyDescriptor ZOOKEEPER_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ZooKeeper Communications Timeout")
            .description("The amount of time to wait for a response from ZooKeeper before determining that there is a communications error")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("30 secs")
            .build();
    public static final PropertyDescriptor KAFKA_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Kafka Communications Timeout")
            .description("The amount of time to wait for a response from Kafka before determining that there is a communications error")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("30 secs")
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Specifies the maximum number of messages to combine into a single FlowFile. These messages will be "
                    + "concatenated together with the <Message Demarcator> string placed between the content of each message. "
                    + "If the messages from Kafka should not be concatenated together, leave this value at 1.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1")
            .build();
    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .description("Specifies the characters to use in order to demarcate multiple messages from Kafka. If the <Batch Size> "
                    + "property is set to 1, this value is ignored. Otherwise, for each two subsequent messages in the batch, "
                    + "this value will be placed in between them.")
            .required(true)
            .addValidator(Validator.VALID) // accept anything as a demarcator, including empty string
            .expressionLanguageSupported(false)
            .defaultValue("\\n")
            .build();

    public static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
            .name("Client Name")
            .description("Client Name to use when communicating with Kafka")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("Group ID")
            .description("A Group ID is used to identify consumers that are within the same consumer group")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name("Auto Offset Reset")
            .description("Automatically reset the offset to the smallest or largest offset available on the broker")
            .required(true)
            .allowableValues(SMALLEST, LARGEST)
            .defaultValue(LARGEST)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();

    private final BlockingQueue<ConsumerIterator<byte[], byte[]>> streamIterators = new LinkedBlockingQueue<>();
    private volatile ConsumerConnector consumer;

    private final AtomicBoolean consumerStreamsReady = new AtomicBoolean();

    private volatile long deadlockTimeout;

    private volatile ExecutorService executor;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor clientNameWithDefault = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(CLIENT_NAME)
                .defaultValue("NiFi-" + getIdentifier())
                .build();
        final PropertyDescriptor groupIdWithDefault = new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(GROUP_ID)
                .defaultValue(getIdentifier())
                .build();

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_CONNECTION_STRING);
        props.add(TOPIC);
        props.add(ZOOKEEPER_COMMIT_DELAY);
        props.add(BATCH_SIZE);
        props.add(MESSAGE_DEMARCATOR);
        props.add(clientNameWithDefault);
        props.add(groupIdWithDefault);
        props.add(KAFKA_TIMEOUT);
        props.add(ZOOKEEPER_TIMEOUT);
        props.add(AUTO_OFFSET_RESET);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    public void createConsumers(final ProcessContext context) {
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();

        final Properties props = new Properties();
        props.setProperty("zookeeper.connect", context.getProperty(ZOOKEEPER_CONNECTION_STRING).evaluateAttributeExpressions().getValue());
        props.setProperty("group.id", context.getProperty(GROUP_ID).evaluateAttributeExpressions().getValue());
        props.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());
        props.setProperty("auto.commit.interval.ms", String.valueOf(context.getProperty(ZOOKEEPER_COMMIT_DELAY).asTimePeriod(TimeUnit.MILLISECONDS)));
        props.setProperty("auto.offset.reset", context.getProperty(AUTO_OFFSET_RESET).getValue());
        props.setProperty("zookeeper.connection.timeout.ms", context.getProperty(ZOOKEEPER_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).toString());
        props.setProperty("socket.timeout.ms", context.getProperty(KAFKA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).toString());

        for (final Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                if (props.containsKey(descriptor.getName())) {
                    this.getLogger().warn("Overriding existing property '" + descriptor.getName() + "' which had value of '"
                        + props.getProperty(descriptor.getName()) + "' with dynamically set value '" + entry.getValue() + "'.");
                }
                props.setProperty(descriptor.getName(), entry.getValue());
            }
        }

        /*
         * Unless user sets it to some explicit value we are setting it to the
         * lowest possible value of 1 millisecond to ensure the
         * consumerStream.hasNext() doesn't block. See
         * http://kafka.apache.org/documentation.html#configuration) as well as
         * comment in 'catch ConsumerTimeoutException' in onTrigger() for more
         * explanation as to the reasoning behind it.
         */
        if (!props.containsKey("consumer.timeout.ms")) {
            this.getLogger().info("Setting 'consumer.timeout.ms' to 1 milliseconds to avoid consumer"
                            + " block in the event when no events are present in Kafka topic. If you wish to change this value "
                            + " set it as dynamic property. If you wish to explicitly enable consumer block (at your own risk)"
                            + " set its value to -1.");
            props.setProperty("consumer.timeout.ms", "1");
        }

        int partitionCount = KafkaUtils.retrievePartitionCountForTopic(
                context.getProperty(ZOOKEEPER_CONNECTION_STRING).evaluateAttributeExpressions().getValue(), context.getProperty(TOPIC).evaluateAttributeExpressions().getValue());

        final ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        final Map<String, Integer> topicCountMap = new HashMap<>(1);

        int concurrentTaskToUse = context.getMaxConcurrentTasks();
        if (context.getMaxConcurrentTasks() < partitionCount){
            this.getLogger().warn("The amount of concurrent tasks '" + context.getMaxConcurrentTasks() + "' configured for "
                    + "this processor is less than the amount of partitions '" + partitionCount + "' for topic '" + context.getProperty(TOPIC).evaluateAttributeExpressions().getValue() + "'. "
                + "Consider making it equal to the amount of partition count for most efficient event consumption.");
        } else if (context.getMaxConcurrentTasks() > partitionCount){
            concurrentTaskToUse = partitionCount;
            this.getLogger().warn("The amount of concurrent tasks '" + context.getMaxConcurrentTasks() + "' configured for "
                    + "this processor is greater than the amount of partitions '" + partitionCount + "' for topic '" + context.getProperty(TOPIC).evaluateAttributeExpressions().getValue() + "'. "
                + "Therefore those tasks would never see a message. To avoid that the '" + partitionCount + "'(partition count) will be used to consume events");
        }

        topicCountMap.put(topic, concurrentTaskToUse);

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        this.streamIterators.clear();

        for (final KafkaStream<byte[], byte[]> stream : streams) {
            streamIterators.add(stream.iterator());
        }
        this.consumerStreamsReady.set(true);
    }

    @OnStopped
    public void shutdownConsumer() {
        this.consumerStreamsReady.set(false);
        if (consumer != null) {
            try {
                consumer.commitOffsets();
            } finally {
                consumer.shutdown();
            }
        }
        if (this.executor != null) {
            this.executor.shutdown();
            try {
                if (!this.executor.awaitTermination(30000, TimeUnit.MILLISECONDS)) {
                    this.executor.shutdownNow();
                    getLogger().warn("Executor did not stop in 30 sec. Terminated.");
                }
                this.executor = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    @OnScheduled
    public void schedule(ProcessContext context) {
        this.deadlockTimeout = context.getProperty(KAFKA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS) * 2;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        /*
         * Will ensure that consumer streams are ready upon the first invocation
         * of onTrigger. Will be reset to 'false' in the event of exception
         */
        synchronized (this.consumerStreamsReady) {
            if (this.executor == null || this.executor.isShutdown()) {
                this.executor = Executors.newCachedThreadPool();
            }
            if (!this.consumerStreamsReady.get()) {
                Future<Void> f = this.executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        createConsumers(context);
                        return null;
                    }
                });
                try {
                    f.get(this.deadlockTimeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    shutdownConsumer();
                    f.cancel(true);
                    Thread.currentThread().interrupt();
                    getLogger().warn("Interrupted while waiting to get connection", e);
                } catch (ExecutionException e) {
                    throw new IllegalStateException(e);
                } catch (TimeoutException e) {
                    shutdownConsumer();
                    f.cancel(true);
                    getLogger().warn("Timed out after " + this.deadlockTimeout + " milliseconds while waiting to get connection", e);
                }
            }
        }
        //===
        if (this.consumerStreamsReady.get()) {
            Future<Void> consumptionFuture = this.executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    ConsumerIterator<byte[], byte[]> iterator = getStreamIterator();
                    if (iterator != null) {
                        consumeFromKafka(context, session, iterator);
                    }
                    return null;
                }
            });
            try {
                consumptionFuture.get(this.deadlockTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                shutdownConsumer();
                consumptionFuture.cancel(true);
                Thread.currentThread().interrupt();
                getLogger().warn("Interrupted while consuming messages", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException(e);
            } catch (TimeoutException e) {
                shutdownConsumer();
                consumptionFuture.cancel(true);
                getLogger().warn("Timed out after " + this.deadlockTimeout + " milliseconds while consuming messages", e);
            }
        }
    }

    protected ConsumerIterator<byte[], byte[]> getStreamIterator() {
        return this.streamIterators.poll();
    }


    private void consumeFromKafka(final ProcessContext context, final ProcessSession session,
            ConsumerIterator<byte[], byte[]> iterator) throws ProcessException {

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final String demarcator = context.getProperty(MESSAGE_DEMARCATOR).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        final byte[] demarcatorBytes = demarcator.getBytes(StandardCharsets.UTF_8);
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();

        FlowFile flowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kafka.topic", topic);
        final long start = System.nanoTime();
        int msgCount = 0;

        try {
            for (; msgCount < batchSize && iterator.hasNext(); msgCount++) {
                final MessageAndMetadata<byte[], byte[]> mam = iterator.next();

                if (batchSize == 1) {
                    final byte[] key = mam.key();
                    // the kafka.key, kafka.offset, and kafka.partition attributes are added only
                    // for a batch size of 1.
                    if (key != null) {
                        attributes.put("kafka.key", new String(key, StandardCharsets.UTF_8));
                    }

                    attributes.put("kafka.offset", String.valueOf(mam.offset()));
                    attributes.put("kafka.partition", String.valueOf(mam.partition()));
                }

                // add the message to the FlowFile's contents
                final boolean firstMessage = msgCount == 0;
                flowFile = session.append(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        if (!firstMessage) {
                            out.write(demarcatorBytes);
                        }
                        out.write(mam.message());
                    }
                });
            }
            this.releaseFlowFile(flowFile, session, attributes, start, topic, msgCount);
        } catch (ConsumerTimeoutException e) {
            /*
             * By default Kafka blocks indefinitely if topic is empty via
             * stream.hasNext(). If 'consumer.timeout.ms' property is set (see
             * http://kafka.apache.org/documentation.html#configuration) the
             * hasNext() will fail with this exception. To this processor it
             * simply means there are no messages and current task should exit
             * in non-failure releasing the flow file if it was able to
             * accumulate any events.
             */
            this.releaseFlowFile(flowFile, session, attributes, start, topic, msgCount);
        } catch (final Exception e) {
            this.shutdownConsumer();
            getLogger().error("Failed to receive FlowFile from Kafka due to {}", new Object[]{e});
            if (flowFile != null) {
                session.remove(flowFile);
            }
        } finally {
            // Add the iterator back to the queue
            if (iterator != null) {
                streamIterators.offer(iterator);
            }
        }
    }

    /**
     * Will release flow file. Releasing of the flow file in the context of this
     * operation implies the following:
     *
     * If Empty then remove from session and return
     * If has something then transfer to REL_SUCCESS
     */
    private void releaseFlowFile(FlowFile flowFile, ProcessSession session, Map<String, String> attributes, long start, String topic, int msgCount){
        if (flowFile.getSize() == 0L) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributes);
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            session.getProvenanceReporter().receive(flowFile, "kafka://" + topic, "Received " + msgCount + " Kafka messages", millis);
            getLogger().info("Successfully received {} from Kafka with {} messages in {} millis", new Object[]{flowFile, msgCount, millis});
            session.transfer(flowFile, REL_SUCCESS);
        }
    }
}
