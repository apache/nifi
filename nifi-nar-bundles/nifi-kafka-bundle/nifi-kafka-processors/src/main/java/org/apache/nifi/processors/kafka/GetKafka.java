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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
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
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Fetches messages from Apache Kafka")
@Tags({"Kafka", "Apache", "Get", "Ingest", "Ingress", "Topic", "PubSub"})
@WritesAttributes({
    @WritesAttribute(attribute = "kafka.topic", description = "The name of the Kafka Topic from which the message was received"),
    @WritesAttribute(attribute = "kafka.key", description = "The key of the Kafka message, if it exists and batch size is 1. If"
            + " the message does not have a key, or if the batch size is greater than 1, this attribute will not be added"),
    @WritesAttribute(attribute = "kafka.partition", description = "The partition of the Kafka Topic from which the message was received. This attribute is added only if the batch size is 1"),
    @WritesAttribute(attribute = "kafka.offset", description = "The offset of the message within the Kafka partition. This attribute is added only if the batch size is 1")})
public class GetKafka extends AbstractProcessor {

    public static final PropertyDescriptor ZOOKEEPER_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("ZooKeeper Connection String")
            .description("The Connection String to use in order to connect to ZooKeeper. This is often a comma-separated list of <host>:<port>"
                    + " combinations. For example, host1:2181,host2:2181,host3:2188")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("The Kafka Topic to pull messages from")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
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
        .expressionLanguageSupported(false)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();

    private final BlockingQueue<ConsumerIterator<byte[], byte[]>> streamIterators = new LinkedBlockingQueue<>();
    private volatile ConsumerConnector consumer;

    final Lock interruptionLock = new ReentrantLock();
    // guarded by interruptionLock
    private final Set<Thread> interruptableThreads = new HashSet<>();

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
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @OnScheduled
    public void createConsumers(final ProcessContext context) {
        final String topic = context.getProperty(TOPIC).getValue();

        final Map<String, Integer> topicCountMap = new HashMap<>(1);
        topicCountMap.put(topic, context.getMaxConcurrentTasks());

        final Properties props = new Properties();
        props.setProperty("zookeeper.connect", context.getProperty(ZOOKEEPER_CONNECTION_STRING).getValue());
        props.setProperty("group.id", context.getProperty(GROUP_ID).getValue());
        props.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());
        props.setProperty("auto.commit.interval.ms", String.valueOf(context.getProperty(ZOOKEEPER_COMMIT_DELAY).asTimePeriod(TimeUnit.MILLISECONDS)));
        props.setProperty("auto.commit.enable", "true"); // just be explicit
        props.setProperty("auto.offset.reset", "smallest");
        props.setProperty("zk.connectiontimeout.ms", context.getProperty(ZOOKEEPER_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).toString());
        props.setProperty("socket.timeout.ms", context.getProperty(KAFKA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).toString());

        final ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        this.streamIterators.clear();

        for (final KafkaStream<byte[], byte[]> stream : streams) {
            streamIterators.add(stream.iterator());
        }
    }

    @OnStopped
    public void shutdownConsumer() {
        if (consumer != null) {
            try {
                consumer.commitOffsets();
            } finally {
                consumer.shutdown();
            }
        }
    }

    @OnUnscheduled
    public void interruptIterators() {
        // Kafka doesn't provide a non-blocking API for pulling messages. We can, however,
        // interrupt the Threads. We do this when the Processor is stopped so that we have the
        // ability to shutdown the Processor.
        interruptionLock.lock();
        try {
            for (final Thread t : interruptableThreads) {
                t.interrupt();
            }

            interruptableThreads.clear();
        } finally {
            interruptionLock.unlock();
        }
    }

    protected ConsumerIterator<byte[], byte[]> getStreamIterator() {
        return streamIterators.poll();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ConsumerIterator<byte[], byte[]> iterator = getStreamIterator();
        if (iterator == null) {
            return;
        }

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final String demarcator = context.getProperty(MESSAGE_DEMARCATOR).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        final byte[] demarcatorBytes = demarcator.getBytes(StandardCharsets.UTF_8);
        final String topic = context.getProperty(TOPIC).getValue();

        FlowFile flowFile = null;
        try {
            // add the current thread to the Set of those to be interrupted if processor stopped.
            interruptionLock.lock();
            try {
                interruptableThreads.add(Thread.currentThread());
            } finally {
                interruptionLock.unlock();
            }

            final long start = System.nanoTime();
            flowFile = session.create();

            final Map<String, String> attributes = new HashMap<>();
            attributes.put("kafka.topic", topic);

            int numMessages = 0;
            for (int msgCount = 0; msgCount < batchSize; msgCount++) {
                // if the processor is stopped, iterator.hasNext() will throw an Exception.
                // In this case, we just break out of the loop.
                try {
                    if (!iterator.hasNext()) {
                        break;
                    }
                } catch (final Exception e) {
                    break;
                }

                final MessageAndMetadata<byte[], byte[]> mam = iterator.next();
                if (mam == null) {
                    return;
                }

                final byte[] key = mam.key();

                if (batchSize == 1) {
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
                numMessages++;
            }

            // If we received no messages, remove the FlowFile. Otherwise, send to success.
            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
            } else {
                flowFile = session.putAllAttributes(flowFile, attributes);
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                session.getProvenanceReporter().receive(flowFile, "kafka://" + topic, "Received " + numMessages + " Kafka messages", millis);
                getLogger().info("Successfully received {} from Kafka with {} messages in {} millis", new Object[]{flowFile, numMessages, millis});
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to receive FlowFile from Kafka due to {}", new Object[]{e});
            if (flowFile != null) {
                session.remove(flowFile);
            }
        } finally {
            // Remove the current thread from the Set of Threads to interrupt.
            interruptionLock.lock();
            try {
                interruptableThreads.remove(Thread.currentThread());
            } finally {
                interruptionLock.unlock();
            }

            // Add the iterator back to the queue
            if (iterator != null) {
                streamIterators.offer(iterator);
            }
        }
    }

}
