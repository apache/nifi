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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes messages from Apache Kafka")
@Tags({ "Kafka", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume" })
public class ConsumeKafka extends AbstractKafkaProcessor<KafkaConsumer<byte[], byte[]>> {

    static final AllowableValue OFFSET_EARLIEST = new AllowableValue("earliest", "earliest", "Automatically reset the offset to the earliest offset");

    static final AllowableValue OFFSET_LATEST = new AllowableValue("latest", "latest", "Automatically reset the offset to the latest offset");

    static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none", "Throw exception to the consumer if no previous offset is found for the consumer's group");

    static final PropertyDescriptor TOPIC = TOPIC_BUILDER
            .expressionLanguageSupported(false)
            .build();
    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.GROUP_ID_CONFIG)
            .displayName("Group ID")
            .description("A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
            .displayName("Offset Reset")
            .description("Allows you to manage teh condition when there is no initial offset in Kafka or if the current offset does not exist any "
                    + "more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.")
            .required(true)
            .allowableValues(OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_NONE)
            .defaultValue(OFFSET_LATEST.getValue())
            .build();
    static final PropertyDescriptor MESSAGE_DEMARCATOR = MESSAGE_DEMARCATOR_BUILDER
            .description("Since KafkaConsumer receives messages in batches, this property allows you to provide a string (interpreted as UTF-8) to use "
                    + "for demarcating apart multiple Kafka messages when building a single FlowFile. If not specified, all messages received form Kafka "
                            + "will be merged into a single content within a FlowFile. By default it will use 'new line' charcater to demarcate individual messages."
                            + "To enter special character such as 'new line' use Shift+Enter.")
            .defaultValue("\n")
            .build();


    static final List<PropertyDescriptor> descriptors;

    static final Set<Relationship> relationships;

    private volatile byte[] demarcatorBytes;

    private volatile String topic;

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    static {
        List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.add(TOPIC);
        _descriptors.addAll(sharedDescriptors);
        _descriptors.add(GROUP_ID);
        _descriptors.add(AUTO_OFFSET_RESET);
        _descriptors.add(MESSAGE_DEMARCATOR);
        descriptors = Collections.unmodifiableList(_descriptors);

        relationships = Collections.unmodifiableSet(sharedRelationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnStopped
    public void shutdownConsumer() {
        if (this.kafkaResource != null) {
            try {
                this.kafkaResource.unsubscribe();
            } finally { // in the event the above fails
                this.kafkaResource.unsubscribe();
            }
            this.kafkaResource.close();
        }
        this.kafkaResource = null;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }


    @Override
    protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession processSession) throws ProcessException {
        ConsumerRecords<byte[], byte[]> consumedRecords = this.kafkaResource.poll(100);
        if (consumedRecords != null && !consumedRecords.isEmpty()) {
            long start = System.nanoTime();
            FlowFile flowFile = processSession.create();
            final AtomicInteger messageCounter = new AtomicInteger();

            for (final ConsumerRecord<byte[], byte[]> consumedRecord : consumedRecords) {
                flowFile = processSession.append(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        if (messageCounter.getAndIncrement() > 0) {
                            out.write(ConsumeKafka.this.demarcatorBytes);
                        }
                        out.write(consumedRecord.value());
                    }
                });
            }

            this.releaseFlowFile(flowFile, processSession, flowFile.getAttributes(), start, this.topic, messageCounter.get() - 1);
            this.kafkaResource.commitSync();
        } else {
            context.yield();
        }
        return true;
    }

    @Override
    protected KafkaConsumer<byte[], byte[]> buildKafkaResource(ProcessContext context, ProcessSession session)
            throws ProcessException {
        this.demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).getValue().getBytes(StandardCharsets.UTF_8);
        this.topic = context.getProperty(TOPIC).getValue();

        Properties kafkaProperties = this.buildKafkaProperties(context);
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singletonList(this.topic));
        return consumer;
    }

    /**
     * Will release flow file. Releasing of the flow file in the context of this
     * operation implies the following:
     *
     * If Empty then remove from session and return If has something then
     * transfer to REL_SUCCESS
     */
    private void releaseFlowFile(FlowFile flowFile, ProcessSession session, Map<String, String> attributes, long start,
            String topic, int msgCount) {
        flowFile = session.putAllAttributes(flowFile, attributes);
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        session.getProvenanceReporter().receive(flowFile, "kafka://" + topic, "Received " + msgCount + " Kafka messages", millis);
        this.getLogger().info("Successfully received {} from Kafka with {} messages in {} millis", new Object[] { flowFile, msgCount, millis });
        session.transfer(flowFile, REL_SUCCESS);
    }
}