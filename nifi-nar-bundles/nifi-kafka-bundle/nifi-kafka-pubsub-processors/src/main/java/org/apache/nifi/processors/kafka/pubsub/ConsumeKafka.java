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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
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
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes messages from Apache Kafka")
@Tags({ "Kafka", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume" })
public class ConsumeKafka extends AbstractKafkaProcessor<Consumer<byte[], byte[]>> {

    static final AllowableValue OFFSET_EARLIEST = new AllowableValue("earliest", "earliest", "Automatically reset the offset to the earliest offset");

    static final AllowableValue OFFSET_LATEST = new AllowableValue("latest", "latest", "Automatically reset the offset to the latest offset");

    static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none", "Throw exception to the consumer if no previous offset is found for the consumer's group");

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
    static final PropertyDescriptor MESSAGE_DEMARCATOR = MESSAGE_DEMARCATOR_BUILDER
            .description("Since KafkaConsumer receives messages in batches, you have an option to output a single FlowFile which contains "
                    + "all Kafka messages in a single batch and this property allows you to provide a string (interpreted as UTF-8) to use "
                    + "for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received "
                    + "in a batch will result in a single FlowFile which essentially means that this processor may output multiple FlowFiles for each "
                    + "time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
            .build();


    static final List<PropertyDescriptor> DESCRIPTORS;

    static final Set<Relationship> RELATIONSHIPS;

    private volatile byte[] demarcatorBytes;

    private volatile String topic;

    private volatile String brokers;

    /*
     * Will ensure that the list of the PropertyDescriptors is build only once,
     * since all other lifecycle methods are invoked multiple times.
     */
    static {
        List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.addAll(SHARED_DESCRIPTORS);
        _descriptors.add(GROUP_ID);
        _descriptors.add(AUTO_OFFSET_RESET);
        _descriptors.add(MESSAGE_DEMARCATOR);
        DESCRIPTORS = Collections.unmodifiableList(_descriptors);

        RELATIONSHIPS = Collections.unmodifiableSet(SHARED_RELATIONSHIPS);
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Will unsubscribe form {@link KafkaConsumer} delegating to 'super' to do
     * the rest.
     */
    @Override
    @OnStopped
    public void close() {
        if (this.kafkaResource != null) {
            try {
                this.kafkaResource.unsubscribe();
            } finally { // in the event the above fails
                super.close();
            }
        }
    }

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     * Will rendezvous with Kafka by performing the following:
     * <br>
     * - poll {@link ConsumerRecords} from {@link KafkaConsumer} in a
     * non-blocking manner, signaling yield if no records were received from
     * Kafka
     * <br>
     * - if records were received form Kafka, the are written to a newly created
     * {@link FlowFile}'s {@link OutputStream} using a provided demarcator (see
     * {@link #MESSAGE_DEMARCATOR}
     */
    @Override
    protected boolean rendezvousWithKafka(ProcessContext context, ProcessSession processSession) {
        ConsumerRecords<byte[], byte[]> consumedRecords = this.kafkaResource.poll(100);
        if (consumedRecords != null && !consumedRecords.isEmpty()) {
            long start = System.nanoTime();
            FlowFile flowFile = processSession.create();
            final AtomicInteger messageCounter = new AtomicInteger();
            final Map<String, String> kafkaAttributes = new HashMap<>();

            final Iterator<ConsumerRecord<byte[], byte[]>> iter = consumedRecords.iterator();
            while (iter.hasNext()){
                flowFile = processSession.append(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        ConsumerRecord<byte[], byte[]> consumedRecord = iter.next();

                        kafkaAttributes.put("kafka.offset", String.valueOf(consumedRecord.offset()));
                        if (consumedRecord.key() != null) {
                            kafkaAttributes.put("kafka.key", new String(consumedRecord.key(), StandardCharsets.UTF_8));
                        }
                        kafkaAttributes.put("kafka.partition", String.valueOf(consumedRecord.partition()));
                        kafkaAttributes.put("kafka.topic", consumedRecord.topic());

                        if (messageCounter.getAndIncrement() > 0 && ConsumeKafka.this.demarcatorBytes != null) {
                            out.write(ConsumeKafka.this.demarcatorBytes);
                        }
                        out.write(consumedRecord.value());
                    }
                });

                flowFile = processSession.putAllAttributes(flowFile, kafkaAttributes);
                /*
                 * Release FlowFile if there are more messages in the
                 * ConsumerRecords batch and no demarcator was provided,
                 * otherwise the FlowFile will be released as soon as this loop
                 * exits.
                 */
                if (iter.hasNext() && ConsumeKafka.this.demarcatorBytes == null){
                    this.releaseFlowFile(flowFile, context, processSession, start, messageCounter.get());
                    flowFile = processSession.create();
                    messageCounter.set(0);
                }
            }
            this.releaseFlowFile(flowFile, context, processSession, start, messageCounter.get());
        }
        return consumedRecords != null && !consumedRecords.isEmpty();
    }

    /**
     * This operation is called from
     * {@link #onTrigger(ProcessContext, ProcessSessionFactory)} method after
     * the process session is committed so that then kafka offset changes can be
     * committed. This can mean in cases of really bad timing we could have data
     * duplication upon recovery but not data loss. We want to commit the flow
     * files in a NiFi sense before we commit them in a Kafka sense.
     */
    @Override
    protected void postCommit(ProcessContext context) {
        this.kafkaResource.commitSync();
    }

    /**
     * Builds and instance of {@link KafkaConsumer} and subscribes to a provided
     * topic.
     */
    @Override
    protected Consumer<byte[], byte[]> buildKafkaResource(ProcessContext context, ProcessSession session) {
        this.demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet()
                ? context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)
                : null;
        this.topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();
        this.brokers = context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        Properties kafkaProperties = this.buildKafkaProperties(context);

        /*
         * Since we are using unconventional way to validate if connectivity to
         * broker is possible we need a mechanism to be able to disable it.
         * 'check.connection' property will serve as such mechanism
         */
        if (!"false".equals(kafkaProperties.get("check.connection"))) {
            this.checkIfInitialConnectionPossible();
        }

        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singletonList(this.topic));
        return consumer;
    }

    /**
     * Checks via brute force if it is possible to establish connection to at
     * least one broker. If not this method will throw {@link ProcessException}.
     */
    private void checkIfInitialConnectionPossible(){
        String[] br = this.brokers.split(",");
        boolean connectionPossible = false;
        for (int i = 0; i < br.length && !connectionPossible; i++) {
            String hostPortCombo = br[i];
            String[] hostPort = hostPortCombo.split(":");
            Socket client = null;
            try {
                client = new Socket();
                client.connect(new InetSocketAddress(hostPort[0].trim(), Integer.parseInt(hostPort[1].trim())), 10000);
                connectionPossible = true;
            } catch (Exception e) {
                this.logger.error("Connection to '" + hostPortCombo + "' is not possible", e);
            } finally {
                try {
                    client.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        if (!connectionPossible){
            throw new ProcessException("Connection to " + this.brokers + " is not possible. See logs for more details");
        }
    }
    /**
     * Will release flow file. Releasing of the flow file in the context of this
     * operation implies the following:
     *
     * If Empty then remove from session and return If has something then
     * transfer to {@link #REL_SUCCESS}
     */
    private void releaseFlowFile(FlowFile flowFile, ProcessContext context, ProcessSession session, long start, int msgCount) {
        long executionDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        String transitUri = this.buildTransitURI(context.getProperty(SECURITY_PROTOCOL).getValue(), this.brokers, topic);
        session.getProvenanceReporter().receive(flowFile, transitUri, "Received " + msgCount + " Kafka messages", executionDuration);
        this.getLogger().info("Successfully received {} from Kafka with {} messages in {} millis", new Object[] { flowFile, msgCount, executionDuration });
        session.transfer(flowFile, REL_SUCCESS);
    }
}