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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.ExternalStateManager;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StandardStateMap;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes messages from Apache Kafka")
@Tags({ "Kafka", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume" })
@Stateful(scopes = {Scope.EXTERNAL}, description = "After consuming messages, ConsumeKafka commits its offset information to Kafka" +
        " so that the state of a consumer group can be retained across events such as consumer reconnect." +
        " Offsets can be cleared when there is no consumer subscribing with the same consumer group id." +
        " It may take more than 30 seconds for a consumer group to become able to be cleared after it is stopped from NiFi." +
        " Once offsets are cleared, ConsumeKafka will resume consuming messages based on Offset Reset configuration.")
public class ConsumeKafka extends AbstractKafkaProcessor<Consumer<byte[], byte[]>> implements ExternalStateManager {

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


    private static final int CONSUMER_GRP_CMD_TIMEOUT_SEC = 10;

    static final List<PropertyDescriptor> DESCRIPTORS;

    static final Set<Relationship> RELATIONSHIPS;

    private volatile byte[] demarcatorBytes;

    private volatile String topic;

    private volatile String brokers = DEFAULT_BOOTSTRAP_SERVERS;

    private volatile Properties kafkaProperties;

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
     * Builds an instance of {@link KafkaConsumer} and subscribes to a provided
     * topic.
     */
    @Override
    protected Consumer<byte[], byte[]> buildKafkaResource(ProcessContext context, ProcessSession session) {
        this.demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet()
                ? context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8)
                : null;

        kafkaProperties = this.buildKafkaProperties(context);

        final Consumer<byte[], byte[]> consumer = buildKafkaResource();
        consumer.subscribe(Collections.singletonList(this.topic));

        return consumer;
    }

    private Consumer<byte[], byte[]> buildKafkaResource() {
        return buildKafkaResource(null);
    }

    /**
     * Builds an instance of {@link KafkaConsumer}, but does not subscribe to a topic yet.
     * @param clientIdSuffix This method creates new KafkaConsumer instance.
     *                         If there's another KafkaConsumer instance is already connected,
     *                         {@link javax.management.InstanceAlreadyExistsException} is thrown.
     *                         Since external state manager methods are called from different thread than onTrigger(),
     *                         it's possible multiple KafkaConsumer instances to connect Kafka at the same time.
     *                         To avoid InstanceAlreadyExistsException, if clientIdSuffix is specified,
     *                         this method updates clientId by appending clientIdSuffix to the original clientId.
     */
    Consumer<byte[], byte[]> buildKafkaResource(final String clientIdSuffix) {
        if (kafkaProperties == null) {
            return null;
        }

        Properties props = kafkaProperties;
        if (!StringUtils.isEmpty(clientIdSuffix)) {
            // Update client.id while keep other properties as it is.
            props = new Properties();
            props.putAll(kafkaProperties);
            final String clientId = kafkaProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) + clientIdSuffix;
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        return consumer;
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

    @Override
    public ExternalStateScope getExternalStateScope() {
        return ExternalStateScope.CLUSTER;
    }

    @Override
    public StateMap getExternalState() throws IOException {

        if (!isReadyToAccessState()) {
            return null;
        }

        final String groupId = kafkaProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        return submitConsumerGroupCommand("Fetch offsets", consumer -> {
            final Map<String, String> partitionOffsets = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .map(tp -> new ImmutablePair<>(tp, consumer.committed(tp)))
                    .filter(tpo -> tpo.right != null)
                    .collect(Collectors.toMap(tpo ->
                                    "partition:" + tpo.left.partition(),
                            tpo -> String.valueOf(tpo.right.offset())));

            logger.info("Retrieved offsets from Kafka, topic={}, groupId={}, partitionOffsets={}",
                    topic, groupId, partitionOffsets);

            return new StandardStateMap(partitionOffsets, System.currentTimeMillis());
        }, null);
    }

    private boolean isReadyToAccessState() {
        return !StringUtils.isEmpty(topic)
                && !StringUtils.isEmpty(brokers)
                && kafkaProperties != null
                && !StringUtils.isEmpty(kafkaProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    /**
     * <p>Clear offsets stored in Kafka, by committing -1 as offset of each partitions of specified topic.</p>
     *
     * <p>Kafka allows commitSync if one of following conditions are met,
     * see kafka.coordinator.GroupCoordinator.handleCommitOffsets for detail:
     * <ol>
     * <li>The consumer is a member of the consumer group. In this case,
     * even if there's other consumers connecting Kafka, offsets can be updated.
     * It's dangerous to clear offsets if there're active consumers.
     * When consumer.subscribe() and poll() are called, the consumer will be a member of the consumer group.</li>
     *
     * <li>There's no connected consumer within the group,
     * and Kafka GroupCoordinator has marked the group as dead.
     * It's safer but can take longer.</li>
     * </ol>
     *
     * <p>The consumer group state transition is an async operation at Kafka group coordinator.
     * Although clearExternalState() can only be called when the processor is stopped,
     * the consumer group may not be fully removed at Kafka, in that case, CommitFailedException will be thrown.</p>
     *
     * <p>Following log msg can be found when GroupCoordinator has marked the group as dead
     * in kafka.out on a Kafka broker server, it can take more than 30 seconds:
     * <blockquote>[GroupCoordinator]: Group [gid] generation 1 is dead
     * and removed (kafka.coordinator.GroupCoordinator)</blockquote></p>
     *
     */
    @Override
    public void clearExternalState() throws IOException {

        if (!isReadyToAccessState()) {
            return;
        }

        synchronized (this) {
            final String groupId = kafkaProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
            final Boolean result = submitConsumerGroupCommand("Clear offsets", consumer -> {

                final Map<TopicPartition, OffsetAndMetadata> freshOffsets = consumer.partitionsFor(topic).stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(-1)));

                consumer.commitSync(freshOffsets);
                return true;

            }, e -> {
                if (e instanceof CommitFailedException) {
                    throw new IOException("The stopped consumer may not have been removed completely." +
                            " It can take more than 30 seconds." +
                            " or there are other consumers connected to the same consumer group. Retrying later may succeed.", e);
                }
            });

            if (result) {
                logger.info("Offset is successfully cleared from Kafka. topic={}, groupId={}", topic, groupId);
            }
        }
    }

    private interface ConsumerGroupCommand<T> {
        T execute(final Consumer<byte[], byte[]> consumer) throws Exception;
    }

    private interface ConsumerGroupCommandExceptionHandler {
        void handle(Exception e) throws IOException;
    }

    /**
     * Submit a consumer group related Kafka command.
     * External state fetch operations can be executed at the same time as onTrigger() is running in a different thread.
     * However, Kafka Consumer instance can not be shared among threads, so, different  Consumer instance is created
     * and used in this method, so that uses can access state while this processor consuming messages from Kafka.
     * Kafka's {@link Utils#getContextOrKafkaClassLoader()}uses current thread's context classloader to load Kafka classes
     * such as ByteArrayDeserializer, but since external state operations are called through NiFi Web API,
     * those threads has Jetty class loader as its context class loader which does not know Kafka jar.
     * As a workaround, this method uses another thread with context classloader unset to load Kafka classes properly.
     *
     * @param commandName command name is used for log message to show what command outputs what log messages
     * @param command a command to submit and execute
     * @param exHander optional exception handler
     * @param <T> command's return type
     * @return command's return value
     */
    private <T> T submitConsumerGroupCommand(final String commandName, final ConsumerGroupCommand<T> command,
                                             final ConsumerGroupCommandExceptionHandler exHander) throws IOException {
        // Use different KafkaConsumer instance because it can only be used
        // from the thread which created the instance.
        final ExecutorService executorService = Executors.newFixedThreadPool(1);
        final Future<T> future = executorService.submit(() -> {
            // To use Kafka's classloader.
            Thread.currentThread().setContextClassLoader(null);
            final Consumer<byte[], byte[]> consumer = buildKafkaResource("-temp-command");
            if (consumer == null) {
                return null;
            }

            try {
                return command.execute(consumer);
            } catch (Exception e) {
                if (exHander == null) {
                    throw e;
                }
                exHander.handle(e);

            } finally {
                consumer.close();
            }

            return null;
        });

        try {
            return future.get(CONSUMER_GRP_CMD_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException|ExecutionException|TimeoutException e) {
            final String msg = commandName + " failed due to " + e;
            logger.error(msg, e);
            throw new IOException(msg, e);
        } finally {
            future.cancel(true);
            executorService.shutdown();
        }
    }

    @Override
    protected Properties getDefaultKafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    /**
     * ConsumerKafka overrides this method in order to capture processor's property values required when it retrieves
     * its state managed externally at Kafka. Since view/clear state operation can be executed before onTrigger() is called,
     * we need to capture these values as it's modified. This method is also called when NiFi restarts and loads configs,
     * so users can access external states right after restart of NiFi.
     * @param descriptor of the modified property
     * @param oldValue non-null property value (previous)
     * @param newValue the new property value or if null indicates the property
     */
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (kafkaProperties == null) {
            kafkaProperties = getDefaultKafkaProperties();
        }
        setKafkaProperty(kafkaProperties, descriptor, new StandardPropertyValue(newValue, null, null));

        if (TOPIC.equals(descriptor)) {
            topic = kafkaProperties.getProperty(TOPIC.getName());
        } else if (BOOTSTRAP_SERVERS.equals(descriptor)) {
            brokers = kafkaProperties.getProperty(BOOTSTRAP_SERVERS.getName());
        }
    }
}