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
package org.apache.nifi.processors.pulsar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.cache.LRUCache;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

public abstract class AbstractPulsarProducerProcessor<T> extends AbstractPulsarProcessor {

    public static final String MSG_COUNT = "msg.count";
    public static final String TOPIC_NAME = "topic.name";

    static final AllowableValue COMPRESSION_TYPE_NONE = new AllowableValue("NONE", "None", "No compression");
    static final AllowableValue COMPRESSION_TYPE_LZ4 = new AllowableValue("LZ4", "LZ4", "Compress with LZ4 algorithm.");
    static final AllowableValue COMPRESSION_TYPE_ZLIB = new AllowableValue("ZLIB", "ZLIB", "Compress with ZLib algorithm");

    static final AllowableValue MESSAGE_ROUTING_MODE_CUSTOM_PARTITION = new AllowableValue("CustomPartition", "Custom Partition", "Route messages to a custom partition");
    static final AllowableValue MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION = new AllowableValue("RoundRobinPartition", "Round Robin Partition", "Route messages to all "
                                                                                                                       + "partitions in a round robin manner");
    static final AllowableValue MESSAGE_ROUTING_MODE_SINGLE_PARTITION = new AllowableValue("SinglePartition", "Single Partition", "Route messages to a single partition");

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("The name of the Pulsar Topic.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ASYNC_ENABLED = new PropertyDescriptor.Builder()
            .name("Async Enabled")
            .description("Control whether the messages will be sent asyncronously or not. Messages sent"
                    + " syncronously will be acknowledged immediately before processing the next message, while"
                    + " asyncronous messages will be acknowledged after the Pulsar broker responds.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_ASYNC_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum Async Requests")
            .description("The maximum number of outstanding asynchronous publish requests for this processor. "
                    + "Each asynchronous call requires memory, so avoid setting this value to high.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor BATCHING_ENABLED = new PropertyDescriptor.Builder()
            .name("Batching Enabled")
            .description("Control whether automatic batching of messages is enabled for the producer. "
                    + "default: false [No batching] When batching is enabled, multiple calls to "
                    + "Producer.sendAsync can result in a single batch to be sent to the broker, leading "
                    + "to better throughput, especially when publishing small messages. If compression is "
                    + "enabled, messages will be compressed at the batch level, leading to a much better "
                    + "compression ratio for similar headers or contents. When enabled default batch delay "
                    + "is set to 10 ms and default batch size is 1000 messages")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor BATCHING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("Batching Max Messages")
            .description("Set the maximum number of messages permitted in a batch. default: "
                    + "1000 If set to a value greater than 1, messages will be queued until this "
                    + "threshold is reached or batch interval has elapsed")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor BATCH_INTERVAL = new PropertyDescriptor.Builder()
            .name("Batch Interval")
            .description("Set the time period within which the messages sent will be batched default: 10ms "
                    + "if batch messages are enabled. If set to a non zero value, messages will be queued until "
                    + "this time interval or until the Batching Max Messages threshould has been reached")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor BLOCK_IF_QUEUE_FULL = new PropertyDescriptor.Builder()
            .name("Block if Message Queue Full")
            .description("Set whether the processor should block when the outgoing message queue is full. "
                    + "Default is false. If set to false, send operations will immediately fail with "
                    + "ProducerQueueIsFullError when there is no space left in pending queue.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Set the compression type for the producer.")
            .required(false)
            .allowableValues(COMPRESSION_TYPE_NONE, COMPRESSION_TYPE_LZ4, COMPRESSION_TYPE_ZLIB)
            .defaultValue(COMPRESSION_TYPE_NONE.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_ROUTING_MODE = new PropertyDescriptor.Builder()
            .name("Message Routing Mode")
            .description("Set the message routing mode for the producer. This applies only if the destination topic is partitioned")
            .required(false)
            .allowableValues(MESSAGE_ROUTING_MODE_CUSTOM_PARTITION, MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION, MESSAGE_ROUTING_MODE_SINGLE_PARTITION)
            .defaultValue(MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION.getValue())
            .build();

    public static final PropertyDescriptor PENDING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("Max Pending Messages")
            .description("Set the max size of the queue holding the messages pending to receive an "
                    + "acknowledgment from the broker.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        properties.add(ASYNC_ENABLED);
        properties.add(MAX_ASYNC_REQUESTS);
        properties.add(BATCHING_ENABLED);
        properties.add(BATCHING_MAX_MESSAGES);
        properties.add(BATCH_INTERVAL);
        properties.add(BLOCK_IF_QUEUE_FULL);
        properties.add(COMPRESSION_TYPE);
        properties.add(MESSAGE_ROUTING_MODE);
        properties.add(PENDING_MAX_MESSAGES);
        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    protected LRUCache<String, Producer<T>> producers;

    // Pool for running multiple publish Async requests
    protected ExecutorService publisherPool;
    protected ExecutorCompletionService<Object> publisherService;

    @OnScheduled
    public void init(ProcessContext context) {
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            publisherPool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger());
            publisherService = new ExecutorCompletionService<>(publisherPool);
        }
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
       if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
           // Stop all the async publishers
           try {
              publisherPool.shutdown();
              publisherPool.awaitTermination(20, TimeUnit.SECONDS);
           } catch (InterruptedException e) {
              getLogger().error("Unable to stop all the Pulsar Producers", e);
           }
       }
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
       getProducerCache(context).clear();
    }

    @SuppressWarnings("rawtypes")
    protected void sendAsync(Producer producer, final ProcessSession session, final byte[] messageContent) {
        try {
            publisherService.submit(new Callable<Object>() {
               @Override
               public Object call() throws Exception {
                 try {
                     return producer.newMessage().value(messageContent).sendAsync().handle((msgId, ex) -> {
                        if (msgId != null) {
                           return msgId;
                        } else {
                           FlowFile flowFile = session.create();
                           session.transfer(flowFile, REL_FAILURE);
                           return null;
                        }
                   }).get();
                 } catch (final Throwable t) {
                   getLogger().error("Unable to send message to Pulsar asyncronously.", t);
                   session.transfer(session.create(), REL_FAILURE);
                   return null;
                 }
              }
             });
        } catch (final RejectedExecutionException ex) {
           getLogger().error("Unable to send message to Pulsar asyncronously, due to RejectedExecutionException: ", ex);
           session.transfer(session.create(), REL_FAILURE);
        }
    }

    protected void handleAsync() {
        try {
           Future<Object> done = null;
           do {
              done = publisherService.poll(50, TimeUnit.MILLISECONDS);
           } while (done != null);
        } catch (InterruptedException e) {
           getLogger().error("Trouble publishing messages ", e);
        }
    }

    private LRUCache<String, Producer<T>> getProducerCache(ProcessContext context) {
        if (producers == null) {
           producers = new LRUCache<String, Producer<T>>(20);
        }
        return producers;
    }

    protected Producer<T> getProducer(ProcessContext context, String topic) throws PulsarClientException {
        Producer<T> producer = getProducerCache(context).get(topic);

        if (producer != null) {
          return producer;
        }

        producer = getBuilder(context, topic).create();

        if (producer != null) {
          producers.put(topic, producer);
        }
        return producer;
    }

    private ProducerBuilder<T> getBuilder(ProcessContext context, String topic) {
        ProducerBuilder<T> builder = (ProducerBuilder<T>) getPulsarClient(context).newProducer();

        builder = builder.topic(topic);

        if (context.getProperty(BATCHING_ENABLED).isSet()) {
            builder = builder.enableBatching(context.getProperty(BATCHING_ENABLED).asBoolean());
        }

        if (context.getProperty(BATCHING_MAX_MESSAGES).isSet()) {
           builder = builder.batchingMaxMessages(context.getProperty(BATCHING_MAX_MESSAGES).asInteger());
        }

        if (context.getProperty(BATCH_INTERVAL).isSet()) {
           builder = builder.batchingMaxPublishDelay(context.getProperty(BATCH_INTERVAL).asLong(), TimeUnit.MILLISECONDS);
        }

        if (context.getProperty(BLOCK_IF_QUEUE_FULL).isSet()) {
           builder = builder.blockIfQueueFull(context.getProperty(BLOCK_IF_QUEUE_FULL).asBoolean());
        }

        if (context.getProperty(COMPRESSION_TYPE).isSet()) {
           builder = builder.compressionType(CompressionType.valueOf(context.getProperty(COMPRESSION_TYPE).getValue()));
        }

        if (context.getProperty(PENDING_MAX_MESSAGES).isSet()) {
           builder = builder.maxPendingMessages(context.getProperty(PENDING_MAX_MESSAGES).asInteger());
        }

        if (context.getProperty(MESSAGE_ROUTING_MODE).isSet()) {
           builder = builder.messageRoutingMode(MessageRoutingMode.valueOf(context.getProperty(MESSAGE_ROUTING_MODE).getValue()));
        }
        return builder;
    }
}