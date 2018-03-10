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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarProducer;
import org.apache.nifi.pulsar.cache.LRUCache;
import org.apache.nifi.pulsar.pool.PulsarProducerFactory;
import org.apache.nifi.pulsar.pool.ResourcePool;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;

public abstract class AbstractPulsarProducerProcessor extends AbstractPulsarProcessor {

    protected static final String MSG_COUNT = "msg.count";

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
            .expressionLanguageSupported(true)
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

    protected LRUCache<String, PulsarProducer> producers;
    protected ProducerConfiguration producerConfig;

    @OnStopped
    public void cleanUp(final ProcessContext context) {
       // Close all of the producers and invalidate them, so they get removed from the Resource Pool
       getProducerCache(context).clear();
    }

    protected PulsarProducer getWrappedProducer(String topic, ProcessContext context) throws PulsarClientException, IllegalArgumentException {

        PulsarProducer producer = getProducerCache(context).get(topic);

        if (producer != null)
            return producer;

        try {
            producer = context.getProperty(PULSAR_CLIENT_SERVICE)
                    .asControllerService(PulsarClientPool.class)
                    .getProducerPool().acquire(getProducerProperties(context, topic));

            if (producer != null) {
                producers.put(topic, producer);
            }

            return producer;

        } catch (InterruptedException e) {
            return null;
        }

    }

    private LRUCache<String, PulsarProducer> getProducerCache(ProcessContext context) {
        if (producers == null) {

            ResourcePool<PulsarProducer> pool = context.getProperty(PULSAR_CLIENT_SERVICE)
                    .asControllerService(PulsarClientPool.class)
                    .getProducerPool();

            producers = new LRUCache<String, PulsarProducer>(20, pool);
        }

        return producers;
    }

    private Properties getProducerProperties(ProcessContext context, String topic) {

        Properties props = new Properties();
        props.put(PulsarProducerFactory.TOPIC_NAME, topic);
        props.put(PulsarProducerFactory.PRODUCER_CONFIG, getProducerConfig(context));
        return props;
    }

    private ProducerConfiguration getProducerConfig(ProcessContext context) {

        if (producerConfig == null) {
            producerConfig = new ProducerConfiguration();

            if (context.getProperty(BATCHING_ENABLED).isSet())
                producerConfig.setBatchingEnabled(context.getProperty(BATCHING_ENABLED).asBoolean());

            if (context.getProperty(BATCHING_MAX_MESSAGES).isSet())
                producerConfig.setBatchingMaxMessages(context.getProperty(BATCHING_MAX_MESSAGES).asInteger());

            if (context.getProperty(BATCH_INTERVAL).isSet())
                producerConfig.setBatchingMaxPublishDelay(context.getProperty(BATCH_INTERVAL).asLong(), TimeUnit.MILLISECONDS);

            if (context.getProperty(BLOCK_IF_QUEUE_FULL).isSet())
                producerConfig.setBlockIfQueueFull(context.getProperty(BLOCK_IF_QUEUE_FULL).asBoolean());

            if (context.getProperty(COMPRESSION_TYPE).isSet())
                producerConfig.setCompressionType(CompressionType.valueOf(context.getProperty(COMPRESSION_TYPE).getValue()));

            if (context.getProperty(PENDING_MAX_MESSAGES).isSet())
                producerConfig.setMaxPendingMessages(context.getProperty(PENDING_MAX_MESSAGES).asInteger());

            if (context.getProperty(MESSAGE_ROUTING_MODE).isSet())
                producerConfig.setMessageRoutingMode(MessageRoutingMode.valueOf(context.getProperty(MESSAGE_ROUTING_MODE).getValue()));
        }

        return producerConfig;
    }
}
