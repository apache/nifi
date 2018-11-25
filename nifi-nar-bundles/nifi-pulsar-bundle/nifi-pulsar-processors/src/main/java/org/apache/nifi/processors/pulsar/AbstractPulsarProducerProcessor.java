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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.cache.PulsarClientLRUCache;
import org.apache.nifi.util.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.org.apache.commons.collections.CollectionUtils;

public abstract class AbstractPulsarProducerProcessor<T> extends AbstractProcessor {

    public static final String MSG_COUNT = "msg.count";
    public static final String TOPIC_NAME = "topic.name";

    static final AllowableValue COMPRESSION_TYPE_NONE = new AllowableValue("NONE", "None", "No compression");
    static final AllowableValue COMPRESSION_TYPE_LZ4 = new AllowableValue("LZ4", "LZ4", "Compress with LZ4 algorithm.");
    static final AllowableValue COMPRESSION_TYPE_ZLIB = new AllowableValue("ZLIB", "ZLIB", "Compress with ZLib algorithm");

    static final AllowableValue MESSAGE_ROUTING_MODE_CUSTOM_PARTITION = new AllowableValue("CustomPartition", "Custom Partition", "Route messages to a custom partition");
    static final AllowableValue MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION = new AllowableValue("RoundRobinPartition", "Round Robin Partition", "Route messages to all "
                                                                                                                       + "partitions in a round robin manner");
    static final AllowableValue MESSAGE_ROUTING_MODE_SINGLE_PARTITION = new AllowableValue("SinglePartition", "Single Partition", "Route messages to a single partition");

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Pulsar.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Pulsar will be routed to this Relationship")
            .build();

    public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("PULSAR_CLIENT_SERVICE")
            .displayName("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("TOPIC")
            .displayName("Topic Name")
            .description("The name of the Pulsar Topic.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ASYNC_ENABLED = new PropertyDescriptor.Builder()
            .name("ASYNC_ENABLED")
            .displayName("Async Enabled")
            .description("Control whether the messages will be sent asyncronously or not. Messages sent"
                    + " syncronously will be acknowledged immediately before processing the next message, while"
                    + " asyncronous messages will be acknowledged after the Pulsar broker responds. Running the"
                    + " processor with async enabled will result in increased the throughput at the risk of potential"
                    + " duplicate data being sent to the Pulsar broker.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_ASYNC_REQUESTS = new PropertyDescriptor.Builder()
            .name("MAX_ASYNC_REQUESTS")
            .displayName("Maximum Async Requests")
            .description("The maximum number of outstanding asynchronous publish requests for this processor. "
                    + "Each asynchronous call requires memory, so avoid setting this value to high.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor BATCHING_ENABLED = new PropertyDescriptor.Builder()
            .name("BATCHING_ENABLED")
            .displayName("Batching Enabled")
            .description("Control whether automatic batching of messages is enabled for the producer. "
                    + "default: false [No batching] When batching is enabled, multiple calls to "
                    + "Producer.sendAsync can result in a single batch to be sent to the broker, leading "
                    + "to better throughput, especially when publishing small messages. If compression is "
                    + "enabled, messages will be compressed at the batch level, leading to a much better "
                    + "compression ratio for similar headers or contents. When enabled default batch delay "
                    + "is set to 10 ms and default batch size is 1000 messages")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor BATCHING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("BATCHING_MAX_MESSAGES")
            .displayName("Batching Max Messages")
            .description("Set the maximum number of messages permitted in a batch within the Pulsar client. "
                    + "default: 1000. If set to a value greater than 1, messages will be queued until this "
                    + "threshold is reached or the batch interval has elapsed, whichever happens first.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor BATCH_INTERVAL = new PropertyDescriptor.Builder()
            .name("BATCH_INTERVAL")
            .displayName("Batch Interval")
            .description("Set the time period within which the messages sent will be batched if batch messages are enabled."
                    + " If set to a non zero value, messages will be queued until this time interval has been reached OR"
                    + " until the Batching Max Messages threshould has been reached, whichever occurs first.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("10 ms")
            .build();

    public static final PropertyDescriptor BLOCK_IF_QUEUE_FULL = new PropertyDescriptor.Builder()
            .name("BLOCK_IF_QUEUE_FULL")
            .displayName("Block if Message Queue Full")
            .description("Set whether the processor should block when the outgoing message queue is full. "
                    + "Default is false. If set to false, send operations will immediately fail with "
                    + "ProducerQueueIsFullError when there is no space left in pending queue.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("COMPRESSION_TYPE")
            .displayName("Compression Type")
            .description("Set the compression type for the producer.")
            .required(true)
            .allowableValues(COMPRESSION_TYPE_NONE, COMPRESSION_TYPE_LZ4, COMPRESSION_TYPE_ZLIB)
            .defaultValue(COMPRESSION_TYPE_NONE.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("MESSAGE_DEMARCATOR")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Pulsar message. "
                + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor MESSAGE_ROUTING_MODE = new PropertyDescriptor.Builder()
            .name("MESSAGE_ROUTING_MODE")
            .displayName("Message Routing Mode")
            .description("Set the message routing mode for the producer. This applies only if the destination topic is partitioned")
            .required(true)
            .allowableValues(MESSAGE_ROUTING_MODE_CUSTOM_PARTITION, MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION, MESSAGE_ROUTING_MODE_SINGLE_PARTITION)
            .defaultValue(MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION.getValue())
            .build();

    public static final PropertyDescriptor PENDING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("PENDING_MAX_MESSAGES")
            .displayName("Max Pending Messages")
            .description("Set the max size of the queue holding the messages pending to receive an "
                    + "acknowledgment from the broker.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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
        properties.add(MESSAGE_DEMARCATOR);
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

    private PulsarClientService pulsarClientService;
    private PulsarClientLRUCache<String, Producer<T>> producers;
    private ExecutorService publisherPool;

    // Used to sync between onTrigger method and shutdown code block.
    protected AtomicBoolean canPublish = new AtomicBoolean();

    // Used to track whether we are reporting errors back to the user or not.
    protected AtomicBoolean trackFailures = new AtomicBoolean();

    private int maxRequests = 1;

    protected BlockingQueue<Pair<String,T>> workQueue;
    protected BlockingQueue<Pair<String,T>> failureQueue;
    protected List<AsyncPublisher> asyncPublishers;

    @OnScheduled
    public void init(ProcessContext context) {
        maxRequests = context.getProperty(MAX_ASYNC_REQUESTS).asInteger();
        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));

        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            setPublisherPool(Executors.newFixedThreadPool(maxRequests));
            setAsyncPublishers(new LinkedList<AsyncPublisher>());
            // Limit the depth of the work queue to 500 per worker, to prevent long shutdown times.
            workQueue = new LinkedBlockingQueue<Pair<String,T>>(500 * maxRequests);

            if (context.hasConnection(REL_FAILURE)) {
                failureQueue = new LinkedBlockingQueue<Pair<String,T>>();
                trackFailures.set(true);
            } else {
                trackFailures.set(false);
            }

            for (int idx = 0; idx < maxRequests; idx++) {
                AsyncPublisher worker = new AsyncPublisher();
                getAsyncPublishers().add(worker);
                getPublisherPool().submit(worker);
            }
            canPublish.set(true);
        }
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
        /*
         * If we are running in asynchronous mode, then we need to stop all of the producer threads that
         * are running in the PublisherPool. After, we have stopped them, we need to wait a little bit
         * to ensure that all of the messages are properly acked, in order to prevent re-processing the
         * same messages in the event of a shutdown and restart of the processor since the un-acked
         * messages would be replayed on startup.
         */
       if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
           try {
              // Stop accepting incoming work
              canPublish.set(false);

              // Halt the background worker threads, allowing them to empty the workQueue
              getAsyncPublishers().forEach(publisher->{
                 publisher.halt();
              });

              // Flush all of the pending messages in the producers
              getProducers().values().forEach(producer -> {
                   try {
                     producer.flush();
                   } catch (PulsarClientException e) {
                      // ignore
                   }
              });

              // Shutdown the thread pool
              getPublisherPool().shutdown();
              getPublisherPool().awaitTermination(1, TimeUnit.SECONDS);
           } catch (InterruptedException e) {
              getLogger().error("Unable to stop all the Pulsar Producers", e);
           }
       }
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            if (canPublish.get()) {
               shutDown(context);
            }
            workQueue.clear();
            getProducers().clear();
            getAsyncPublishers().clear();
        }
    }

    /**
     * If the processor is configured to run in asynchronous mode, then we need to periodically
     * check the failureList and route those records to the FAILURE relationship, so that the end
     * user is aware of the failures and can handle them as they see fit.
     */
    protected void handleFailures(ProcessSession session) {

        if (!trackFailures.get() || CollectionUtils.isEmpty(failureQueue)) {
           return;
        }

        Pair<String,T> failure = failureQueue.poll();

        while (failure != null) {
            FlowFile flowFile = session.create();
            final byte[] value = (byte[]) failure.getValue();
            flowFile = session.write(flowFile, out -> {
                 out.write(value);
            });
            session.putAttribute(flowFile, TOPIC_NAME, failure.getKey());
            session.transfer(flowFile, REL_FAILURE);
            failure = failureQueue.poll();
        }
    }

    private synchronized List<AbstractPulsarProducerProcessor<T>.AsyncPublisher> getAsyncPublishers() {
        return asyncPublishers;
    }

    private synchronized void setAsyncPublishers(List<AbstractPulsarProducerProcessor<T>.AsyncPublisher> list) {
       asyncPublishers = list;
    }

    protected synchronized Producer<T> getProducer(ProcessContext context, String topic) {

        /* Avoid creating producers for non-existent topics */
        if (StringUtils.isBlank(topic)) {
           return null;
        }

        Producer<T> producer = getProducers().get(topic);

        try {
            if (producer != null && producer.isConnected()) {
              return producer;
            }

            producer = getBuilder(context, topic).create();

            if (producer != null && producer.isConnected()) {
              getProducers().put(topic, producer);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to create Pulsar Producer ", e);
            producer = null;
        }
        return (producer != null && producer.isConnected()) ? producer : null;
    }

    private synchronized ProducerBuilder<T> getBuilder(ProcessContext context, String topic) {
        ProducerBuilder<T> builder = (ProducerBuilder<T>) getPulsarClientService().getPulsarClient().newProducer();
        return builder.topic(topic)
                      .enableBatching(context.getProperty(BATCHING_ENABLED).asBoolean())
                      .batchingMaxMessages(context.getProperty(BATCHING_MAX_MESSAGES).evaluateAttributeExpressions().asInteger())
                      .batchingMaxPublishDelay(context.getProperty(BATCH_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                      .blockIfQueueFull(context.getProperty(BLOCK_IF_QUEUE_FULL).asBoolean())
                      .compressionType(CompressionType.valueOf(context.getProperty(COMPRESSION_TYPE).getValue()))
                      .maxPendingMessages(context.getProperty(PENDING_MAX_MESSAGES).evaluateAttributeExpressions().asInteger())
                      .messageRoutingMode(MessageRoutingMode.valueOf(context.getProperty(MESSAGE_ROUTING_MODE).getValue()));
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    protected synchronized PulsarClientLRUCache<String, Producer<T>> getProducers() {
       if (producers == null) {
         producers = new PulsarClientLRUCache<String, Producer<T>>(20);
       }
       return producers;
    }

    protected synchronized void setProducers(PulsarClientLRUCache<String, Producer<T>> producers) {
       this.producers = producers;
    }

    protected synchronized ExecutorService getPublisherPool() {
       return publisherPool;
    }

    protected synchronized void setPublisherPool(ExecutorService publisherPool) {
       this.publisherPool = publisherPool;
    }

    private final class AsyncPublisher implements Runnable {
        private boolean keepRunning = true;
        private boolean completed = false;

        public void halt() {
           keepRunning = false;

           // Finish up
           completed = workQueue.isEmpty();
           while (!completed) {
               process();
               completed = workQueue.isEmpty();
           }
        }

        @Override
        public void run() {
            while (keepRunning) {
               process();
            }
        }

        private void process() {
            try {
                Pair<String,T> item = workQueue.take();
                Producer<T> producer = getProducers().get(item.getLeft());

                if (!trackFailures.get()) {
                    // We don't care about failures, so just fire & forget
                    producer.sendAsync(item.getValue());
                } else if (producer == null || !producer.isConnected()) {
                    // We cannot get a valid producer, so add the item to the failure queue
                    failureQueue.put(item);
                } else {
                    try {
                        // Send the item asynchronously and confirm we get a messageId back from Pulsar.
                        if (producer.sendAsync(item.getValue()).join() == null) {
                            // No messageId indicates failure
                            failureQueue.put(item);
                        }
                    } catch (final Throwable t) {
                        // Any exception during sendAsync() call indicates failure
                        failureQueue.put(item);
                    }
                }
            } catch (InterruptedException e) {
                // Ignore these
            }
        }
    }
}