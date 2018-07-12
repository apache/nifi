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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

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
import org.apache.nifi.processors.pulsar.pubsub.InFlightMessageMonitor;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.cache.LRUCache;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

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
            .name("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

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
                    + " asyncronous messages will be acknowledged after the Pulsar broker responds. Running the"
                    + " processor with async enabled will result in increased the throughput at the risk of potential"
                    + " duplicate data being sent to the Pulsar broker.")
            .required(true)
            .allowableValues("true", "false")
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
            .allowableValues("true", "false")
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
            .description("Set the time period within which the messages sent will be batched if batch messages are enabled."
                    + " If set to a non zero value, messages will be queued until this time interval has been reached OR"
                    + " until the Batching Max Messages threshould has been reached, whichever occurs first.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 ms")
            .build();

    public static final PropertyDescriptor BLOCK_IF_QUEUE_FULL = new PropertyDescriptor.Builder()
            .name("Block if Message Queue Full")
            .description("Set whether the processor should block when the outgoing message queue is full. "
                    + "Default is false. If set to false, send operations will immediately fail with "
                    + "ProducerQueueIsFullError when there is no space left in pending queue.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("Compression Type")
            .description("Set the compression type for the producer.")
            .required(false)
            .allowableValues(COMPRESSION_TYPE_NONE, COMPRESSION_TYPE_LZ4, COMPRESSION_TYPE_ZLIB)
            .defaultValue(COMPRESSION_TYPE_NONE.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("message-demarcator")
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
    private LRUCache<String, Producer<T>> producers;
    private ExecutorService publisherPool;
    private ExecutorCompletionService<Object> publisherService;

    @OnScheduled
    public void init(ProcessContext context) {
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            setPublisherPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger()));
            setPublisherService(new ExecutorCompletionService<>(publisherPool));
        }
        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
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
              getPublisherPool().shutdown();
              getPublisherPool().awaitTermination(20, TimeUnit.SECONDS);
           } catch (InterruptedException e) {
              getLogger().error("Unable to stop all the Pulsar Producers", e);
           }
       }
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
       getProducers().clear();
    }

    protected void sendAsync(Producer<byte[]> producer, ProcessSession session, FlowFile flowFile, InFlightMessageMonitor<byte[]> monitor) {
        if (monitor == null || monitor.getRecords().isEmpty())
           return;

        for (byte[] record: monitor.getRecords() ) {
           try {
              getPublisherService().submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                  try {
                     return producer.sendAsync(record).handle((msgId, ex) -> {
                         if (msgId != null) {
                            monitor.getSuccessCounter().incrementAndGet();
                            return msgId;
                         } else {
                            monitor.getFailureCounter().incrementAndGet();
                            monitor.getFailures().add(record);
                            return null;
                         }
                     }).get();

                   } catch (final Throwable t) {
                      // This traps any exceptions thrown while calling the producer.sendAsync() method.
                      monitor.getFailureCounter().incrementAndGet();
                      monitor.getFailures().add(record);
                      return null;
                   } finally {
                      monitor.getLatch().countDown();
                   }
               }
             });
          } catch (final RejectedExecutionException ex) {
            // This can happen if the processor is being Unscheduled.
          }
       }
    }

    protected void handleAsync(InFlightMessageMonitor<byte[]> monitor, ProcessSession session, FlowFile flowFile, String topic) {
       try {
           monitor.getLatch().await();

           if (monitor.getSuccessCounter().intValue() > 0) {
               // Report the number of messages successfully sent to Pulsar.
               session.getProvenanceReporter().send(flowFile, "Sent " + monitor.getSuccessCounter().get() + " records to " + getPulsarClientService().getPulsarBrokerRootURL() );
           }

           if (monitor.getFailureCounter().intValue() > 0) {
              session.putAttribute(flowFile, MSG_COUNT, String.valueOf(monitor.getFailureCounter().get()));
              session.transfer(flowFile, REL_FAILURE);
           } else {
               flowFile = session.putAttribute(flowFile, MSG_COUNT, monitor.getSuccessCounter().get() + "");
               flowFile = session.putAttribute(flowFile, TOPIC_NAME, topic);
               session.adjustCounter("Messages Sent", monitor.getSuccessCounter().get(), true);
               session.transfer(flowFile, REL_SUCCESS);
           }
        } catch (InterruptedException e) {
          getLogger().error("Pulsar did not receive all async messages", e);
        }
    }

    protected Producer<T> getProducer(ProcessContext context, String topic) {
        Producer<T> producer = getProducers().get(topic);

        try {
            if (producer != null) {
              return producer;
            }
            producer = getBuilder(context, topic).create();
            if (producer != null) {
              getProducers().put(topic, producer);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to create Pulsar Producer ", e);
            producer = null;
        }
        return producer;
    }

    private ProducerBuilder<T> getBuilder(ProcessContext context, String topic) {
        ProducerBuilder<T> builder = (ProducerBuilder<T>) getPulsarClientService().getPulsarClient().newProducer();
        return builder.topic(topic)
                      .enableBatching(context.getProperty(BATCHING_ENABLED).asBoolean())
                      .batchingMaxMessages(context.getProperty(BATCHING_MAX_MESSAGES).asInteger())
                      .batchingMaxPublishDelay(context.getProperty(BATCH_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                      .blockIfQueueFull(context.getProperty(BLOCK_IF_QUEUE_FULL).asBoolean())
                      .compressionType(CompressionType.valueOf(context.getProperty(COMPRESSION_TYPE).getValue()))
                      .maxPendingMessages(context.getProperty(PENDING_MAX_MESSAGES).asInteger())
                      .messageRoutingMode(MessageRoutingMode.valueOf(context.getProperty(MESSAGE_ROUTING_MODE).getValue()));
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    protected synchronized LRUCache<String, Producer<T>> getProducers() {
       if (producers == null) {
         producers = new LRUCache<String, Producer<T>>(20);
       }
       return producers;
    }

    protected synchronized void setProducers(LRUCache<String, Producer<T>> producers) {
       this.producers = producers;
    }

    protected synchronized ExecutorService getPublisherPool() {
       return publisherPool;
    }

    protected synchronized void setPublisherPool(ExecutorService publisherPool) {
       this.publisherPool = publisherPool;
    }

    protected synchronized ExecutorCompletionService<Object> getPublisherService() {
       return publisherService;
    }

    protected synchronized void setPublisherService(ExecutorCompletionService<Object> publisherService) {
       this.publisherService = publisherService;
    }
}