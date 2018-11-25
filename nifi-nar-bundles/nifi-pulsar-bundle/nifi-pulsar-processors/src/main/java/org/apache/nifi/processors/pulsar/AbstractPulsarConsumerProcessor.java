/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public abstract class AbstractPulsarConsumerProcessor<T> extends AbstractProcessor {

    static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
            + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages.");

    static final AllowableValue CONSUME = new AllowableValue(ConsumerCryptoFailureAction.CONSUME.name(), "Consume",
            "Mark the message as consumed despite being unable to decrypt the contents");
    static final AllowableValue DISCARD = new AllowableValue(ConsumerCryptoFailureAction.DISCARD.name(), "Discard",
            "Discard the message and don't perform any addtional processing on the message");
    static final AllowableValue FAIL = new AllowableValue(ConsumerCryptoFailureAction.FAIL.name(), "Fail",
            "Report a failure condition, and then route the message contents to the FAILED relationship.");

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was consumed from Pulsar.")
            .build();

    public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("PULSAR_CLIENT_SERVICE")
            .displayName("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

    public static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("TOPICS")
            .displayName("Topic Names")
            .description("Specify the topics this consumer will subscribe on. "
                    + "You can specify multiple topics in a comma-separated list."
                    + "E.g topicA, topicB, topicC ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOPICS_PATTERN = new PropertyDescriptor.Builder()
            .name("TOPICS_PATTERN")
            .displayName("Topics Pattern")
            .description("Alternatively, you can specify a pattern for topics that this consumer "
                    + "will subscribe on. It accepts a regular expression and will be compiled into "
                    + "a pattern internally. E.g. \"persistent://my-tenant/ns-abc/pattern-topic-.*\" "
                    + "would subscribe to any topic whose name started with 'pattern-topic-' that was in "
                    + "the 'ns-abc' namespace, and belonged to the 'my-tenant' tentant.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_NAME = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_NAME")
            .displayName("Subscription Name")
            .description("Specify the subscription name for this consumer.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ASYNC_ENABLED = new PropertyDescriptor.Builder()
            .name("ASYNC_ENABLED")
            .displayName("Async Enabled")
            .description("Control whether the messages will be consumed asyncronously or not. Messages consumed"
                    + " syncronously will be acknowledged immediately before processing the next message, while"
                    + " asyncronous messages will be acknowledged after the Pulsar broker responds. \n"
                    + "Enabling asyncronous message consumption introduces the possibility of duplicate data "
                    + "consumption in the case where the Processor is stopped before it has time to send an "
                    + "acknowledgement back to the Broker. In this scenario, the Broker would assume that the "
                    + "un-acknowledged message was not successuflly processed and re-send it when the Processor restarted.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_ASYNC_REQUESTS = new PropertyDescriptor.Builder()
            .name("MAX_ASYNC_REQUESTS")
            .displayName("Maximum Async Requests")
            .description("The maximum number of outstanding asynchronous consumer requests for this processor. "
                    + "Each asynchronous call requires memory, so avoid setting this value to high.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor ACK_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ACK_TIMEOUT")
            .displayName("Acknowledgment Timeout")
            .description("Set the timeout for unacked messages. Messages that are not acknowledged within the "
                    + "configured timeout will be replayed. This value needs to be greater than 10 seconds.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .required(false)
            .build();

    public static final PropertyDescriptor CONSUMER_NAME = new PropertyDescriptor.Builder()
            .name("CONSUMER_NAME")
            .displayName("Consumer Name")
            .description("Set the name of the consumer to uniquely identify this client on the Broker")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIORITY_LEVEL = new PropertyDescriptor.Builder()
            .name("PRIORITY_LEVEL")
            .displayName("Consumer Priority Level")
            .description("Sets priority level for the shared subscription consumers to which broker "
                    + "gives more priority while dispatching messages. Here, broker follows descending "
                    + "priorities. (eg: 0=max-priority, 1, 2,..) ")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor RECEIVER_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("RECEIVER_QUEUE_SIZE")
            .displayName("Consumer Receiver Queue Size")
            .description("The consumer receive queue controls how many messages can be accumulated "
                    + "by the Consumer before the application calls Consumer.receive(). Using a higher "
                    + "value could potentially increase the consumer throughput at the expense of bigger "
                    + "memory utilization. \n"
                    + "Setting the consumer queue size as zero, \n"
                    + "\t - Decreases the throughput of the consumer, by disabling pre-fetching of messages. \n"
                    + "\t - Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer "
                    + "connection with broker and consumer will not be able receive any further message unless batch-message "
                    + "in pipeline is removed")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_TYPE = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_TYPE")
            .displayName("Subscription Type")
            .description("Select the subscription type to be used when subscribing to the topic.")
            .required(true)
            .allowableValues(EXCLUSIVE, SHARED, FAILOVER)
            .defaultValue(SHARED.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("MESSAGE_DEMARCATOR")
            .displayName("Message Demarcator")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\n")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages consumed from Pulsar within "
                + "a single FlowFile. If not specified, the content of the FlowFile will consist of all of the messages consumed from Pulsar "
                + "concatenated together. If specified, the contents of the individual Pulsar messages will be separate by this delimiter. "
                + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor CONSUMER_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("CONSUMER_BATCH_SIZE")
            .displayName("Consumer Message Batch Size")
            .description("Set the maximum number of messages consumed at a time, and published to a single FlowFile. "
                    + "default: 1000. If set to a value greater than 1, messages within the FlowFile will be seperated "
                    + "by the Message Demarcator.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPICS);
        properties.add(TOPICS_PATTERN);
        properties.add(SUBSCRIPTION_NAME);
        properties.add(CONSUMER_NAME);
        properties.add(ASYNC_ENABLED);
        properties.add(MAX_ASYNC_REQUESTS);
        properties.add(ACK_TIMEOUT);
        properties.add(PRIORITY_LEVEL);
        properties.add(RECEIVER_QUEUE_SIZE);
        properties.add(SUBSCRIPTION_TYPE);
        properties.add(CONSUMER_BATCH_SIZE);
        properties.add(MESSAGE_DEMARCATOR);

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    private PulsarClientService pulsarClientService;
    private PulsarClientLRUCache<String, Consumer<T>> consumers;
    private ExecutorService consumerPool;
    private ExecutorCompletionService<List<Message<T>>> consumerService;
    private ExecutorService ackPool;
    private ExecutorCompletionService<Object> ackService;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        boolean topicsSet = validationContext.getProperty(TOPICS).isSet();
        boolean topicPatternSet = validationContext.getProperty(TOPICS_PATTERN).isSet();

        if (!topicsSet && !topicPatternSet) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "At least one of the 'Topics' or 'Topic Pattern' properties must be specified.").build());
        } else if (topicsSet && topicPatternSet) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "Only one of the two properties ('Topics' and 'Topic Pattern') can be specified.").build());
        }

        if (validationContext.getProperty(ACK_TIMEOUT).asTimePeriod(TimeUnit.SECONDS) < 10) {
           results.add(new ValidationResult.Builder().valid(false).explanation(
               "Acknowledgment Timeout needs to be greater than 10 seconds.").build());
        }

        return results;
    }

    @OnScheduled
    public void init(ProcessContext context) {
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            setConsumerPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger()));
            setConsumerService(new ExecutorCompletionService<>(getConsumerPool()));
            setAckPool(Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger() + 1));
            setAckService(new ExecutorCompletionService<>(getAckPool()));
        }

        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
        /*
         * If we are running in asynchronous mode, then we need to stop all of the consumer threads that
         * are running in the ConsumerPool. After, we have stopped them, we need to wait a little bit
         * to ensure that all of the messages are properly acked, in order to prevent re-processing the
         * same messages in the event of a shutdown and restart of the processor since the un-acked
         * messages would be replayed on startup.
         */
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            try {
                getConsumerPool().shutdown();
                getAckPool().shutdown();

                // Allow some time for the acks to be sent back to the Broker.
                getConsumerPool().awaitTermination(10, TimeUnit.SECONDS);
                getAckPool().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getLogger().error("Unable to stop all the Pulsar Consumers", e);
            }
        }
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
        shutDown(context);
        getConsumers().clear();
    }

    /**
     * Method returns a string that uniquely identifies a consumer by concatenating
     * the topic name and subscription properties together.
     */
    protected String getConsumerId(final ProcessContext context, FlowFile flowFile) {
        if (context == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();

        if (context.getProperty(TOPICS).isSet()) {
           sb.append(context.getProperty(TOPICS).evaluateAttributeExpressions(flowFile).getValue());
        } else {
           sb.append(context.getProperty(TOPICS_PATTERN).getValue());
        }

        sb.append("-").append(context.getProperty(SUBSCRIPTION_NAME).getValue());

        if (context.getProperty(CONSUMER_NAME).isSet()) {
            sb.append("-").append(context.getProperty(CONSUMER_NAME).getValue());
        }
        return sb.toString();
    }

    protected void consumeAsync(final Consumer<T> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            getConsumerService().submit(() -> {
                List<Message<T>> messages = new LinkedList<Message<T>>();
                Message<T> msg = null;
                AtomicInteger msgCount = new AtomicInteger(0);

                while (((msg = consumer.receive(0, TimeUnit.SECONDS)) != null) && msgCount.get() < maxMessages) {
                    messages.add(msg);
                    msgCount.incrementAndGet();
                }
                return messages;
            });
        } catch (final RejectedExecutionException ex) {
            getLogger().error("Unable to consume any more Pulsar messages", ex);
            context.yield();
        }
    }

    protected synchronized Consumer<T> getConsumer(ProcessContext context, String topic) throws PulsarClientException {

        /* Avoid creating producers for non-existent topics */
        if (StringUtils.isBlank(topic)) {
          return null;
        }

        Consumer<T> consumer = getConsumers().get(topic);

        if (consumer != null && consumer.isConnected()) {
           return consumer;
        }

        // Create a new consumer and validate that it is connected before returning it.
        consumer = getConsumerBuilder(context).subscribe();
        if (consumer != null && consumer.isConnected()) {
           getConsumers().put(topic, consumer);
        }

        return (consumer != null && consumer.isConnected()) ? consumer : null;
    }

    protected synchronized ConsumerBuilder<T> getConsumerBuilder(ProcessContext context) throws PulsarClientException {

        ConsumerBuilder<T> builder = (ConsumerBuilder<T>) getPulsarClientService().getPulsarClient().newConsumer();

        if (context.getProperty(TOPICS).isSet()) {
            builder = builder.topic(Arrays.stream(context.getProperty(TOPICS).evaluateAttributeExpressions().getValue().split("[, ]"))
                    .map(String::trim).toArray(String[]::new));
        } else if (context.getProperty(TOPICS_PATTERN).isSet()) {
            builder = builder.topicsPattern(context.getProperty(TOPICS_PATTERN).getValue());
        }

        if (context.getProperty(CONSUMER_NAME).isSet()) {
            builder = builder.consumerName(context.getProperty(CONSUMER_NAME).getValue());
        }

        return builder.subscriptionName(context.getProperty(SUBSCRIPTION_NAME).getValue())
                .ackTimeout(context.getProperty(ACK_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                .priorityLevel(context.getProperty(PRIORITY_LEVEL).asInteger())
                .receiverQueueSize(context.getProperty(RECEIVER_QUEUE_SIZE).asInteger())
                .subscriptionType(SubscriptionType.valueOf(context.getProperty(SUBSCRIPTION_TYPE).getValue()));
    }

    protected synchronized ExecutorService getConsumerPool() {
        return consumerPool;
    }

    protected synchronized void setConsumerPool(ExecutorService pool) {
        this.consumerPool = pool;
    }

    protected synchronized ExecutorCompletionService<List<Message<T>>> getConsumerService() {
        return consumerService;
    }

    protected synchronized void setConsumerService(ExecutorCompletionService<List<Message<T>>> service) {
        this.consumerService = service;
    }

    protected synchronized ExecutorService getAckPool() {
       return ackPool;
    }

    protected synchronized void setAckPool(ExecutorService pool) {
       this.ackPool = pool;
    }

    protected synchronized ExecutorCompletionService<Object> getAckService() {
       return ackService;
    }

    protected synchronized void setAckService(ExecutorCompletionService<Object> ackService) {
       this.ackService = ackService;
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    protected synchronized PulsarClientLRUCache<String, Consumer<T>> getConsumers() {
        if (consumers == null) {
           consumers = new PulsarClientLRUCache<String, Consumer<T>>(20);
        }
        return consumers;
    }

    protected void setConsumers(PulsarClientLRUCache<String, Consumer<T>> consumers) {
        this.consumers = consumers;
    }
}
