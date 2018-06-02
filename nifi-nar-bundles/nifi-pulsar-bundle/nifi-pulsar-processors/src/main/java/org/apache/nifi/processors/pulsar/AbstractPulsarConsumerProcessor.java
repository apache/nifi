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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public abstract class AbstractPulsarConsumerProcessor<T> extends AbstractPulsarProcessor {

    static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
                    + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages");

    static final AllowableValue CONSUME = new AllowableValue(ConsumerCryptoFailureAction.CONSUME.name(), "Consume",
            "Mark the message as consumed despite being unable to decrypt the contents");
    static final AllowableValue DISCARD = new AllowableValue(ConsumerCryptoFailureAction.DISCARD.name(), "Discard",
            "Discard the message and don't perform any addtional processing on the message");
    static final AllowableValue FAIL = new AllowableValue(ConsumerCryptoFailureAction.FAIL.name(), "Fail",
            "Report a failure condition, and the route the message contents to the FAILED relationship.");

    public static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("topics")
            .displayName("Topic Names")
            .description("Specify the topics this consumer will subscribe on. "
                    + "You can specify multiple topics in a comma-separated list."
                    + "E.g topicA, topicB, topicC ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOPICS_PATTERN = new PropertyDescriptor.Builder()
            .name("Topics Pattern")
            .description("Specify a pattern for topics that this consumer will subscribe on. "
                    + "It accepts regular expression and will be compiled into a pattern internally. "
                    + "E.g. \"persistent://prop/use/ns-abc/pattern-topic-.*\"")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_NAME = new PropertyDescriptor.Builder()
            .name("Subscription Name")
            .description("Specify the subscription name for this consumer.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ASYNC_ENABLED = new PropertyDescriptor.Builder()
            .name("Async Enabled")
            .description("Control whether the messages will be consumed asyncronously or not. Messages consumed"
                    + " syncronously will be acknowledged immediately before processing the next message, while"
                    + " asyncronous messages will be acknowledged after the Pulsar broker responds.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_ASYNC_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum Async Requests")
            .description("The maximum number of outstanding asynchronous consumer requests for this processor. "
                    + "Each asynchronous call requires memory, so avoid setting this value to high.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor ACK_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Acknowledgment Timeout")
            .description("Set the timeout (in milliseconds) for unacked messages, truncated to the "
                    + "nearest millisecond. A value of 0 means there is no timeout. If a non-zero value "
                    + "is sepcified, then messages that are not acknowledged within the configured"
                    + " timeout will be replayed.")
            .required(false)
            .build();

    public static final PropertyDescriptor CONSUMER_NAME = new PropertyDescriptor.Builder()
            .name("Consumer Name")
            .description("Set the name of the consumer to uniquely identify this client on the Broker")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIORITY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consumer Priority Level")
            .description("Sets priority level for the shared subscription consumers to which broker "
                    + "gives more priority while dispatching messages. Here, broker follows descending "
                    + "priorities. (eg: 0=max-priority, 1, 2,..) ")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor RECEIVER_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Consumer receive queue size.")
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
            .name("Subscription Type")
            .description("Select the subscription type to be used when subscribing to the topic.")
            .required(false)
            .allowableValues(EXCLUSIVE, SHARED, FAILOVER)
            .defaultValue(SHARED.getValue())
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

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    protected Consumer<T> consumer;
    protected ExecutorService consumerPool;
    protected ExecutorCompletionService<Message<T>> consumerService;
    protected ExecutorService ackPool;
    protected ExecutorCompletionService<Object> ackService;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void init(ProcessContext context) {
       if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
           consumerPool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger());
           consumerService = new ExecutorCompletionService<>(consumerPool);
           ackPool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger() + 1);
           ackService = new ExecutorCompletionService<>(ackPool);
       }
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
        if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
            try {
                consumerPool.shutdown();
                consumerPool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getLogger().error("Unable to stop all the Pulsar Consumers", e);
            }

            try {
                ackPool.shutdown();
                ackPool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                getLogger().error("Unable to wait for all of the message acknowledgments to be sent", e);
            }
        }
        close(context);
    }

    @OnStopped
    public void close(final ProcessContext context) {
        try {
            getLogger().info("Disconnecting Pulsar Consumer");
            if (consumer != null) {
              consumer.close();
            }
        } catch (Exception e) {
           getLogger().error("Unable to close Pulsar consumer", e);
        } finally {
           consumer = null;
        }
    }

    protected void consumeAsync(ProcessContext context, ProcessSession session) throws PulsarClientException {
        Consumer<T> consumer = getConsumer(context);

        try {
            consumerService.submit(() -> {
               return consumer.receive();
            });
        } catch (final RejectedExecutionException ex) {
            getLogger().error("Unable to consume aany more Pulsar messages", ex);
        }
    }

    protected Consumer<T> getConsumer(ProcessContext context) throws PulsarClientException {

        if (consumer == null) {
           ConsumerBuilder<T> builder = (ConsumerBuilder<T>) getPulsarClient(context).newConsumer();

           if (context.getProperty(TOPICS).isSet()) {
              builder = builder.topic(context.getProperty(TOPICS)
                               .evaluateAttributeExpressions().getValue().split("[, ]"));
           } else if (context.getProperty(TOPICS_PATTERN).isSet()) {
              builder = builder.topicsPattern(context.getProperty(TOPICS_PATTERN).getValue());
           } else {
             throw new PulsarClientException("No topic specified.");
           }

           if (context.getProperty(SUBSCRIPTION_NAME).isSet()) {
             builder = builder.subscriptionName(context.getProperty(SUBSCRIPTION_NAME).getValue());
           } else {
             throw new PulsarClientException("No subscription specified.");
           }

           if (context.getProperty(ACK_TIMEOUT).isSet()) {
             builder = builder.ackTimeout(context.getProperty(ACK_TIMEOUT).asLong(), TimeUnit.MILLISECONDS);
           }

           if (context.getProperty(CONSUMER_NAME).isSet()) {
             builder = builder.consumerName(context.getProperty(CONSUMER_NAME).getValue());
           }

           if (context.getProperty(PRIORITY_LEVEL).isSet()) {
              builder = builder.priorityLevel(context.getProperty(PRIORITY_LEVEL).asInteger());
           }

           if (context.getProperty(RECEIVER_QUEUE_SIZE).isSet()) {
              builder = builder.receiverQueueSize(context.getProperty(RECEIVER_QUEUE_SIZE).asInteger());
           }

           if (context.getProperty(SUBSCRIPTION_TYPE).isSet()) {
              builder = builder.subscriptionType(SubscriptionType.valueOf(context.getProperty(SUBSCRIPTION_TYPE).getValue()));
           }
           consumer = builder.subscribe();
        }
        return consumer;
    }
}
