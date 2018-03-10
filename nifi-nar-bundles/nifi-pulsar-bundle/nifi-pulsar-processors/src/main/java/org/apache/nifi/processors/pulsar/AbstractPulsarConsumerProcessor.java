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
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public abstract class AbstractPulsarConsumerProcessor extends AbstractPulsarProcessor {

    static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
            + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages");

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("The name of the Pulsar Topic.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION = new PropertyDescriptor.Builder()
            .name("Subscription")
            .displayName("Subscription Name")
            .description("The name of the Pulsar subscription to consume from.")
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
                    + "nearest millisecond. The timeout needs to be greater than 10 seconds.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor PRIORITY_LEVEL = new PropertyDescriptor.Builder()
            .name("Consumer Priority Level")
            .description("Sets priority level for the shared subscription consumers to which broker "
                    + "gives more priority while dispatching messages. Here, broker follows descending "
                    + "priorities. (eg: 0=max-priority, 1, 2,..) ")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
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


    // Reuse the same consumer for a given topic / subscription
    protected PulsarConsumer consumer;
    protected ConsumerConfiguration consumerConfig;

    // Pool for running multiple consume Async requests
    protected ExecutorService pool;
    protected ExecutorCompletionService<Message> completionService;

    @OnScheduled
    public void init(ProcessContext context) {
        pool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger());
        completionService = new ExecutorCompletionService<>(pool);
    }

    @OnUnscheduled
    public void shutDown() {
        // Stop all the async consumers
        pool.shutdownNow();
    }

    protected PulsarConsumer getWrappedConsumer(ProcessContext context) throws PulsarClientException {

        if (consumer != null)
            return consumer;

        final PulsarClientPool pulsarClientService = context.getProperty(PULSAR_CLIENT_SERVICE)
                .asControllerService(PulsarClientPool.class);

        try {
            consumer = pulsarClientService.getConsumerPool()
                    .acquire(getConsumerProperties(context));

            if (consumer == null || consumer.getConsumer() == null) {
                throw new PulsarClientException("Unable to create Pulsar Consumer");
            }

            return consumer;
        } catch (final InterruptedException ex) {
            return null;
        }
    }

    private Properties getConsumerProperties(ProcessContext context) {

        Properties props = new Properties();
        props.put(PulsarConsumerFactory.TOPIC_NAME, context.getProperty(TOPIC).getValue());
        props.put(PulsarConsumerFactory.SUBSCRIPTION_NAME, context.getProperty(SUBSCRIPTION).getValue());
        props.put(PulsarConsumerFactory.CONSUMER_CONFIG, getConsumerConfig(context));
        return props;
    }

    private ConsumerConfiguration getConsumerConfig(ProcessContext context) {

        if (consumerConfig == null) {
            consumerConfig = new ConsumerConfiguration();

            if (context.getProperty(ACK_TIMEOUT).isSet())
                consumerConfig.setAckTimeout(context.getProperty(ACK_TIMEOUT).asLong(), TimeUnit.MILLISECONDS);

            if (context.getProperty(PRIORITY_LEVEL).isSet())
                consumerConfig.setPriorityLevel(context.getProperty(PRIORITY_LEVEL).asInteger());

            if (context.getProperty(RECEIVER_QUEUE_SIZE).isSet())
                consumerConfig.setReceiverQueueSize(context.getProperty(RECEIVER_QUEUE_SIZE).asInteger());

            if (context.getProperty(SUBSCRIPTION_TYPE).isSet())
                consumerConfig.setSubscriptionType(SubscriptionType.valueOf(context.getProperty(SUBSCRIPTION_TYPE).getValue()));
        }

        return consumerConfig;

    }

}
