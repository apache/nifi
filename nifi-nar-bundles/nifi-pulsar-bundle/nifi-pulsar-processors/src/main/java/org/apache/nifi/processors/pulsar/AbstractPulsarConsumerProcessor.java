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

import java.util.Properties;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.pulsar.client.api.Consumer;
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

        public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
                        .name("Max Wait Time")
                        .description("The maximum amount of time allowed for a Pulsar consumer to poll a subscription for data "
                                        + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
                        .defaultValue("2 seconds")
                        .required(true)
                        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
                        .expressionLanguageSupported(true)
                        .build();

        // Reuse the same consumer for a given topic / subscription
        protected PulsarConsumer consumer;
        protected ConsumerConfiguration consumerConfig;

        // Pool for running multiple consume Async requests
        protected ExecutorService consumerPool;
        protected ExecutorCompletionService<Message> consumerService;

        // Pool for async acknowledgments. This way we can wait for them all to be completed on shut down
        protected ExecutorService ackPool;
        protected ExecutorCompletionService<Void> ackService;

        private AsyncAcknowledgmentMonitorThread ackMonitor;


        @OnScheduled
        public void init(ProcessContext context) {
           // We only need this background thread if we are running in Async mode
           if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
                 consumerPool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger());
                 consumerService = new ExecutorCompletionService<>(consumerPool);

                 ackPool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger() + 1);
                 ackService = new ExecutorCompletionService<>(ackPool);

                 ackMonitor = new AsyncAcknowledgmentMonitorThread(ackService);
                 ackPool.submit(ackMonitor);
           }
        }

        @OnUnscheduled
        public void shutDown(final ProcessContext context) {

             if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
                 // Stop all the async consumers
                 try {
                     consumerPool.shutdown();
                     consumerPool.awaitTermination(10, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                        getLogger().error("Unable to stop all the Pulsar Consumers", e);
                  }

                 // Wait for the async acknowledgments to complete.
                 try {
                      ackPool.shutdown();
                      ackPool.awaitTermination(10, TimeUnit.SECONDS);
                 } catch (InterruptedException e) {
                      getLogger().error("Unable to wait for all of the message acknowledgments to be sent", e);
                 }
             }
        }

        @OnStopped
        public void close(final ProcessContext context) {

            getLogger().info("Disconnecting Pulsar Consumer");
            if (consumer != null) {

                context.getProperty(PULSAR_CLIENT_SERVICE)
                       .asControllerService(PulsarClientPool.class)
                       .getConsumerPool().evict(consumer);
            }

            consumer = null;
        }

        /*
         * For now let's assume that this processor will be configured to run for a longer
         * duration than 0 milliseconds. So we will be grabbing as many messages off the topic
         * as possible and committing them as FlowFiles
         */
        protected void consumeAsync(ProcessContext context, ProcessSession session) throws PulsarClientException {

            Consumer consumer = getWrappedConsumer(context).getConsumer();

            try {
                    consumerService.submit(new Callable<Message>() {
                         @Override
                         public Message call() throws Exception {
                             return consumer.receiveAsync().get();
                         }
                 });
            } catch (final RejectedExecutionException ex) {
                    // This can happen if the processor is being Unscheduled.
            }

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

        protected Properties getConsumerProperties(ProcessContext context) {

            Properties props = new Properties();
            props.put(PulsarConsumerFactory.TOPIC_NAME, context.getProperty(TOPIC).getValue());
            props.put(PulsarConsumerFactory.SUBSCRIPTION_NAME, context.getProperty(SUBSCRIPTION).getValue());
            props.put(PulsarConsumerFactory.CONSUMER_CONFIG, getConsumerConfig(context));
            return props;
        }

        protected ConsumerConfiguration getConsumerConfig(ProcessContext context) {

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

        private class AsyncAcknowledgmentMonitorThread extends Thread {

            private ExecutorCompletionService<Void> ackService;

            public AsyncAcknowledgmentMonitorThread(final ExecutorCompletionService<Void> ackService) {
                 this.ackService = ackService;
            }

            @Override
            public void run() {
                 handleAsyncAcks();
            }

            protected void handleAsyncAcks() {
                 Future<Void> completed = null;
                 do {
                     try {
                            completed = ackService.poll(0, TimeUnit.SECONDS);
                        } catch (InterruptedException e) { /* Ignore these */ }
                 } while (completed != null);
            }
        }
}
