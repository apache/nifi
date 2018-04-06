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
package org.apache.nifi.processors.pulsar.pubsub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessor;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar "
        + "The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ConsumePulsar_1_X extends AbstractPulsarProcessor {

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

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    // Reuse the same consumer for a given topic / subscription
    private PulsarConsumer consumer;
    private ConsumerConfiguration consumerConfig;

    // Pool for running multiple consume Async requests
    ExecutorService pool;
    ExecutorCompletionService<Message> completionService;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        properties.add(SUBSCRIPTION);
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
        pool = Executors.newFixedThreadPool(context.getProperty(MAX_ASYNC_REQUESTS).asInteger());
        completionService = new ExecutorCompletionService<>(pool);
    }

    @OnUnscheduled
    public void shutDown() {
        // Stop all the async consumers
        pool.shutdownNow();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {
            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                // Launch consumers
                consumeAsync(context, session);

                // Handle completed consumers
                handleAsync(context, session);

            } else {
                consume(context, session);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }

    }

    private void handleAsync(ProcessContext context, ProcessSession session) {

        try {
            Future<Message> done = completionService.take();
            Message msg = done.get();

            if (msg != null) {
                FlowFile flowFile = null;
                final byte[] value = msg.getData();
                if (value != null && value.length > 0) {
                    flowFile = session.create();
                    flowFile = session.write(flowFile, out -> {
                        out.write(value);
                    });

                   session.getProvenanceReporter().receive(flowFile, "From " + getWrappedConsumer(context).getTransitURL());
                   session.transfer(flowFile, REL_SUCCESS);
                   session.commit();
                   getWrappedConsumer(context).getConsumer().acknowledgeAsync(msg);
                }
            }

        } catch (InterruptedException | ExecutionException | PulsarClientException e) {
            getLogger().error("Trouble consuming messages ", e);
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
    private void consumeAsync(ProcessContext context, ProcessSession session) throws PulsarClientException {

        Consumer consumer = getWrappedConsumer(context).getConsumer();

        completionService.submit(new Callable<Message>() {
            @Override
            public Message call() throws Exception {
                return consumer.receiveAsync().get();
            }
        });

    }

    /*
     * When this Processor expects to receive many small files, it may
     * be advisable to create several FlowFiles from a single session
     * before committing the session. Typically, this allows the Framework
     * to treat the content of the newly created FlowFiles much more efficiently.
     */
    private void consume(ProcessContext context, ProcessSession session) throws PulsarClientException {

        Consumer consumer = getWrappedConsumer(context).getConsumer();

        final ComponentLog logger = getLogger();
        final Message msg;
        FlowFile flowFile = null;

        try {

            msg = consumer.receive();
            final byte[] value = msg.getData();

            if (value != null && value.length > 0) {
                flowFile = session.create();
                flowFile = session.write(flowFile, out -> {
                    out.write(value);
                });

                session.getProvenanceReporter().receive(flowFile, "From " + context.getProperty(TOPIC).getValue());
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                        new Object[]{flowFile, 1});

                session.commit();

                /*
                 * This Processor acknowledges receipt of the data and/or removes the data
                 * from the external source in order to prevent receipt of duplicate files.
                 * This is done only after the ProcessSession by which the FlowFile was created
                 * has been committed! Failure to adhere to this principle may result in data
                 * loss, as restarting NiFi before the session has been committed will result
                 * in the temporary file being deleted. Note, however, that it is possible using
                 * this approach to receive duplicate data because the application could be
                 * restarted after committing the session and before acknowledging or removing
                 * the data from the external source. In general, though, potential data duplication
                 * is preferred over potential data loss.
                 */
                getLogger().info("Acknowledging message " + msg.getMessageId());
                consumer.acknowledge(msg);

            } else {
                // We didn't consume any data, so
                session.commit();
            }

        } catch (PulsarClientException e) {
            context.yield();
            session.rollback();
        }

    }

    private PulsarConsumer getWrappedConsumer(ProcessContext context) throws PulsarClientException {

        if (consumer != null) {
            return consumer;
        }

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
