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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar "
        + "The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ConsumePulsar_1_0 extends AbstractPulsarConsumerProcessor {

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {
            if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
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
                    }

                    session.getProvenanceReporter().receive(flowFile, "From " + context.getProperty(TOPIC).getValue());
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commit();
                    getWrappedConsumer(context).getConsumer().acknowledgeAsync(msg);
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

}
