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
package org.apache.nifi.processors.jms;

import static org.apache.nifi.processors.jms.util.JmsProperties.ACKNOWLEDGEMENT_MODE;
import static org.apache.nifi.processors.jms.util.JmsProperties.ACK_MODE_CLIENT;
import static org.apache.nifi.processors.jms.util.JmsProperties.BATCH_SIZE;
import static org.apache.nifi.processors.jms.util.JmsProperties.CLIENT_ID_PREFIX;
import static org.apache.nifi.processors.jms.util.JmsProperties.DESTINATION_NAME;
import static org.apache.nifi.processors.jms.util.JmsProperties.JMS_PROPS_TO_ATTRIBUTES;
import static org.apache.nifi.processors.jms.util.JmsProperties.JMS_PROVIDER;
import static org.apache.nifi.processors.jms.util.JmsProperties.MESSAGE_SELECTOR;
import static org.apache.nifi.processors.jms.util.JmsProperties.PASSWORD;
import static org.apache.nifi.processors.jms.util.JmsProperties.TIMEOUT;
import static org.apache.nifi.processors.jms.util.JmsProperties.URL;
import static org.apache.nifi.processors.jms.util.JmsProperties.USERNAME;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.BufferedOutputStream;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.jms.util.JmsFactory;
import org.apache.nifi.processors.jms.util.WrappedMessageConsumer;
import org.apache.nifi.util.BooleanHolder;
import org.apache.nifi.util.IntegerHolder;
import org.apache.nifi.util.LongHolder;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.StopWatch;

public abstract class JmsConsumer extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles are routed to success").build();

    private final Set<Relationship> relationships;
    private final List<PropertyDescriptor> propertyDescriptors;

    public JmsConsumer() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(JMS_PROVIDER);
        descriptors.add(URL);
        descriptors.add(DESTINATION_NAME);
        descriptors.add(TIMEOUT);
        descriptors.add(BATCH_SIZE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(ACKNOWLEDGEMENT_MODE);
        descriptors.add(MESSAGE_SELECTOR);
        descriptors.add(JMS_PROPS_TO_ATTRIBUTES);
        descriptors.add(CLIENT_ID_PREFIX);
        this.propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    public void consume(final ProcessContext context, final ProcessSession session, final WrappedMessageConsumer wrappedConsumer) throws ProcessException {
        final ProcessorLog logger = getLogger();

        final MessageConsumer consumer = wrappedConsumer.getConsumer();
        final boolean clientAcknowledge = context.getProperty(ACKNOWLEDGEMENT_MODE).getValue().equalsIgnoreCase(ACK_MODE_CLIENT);
        final long timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean addAttributes = context.getProperty(JMS_PROPS_TO_ATTRIBUTES).asBoolean();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        final ObjectHolder<Message> lastMessageReceived = new ObjectHolder<>(null);
        final ObjectHolder<Map<String, String>> attributesFromJmsProps = new ObjectHolder<>(null);
        final Set<FlowFile> allFlowFilesCreated = new HashSet<>();
        final IntegerHolder messagesReceived = new IntegerHolder(0);
        final LongHolder bytesReceived = new LongHolder(0L);

        final StopWatch stopWatch = new StopWatch(true);
        for (int i = 0; i < batchSize; i++) {
            final BooleanHolder failure = new BooleanHolder(false);

            final Message message;
            try {
                // If we haven't received a message, wait until one is available. If we have already received at least one
                // message, then we are not willing to wait for more to become available, but we are willing to keep receiving
                // all messages that are immediately available.
                if (messagesReceived.get() == 0) {
                    message = consumer.receive(timeout);
                } else {
                    message = consumer.receiveNoWait();
                }
            } catch (final JMSException e) {
                logger.error("Failed to receive JMS Message due to {}", e);
                wrappedConsumer.close(logger);
                failure.set(true);
                break;
            }

            if (message == null) {    // if no messages, we're done
                break;
            }

            final IntegerHolder msgsThisFlowFile = new IntegerHolder(0);
            FlowFile flowFile = session.create();
            try {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream rawOut) throws IOException {
                        try (final OutputStream out = new BufferedOutputStream(rawOut, 65536)) {
                            messagesReceived.getAndIncrement();
                            final Map<String, String> attributes = (addAttributes ? JmsFactory.createAttributeMap(message) : null);
                            attributesFromJmsProps.set(attributes);

                            final byte[] messageBody = JmsFactory.createByteArray(message);
                            out.write(messageBody);
                            bytesReceived.addAndGet(messageBody.length);
                            msgsThisFlowFile.incrementAndGet();
                            lastMessageReceived.set(message);
                        } catch (final JMSException e) {
                            logger.error("Failed to receive JMS Message due to {}", e);
                            failure.set(true);
                        }
                    }
                });
            } finally {
                if (failure.get()) {    // no flowfile created
                    session.remove(flowFile);
                    wrappedConsumer.close(logger);
                } else {
                    allFlowFilesCreated.add(flowFile);

                    final Map<String, String> attributes = attributesFromJmsProps.get();
                    if (attributes != null) {
                        flowFile = session.putAllAttributes(flowFile, attributes);
                    }

                    session.getProvenanceReporter().receive(flowFile, context.getProperty(URL).getValue());
                    session.transfer(flowFile, REL_SUCCESS);
                    logger.info("Created {} from {} messages received from JMS Server and transferred to 'success'", new Object[]{flowFile, msgsThisFlowFile.get()});
                }
            }
        }

        if (allFlowFilesCreated.isEmpty()) {
            context.yield();
            return;
        }

        session.commit();

        stopWatch.stop();
        if (!allFlowFilesCreated.isEmpty()) {
            final float secs = ((float) stopWatch.getDuration(TimeUnit.MILLISECONDS) / 1000F);
            float messagesPerSec = ((float) messagesReceived.get()) / secs;
            final String dataRate = stopWatch.calculateDataRate(bytesReceived.get());
            logger.info("Received {} messages in {} milliseconds, at a rate of {} messages/sec or {}", new Object[]{messagesReceived.get(), stopWatch.getDuration(TimeUnit.MILLISECONDS), messagesPerSec, dataRate});
        }

        // if we need to acknowledge the messages, do so now.
        final Message lastMessage = lastMessageReceived.get();
        if (clientAcknowledge && lastMessage != null) {
            try {
                lastMessage.acknowledge();  // acknowledge all received messages by acknowledging only the last.
            } catch (final JMSException e) {
                logger.error("Failed to acknowledge {} JMS Message(s). This may result in duplicate messages. Reason for failure: {}", new Object[]{messagesReceived.get(), e});
            }
        }
    }
}
