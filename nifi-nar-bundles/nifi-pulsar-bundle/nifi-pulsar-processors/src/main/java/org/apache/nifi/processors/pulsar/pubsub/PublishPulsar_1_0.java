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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar 1.21 Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
public class PublishPulsar_1_0 extends AbstractPulsarProducerProcessor {

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        properties.add(ASYNC_ENABLED);
        properties.add(BATCHING_ENABLED);
        properties.add(BATCHING_MAX_MESSAGES);
        properties.add(BATCH_INTERVAL);
        properties.add(BLOCK_IF_QUEUE_FULL);
        properties.add(COMPRESSION_TYPE);
        properties.add(MESSAGE_ROUTING_MODE);
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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();

        if (flowFile == null)
            return;

        final ComponentLog logger = getLogger();
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();

        if (StringUtils.isBlank(topic)) {
            logger.error("Invalid topic specified {}", new Object[] {topic});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Read the contents of the FlowFile into a byte array
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });

        // Nothing to do, so skip this Flow file.
        if (messageContent == null || messageContent.length < 1) {
            session.transfer(flowFile, REL_SUCCESS);
            return;
        }

        try {

            Producer producer = getWrappedProducer(topic, context).getProducer();

            if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
                this.sendAsync(producer, session, flowFile, messageContent);
            } else {
                this.send(producer, session, flowFile, messageContent);
            }

        } catch (final PulsarClientException e) {
            logger.error("Failed to connect to Pulsar Server due to {}", new Object[]{e});
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }


    private void send(Producer producer, ProcessSession session, FlowFile flowFile, byte[] messageContent) throws PulsarClientException {

            MessageId msgId = producer.send(messageContent);

            if (msgId != null) {
                flowFile = session.putAttribute(flowFile, MSG_COUNT, "1");
                session.adjustCounter("Messages Sent", 1, true);
                session.getProvenanceReporter().send(flowFile, "Sent message " + msgId + " to " + producer.getTopic() );
                session.transfer(flowFile, REL_SUCCESS);

        } else {
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    private void sendAsync(Producer producer, ProcessSession session, FlowFile flowFile, byte[] messageContent) {

        producer.sendAsync(messageContent).handle((msgId, ex) -> {
            if (msgId != null) {
                return msgId;
            } else {
               // TODO Communicate the error back up to the onTrigger method so we can invalidate this producer.
               getLogger().warn("Problem ", ex);
               return null;
            }
        });

        flowFile = session.putAttribute(flowFile, MSG_COUNT, "1");
        session.adjustCounter("Messages Sent", 1, true);
        session.getProvenanceReporter().send(flowFile, "Sent async message to " + producer.getTopic() );
        session.transfer(flowFile, REL_SUCCESS);

    }

}
