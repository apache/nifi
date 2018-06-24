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

import java.io.ByteArrayOutputStream;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.util.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

@SeeAlso(ConsumePulsar.class)
@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar Producer API."
    + "The messages to send may be individual FlowFiles or may be delimited, using a "
    + "user-specified delimiter, such as a new-line. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
public class PublishPulsar extends AbstractPulsarProducerProcessor<byte[]> {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();

        if (StringUtils.isBlank(topic)) {
            getLogger().error("Invalid topic specified {}", new Object[] {topic});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Read the contents of the FlowFile into a byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);

        final byte[] messageContent = baos.toByteArray();
        // Nothing to do, so skip this Flow file.
        if (messageContent == null || messageContent.length < 1) {
            session.putAttribute(flowFile, "msg.count", "0");
            session.transfer(flowFile, REL_SUCCESS);
            return;
        }

        try {
            Producer<byte[]> producer = getProducer(context, topic);

            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                sendAsync(producer, session, messageContent);
                session.remove(flowFile);
            } else {
                send(producer, session, flowFile, messageContent);
            }
        } catch (final PulsarClientException e) {
            getLogger().error("Failed to connect to Pulsar Server due to {}", new Object[]{e});
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void send(Producer<byte[]> producer, ProcessSession session, FlowFile flowFile, byte[] messageContent) throws PulsarClientException {
        MessageId msgId = producer.send(messageContent);

        if (msgId != null) {
            flowFile = session.putAttribute(flowFile, MSG_COUNT, "1");
            session.adjustCounter("Messages Sent", 1, true);
            session.getProvenanceReporter().send(flowFile, "Sent message " + msgId + " to " + getPulsarClientService().getPulsarBrokerRootURL() );
            session.transfer(flowFile, REL_SUCCESS);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
