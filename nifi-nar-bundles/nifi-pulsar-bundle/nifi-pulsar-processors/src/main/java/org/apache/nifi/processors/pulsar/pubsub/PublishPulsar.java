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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.StreamDemarcator;
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
        final boolean useDemarcator = context.getProperty(MESSAGE_DEMARCATOR).isSet();
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(25, DataUnit.MB, 500));

        Map<String, List<FlowFile>> sorted = sortFlowFiles(flowFiles, context, session);

        if (!context.getProperty(ASYNC_ENABLED).asBoolean()) {
            sorted.forEach((k,v)->{
                Producer<byte[]> producer = getProducer(context, k);
                if (producer != null) {
                   v.stream().forEach(flowFile->{
                      try {
                         if (useDemarcator) {
                            send(producer, session, flowFile,
                               context.getProperty(MESSAGE_DEMARCATOR)
                                  .evaluateAttributeExpressions(flowFile).getValue().getBytes(StandardCharsets.UTF_8));
                         } else {
                            send(producer, session, flowFile);
                         }
                      } catch (final PulsarClientException e) {
                        getLogger().error("Failed to connect to Pulsar Server due to {}", new Object[]{e});
                        session.transfer(flowFile, REL_FAILURE);
                      }
                   });
                }
            });
        } else {
            sorted.forEach((k,v)->{
                Producer<byte[]> producer = getProducer(context, k);
                if (producer != null) {
                   v.stream().forEach(flowFile->{
                      try {
                          InFlightMessageMonitor<byte[]> bundle = getInFlightMessages(session, flowFile,
                             useDemarcator ? context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions(flowFile)
                                .getValue().getBytes(StandardCharsets.UTF_8) : null);
                           sendAsync(producer, session, flowFile, bundle);
                           handleAsync(bundle, session, flowFile, k);
                      } catch (ProcessException | IOException e) {
                         getLogger().error("Failed to connect to Pulsar Server due to {}", new Object[]{e});
                         session.transfer(flowFile, REL_FAILURE);
                      }
                   });
                }
            });
        }
    }

    /**
     * Sort the given list of FlowFiles based on their destination TOPIC
     */
    private Map<String, List<FlowFile>> sortFlowFiles(List<FlowFile> flowFiles, ProcessContext context, ProcessSession session) {
        HashMap<String, List<FlowFile>> result = new HashMap<String, List<FlowFile>>();

        for (FlowFile ff: flowFiles) {
            String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(ff).getValue();

            if (StringUtils.isBlank(topic)) {
                getLogger().error("Invalid topic specified {}", new Object[] {topic});
                session.transfer(ff, REL_FAILURE);
                continue;
            }

            if (!result.containsKey(topic)) {
              result.put(topic, new ArrayList<FlowFile>());
            }
            result.get(topic).add(ff);
        }

        return result;
    }

    private InFlightMessageMonitor<byte[]> getInFlightMessages(ProcessSession session, FlowFile flowFile, byte[] demarcatorBytes) throws IOException {
        final InputStream in = session.read(flowFile);
        byte[] messageContent;
        ArrayList<byte[]> records = new ArrayList<byte[]>();

        if (demarcatorBytes != null && demarcatorBytes.length > 0) {
            final StreamDemarcator demarcator = new StreamDemarcator(in, demarcatorBytes, Integer.MAX_VALUE);
            while ((messageContent = demarcator.nextToken()) != null) {
               records.add(messageContent);
            }
            demarcator.close();
        } else {
            messageContent = new byte[(int) flowFile.getSize()];
            StreamUtils.fillBuffer(in, messageContent);
            records.add(messageContent);
        }

        in.close();
        return new InFlightMessageMonitor<byte[]>(records);
    }

    /**
     * Sends the entire FlowFile contents to Apache Pulsar as a single message.
     */
    private void send(Producer<byte[]> producer, ProcessSession session, FlowFile flowFile) throws PulsarClientException {
        final InputStream in = session.read(flowFile);
        try {
            byte[]  messageContent = new byte[(int) flowFile.getSize()];
            StreamUtils.fillBuffer(in, messageContent);
            MessageId msgId = producer.send(messageContent);

            if (msgId != null) {
                flowFile = session.putAttribute(flowFile, MSG_COUNT, "1");
                session.adjustCounter("Messages Sent", 1, true);
                session.getProvenanceReporter().send(flowFile, "Sent message " + msgId + " to " + getPulsarClientService().getPulsarBrokerRootURL() );
                session.transfer(flowFile, REL_SUCCESS);
            } else {
               session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final IOException ioEx) {
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            try {
               in.close();
            } catch (IOException e) {
               // Ignore these
            }
        }
    }

    private void send(Producer<byte[]> producer, ProcessSession session, FlowFile flowFile, byte[] demarcatorBytes) throws PulsarClientException {
        final InputStream in = session.read(flowFile);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger failureCounter = new AtomicInteger(0);
        byte[] messageContent;

        try (final StreamDemarcator demarcator = new StreamDemarcator(in, demarcatorBytes, Integer.MAX_VALUE)) {
           while ((messageContent = demarcator.nextToken()) != null) {
              if (producer.send(messageContent) != null) {
                 successCounter.incrementAndGet();
              } else {
                 failureCounter.incrementAndGet();
                 break;  // Quit sending messages if we encounter a failure.
              }
            }

            if (failureCounter.intValue() < 1) {
               session.adjustCounter("Messages Sent", successCounter.get(), true);
               session.getProvenanceReporter().send(flowFile, "Sent " + successCounter.get() + " messages to " + getPulsarClientService().getPulsarBrokerRootURL() );
               session.transfer(flowFile, REL_SUCCESS);
            } else {
               session.transfer(flowFile, REL_FAILURE);
            }
        } catch (final IOException ioEx) {
            getLogger().error("Unable to publish message to Pulsar broker " + getPulsarClientService().getPulsarBrokerRootURL(), ioEx);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
           try {
             in.close();
           } catch (IOException e) {
             // Ignore these
           }
        }
    }
}
