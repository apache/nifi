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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;

@SeeAlso({PublishPulsar.class, ConsumePulsarRecord.class, PublishPulsarRecord.class})
@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar. The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "message.count", description = "The number of messages received from Pulsar")
})
public class ConsumePulsar extends AbstractPulsarConsumerProcessor<byte[]> {

    public static final String MSG_COUNT = "message.count";

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            Consumer<byte[]> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (consumer == null) {
                context.yield();
                return;
            }

            if (context.getProperty(ASYNC_ENABLED).asBoolean()) {
                consumeAsync(consumer, context, session);
                handleAsync(consumer, context, session);
            } else {
                consume(consumer, context, session);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

    private void handleAsync(final Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) {
        try {
            Future<List<Message<byte[]>>> done = getConsumerService().poll(5, TimeUnit.SECONDS);

            if (done != null) {

                final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;

                List<Message<byte[]>> messages = done.get();

                if (CollectionUtils.isNotEmpty(messages)) {
                    FlowFile flowFile = session.create();
                    OutputStream out = session.write(flowFile);
                    AtomicInteger msgCount = new AtomicInteger(0);

                    messages.forEach(msg -> {
                        try {
                            out.write(msg.getValue());
                            out.write(demarcatorBytes);
                            msgCount.getAndIncrement();
                        } catch (final IOException ioEx) {
                            session.rollback();
                            return;
                        }
                    });

                    IOUtils.closeQuietly(out);

                    session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                    session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commit();
                }
                // Acknowledge consuming the message
                getAckService().submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                       return consumer.acknowledgeCumulativeAsync(messages.get(messages.size()-1)).get();
                    }
                });
            }
        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        }
    }

    private void consume(Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
            final int maxMessages = context.getProperty(CONSUMER_BATCH_SIZE).isSet() ? context.getProperty(CONSUMER_BATCH_SIZE)
                    .evaluateAttributeExpressions().asInteger() : Integer.MAX_VALUE;

            final byte[] demarcatorBytes = context.getProperty(MESSAGE_DEMARCATOR).isSet() ? context.getProperty(MESSAGE_DEMARCATOR)
                    .evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8) : null;

            FlowFile flowFile = session.create();
            OutputStream out = session.write(flowFile);
            Message<byte[]> msg = null;
            Message<byte[]> lastMsg = null;
            AtomicInteger msgCount = new AtomicInteger(0);
            AtomicInteger loopCounter = new AtomicInteger(0);

            while (((msg = consumer.receive(0, TimeUnit.SECONDS)) != null) && loopCounter.get() < maxMessages) {
                try {

                    lastMsg = msg;
                    loopCounter.incrementAndGet();

                    // Skip empty messages, as they cause NPE's when we write them to the OutputStream
                    if (msg.getValue() == null || msg.getValue().length < 1) {
                      continue;
                    }
                    out.write(msg.getValue());
                    out.write(demarcatorBytes);
                    msgCount.getAndIncrement();

                } catch (final IOException ioEx) {
                  session.rollback();
                  return;
                }
            }

            IOUtils.closeQuietly(out);

            if (lastMsg != null)  {
                consumer.acknowledgeCumulative(lastMsg);
            }

            if (msgCount.get() < 1) {
                session.remove(flowFile);
                session.commit();
            } else {
                session.putAttribute(flowFile, MSG_COUNT, msgCount.toString());
                session.getProvenanceReporter().receive(flowFile, getPulsarClientService().getPulsarBrokerRootURL() + "/" + consumer.getTopic());
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                   new Object[]{flowFile, msgCount.toString()});
            }

        } catch (PulsarClientException e) {
            context.yield();
            session.rollback();
        }
    }
}