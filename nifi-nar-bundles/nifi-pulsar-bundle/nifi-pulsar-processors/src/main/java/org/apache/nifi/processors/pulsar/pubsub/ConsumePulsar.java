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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
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

@SeeAlso(PublishPulsar.class)
@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar "
        + "The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ConsumePulsar extends AbstractPulsarConsumerProcessor<byte[]> {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            Consumer<byte[]> consumer = getConsumer(context, getConsumerId(context, session.get()));

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
            Future<Message<byte[]>> done = getConsumerService().poll(50, TimeUnit.MILLISECONDS);

            if (done != null) {
               Message<byte[]> msg = done.get();

               if (msg != null) {
                  FlowFile flowFile = null;
                  final byte[] value = msg.getData();
                  if (value != null && value.length > 0) {
                      flowFile = session.create();
                      flowFile = session.write(flowFile, out -> {
                          out.write(value);
                      });

                     session.getProvenanceReporter().receive(flowFile, "From " + getPulsarClientService().getPulsarBrokerRootURL());
                     session.transfer(flowFile, REL_SUCCESS);
                     session.commit();
                  }
                  // Acknowledge consuming the message
                  getAckService().submit(new Callable<Object>() {
                      @Override
                      public Object call() throws Exception {
                         return consumer.acknowledgeAsync(msg).get();
                      }
                   });
              }
            } else {
               // Check if the processor is stopped and if so gracefully stop processing.
            }

        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        }
    }

    private void consume(Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
            final Message<byte[]> msg = consumer.receive();
            final byte[] value = msg.getData();

            if (value != null && value.length > 0) {
                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, out -> {
                    out.write(value);
                });

                session.getProvenanceReporter().receive(flowFile, "From " + getPulsarClientService().getPulsarBrokerRootURL());
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().debug("Created {} from {} messages received from Pulsar Server and transferred to 'success'",
                        new Object[]{flowFile, 1});

                session.commit();
                getLogger().debug("Acknowledging message " + msg.getMessageId());

            } else {
                session.commit();
            }
            consumer.acknowledge(msg);

        } catch (PulsarClientException e) {
            context.yield();
            session.rollback();
        }
    }
}
