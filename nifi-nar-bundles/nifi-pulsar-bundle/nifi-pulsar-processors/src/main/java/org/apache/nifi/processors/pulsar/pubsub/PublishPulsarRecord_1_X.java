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

import static org.apache.nifi.processors.pulsar.pubsub.RecordBasedConst.RECORD_READER;
import static org.apache.nifi.processors.pulsar.pubsub.RecordBasedConst.RECORD_WRITER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

@Tags({"Apache", "Pulsar", "Record", "csv", "json", "avro", "logs", "Put", "Send", "Message", "PubSub", "1.0"})
@CapabilityDescription("Sends the contents of a FlowFile as individual records to Apache Pulsar using the Pulsar 1.x client API. "
    + "The contents of the FlowFile are expected to be record-oriented data that can be read by the configured Record Reader. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsarRecord_1_0.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
@SeeAlso({PublishPulsar_1_X.class, ConsumePulsar_1_X.class, ConsumePulsarRecord_1_X.class})
public class PublishPulsarRecord_1_X extends AbstractPulsarProducerProcessor {

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(TOPIC);
        properties.add(ASYNC_ENABLED);
        properties.add(MAX_ASYNC_REQUESTS);
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

        final FlowFile flowFile = session.get();
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

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                    .asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> attributes = flowFile.getAttributes();
        final AtomicLong messagesSent = new AtomicLong(0L);

        try {
            final InputStream in = new ByteArrayInputStream(messageContent);
            final RecordReader reader = readerFactory.createRecordReader(attributes, in, getLogger());
            final RecordSet recordSet = reader.createRecordSet();
            final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());
            final Producer producer = getWrappedProducer(topic, context).getProducer();

            if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
               InFlightMessageMonitor bundle = getInFlightMessages(writerFactory, schema, recordSet);
               this.sendAsync(producer, session, flowFile, bundle);
               handleAsync(bundle, session, flowFile, topic);
           } else {
               messagesSent.addAndGet(send(producer, writerFactory, schema, recordSet));
               session.putAttribute(flowFile, MSG_COUNT, messagesSent.get() + "");
               session.putAttribute(flowFile, TOPIC_NAME, topic);
               session.adjustCounter("Messages Sent", messagesSent.get(), true);
               session.getProvenanceReporter().send(flowFile, "Sent " + messagesSent.get() + " records to " + topic );
               session.transfer(flowFile, REL_SUCCESS);
           }
        } catch (final SchemaNotFoundException | MalformedRecordException | IOException e) {
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    private int send(final Producer producer, final RecordSetWriterFactory writerFactory, final RecordSchema schema, final RecordSet recordSet) throws IOException, SchemaNotFoundException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        int recordCount = 0;

        while ((record = recordSet.next()) != null) {
            recordCount++;
            baos.reset();

            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos)) {
                writer.write(record);
                writer.flush();
            }

            producer.send(baos.toByteArray());
        }

        return recordCount;
    }

    private InFlightMessageMonitor getInFlightMessages(RecordSetWriterFactory writerFactory, RecordSchema schema, RecordSet recordSet) throws IOException, SchemaNotFoundException {
        ArrayList<byte[]> records = new ArrayList<byte[]>();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;

        while ((record = recordSet.next()) != null) {
            baos.reset();

            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos)) {
                writer.write(record);
                writer.flush();
            }
            records.add(baos.toByteArray());
        }

        return new InFlightMessageMonitor(records);
    }

    /* Launches all of the async send requests
     *
     */
    protected void sendAsync(Producer producer, ProcessSession session, FlowFile flowFile, InFlightMessageMonitor monitor) {

        if (monitor == null || monitor.getRecords().isEmpty())
           return;

        for (byte[] record: monitor.getRecords() ) {
           try {

              publisherService.submit(new Callable<MessageId>() {
                @Override
                public MessageId call() throws Exception {
                  try {
                     return producer.sendAsync(record).handle((msgId, ex) -> {
                         if (msgId != null) {
                            monitor.getSuccessCounter().incrementAndGet();
                            return msgId;
                         } else {
                            monitor.getFailureCounter().incrementAndGet();
                            monitor.getFailures().add(record);
                            return null;
                         }
                     }).get();

                   } catch (final Throwable t) {
                      // This traps any exceptions thrown while calling the producer.sendAsync() method.
                      monitor.getFailureCounter().incrementAndGet();
                      monitor.getFailures().add(record);
                      return null;
                   } finally {
                      monitor.getLatch().countDown();
                   }
               }
             });
          } catch (final RejectedExecutionException ex) {
            // This can happen if the processor is being Unscheduled.
          }
       }
    }

    private void handleAsync(InFlightMessageMonitor monitor, ProcessSession session, FlowFile flowFile, String topic) {
       try {

           boolean useOriginalForFailures = false;
           monitor.getLatch().await();

           if (monitor.getSuccessCounter().intValue() > 0) {
               session.putAttribute(flowFile, MSG_COUNT, monitor.getSuccessCounter().get() + "");
               session.putAttribute(flowFile, TOPIC_NAME, topic);
               session.adjustCounter("Messages Sent", monitor.getSuccessCounter().get(), true);
               session.getProvenanceReporter().send(flowFile, "Sent " + monitor.getSuccessCounter().get() + " records to " + topic );
               session.transfer(flowFile, REL_SUCCESS);
           } else {
              // Route the original FlowFile to failure, otherwise we need to create a new FlowFile for the Failure relationship
              useOriginalForFailures = true;
           }

           if (monitor.getFailureCounter().intValue() > 0) {
              // Create a second flow file for failures.
              FlowFile failureFlowFile = useOriginalForFailures ? flowFile : session.create();

              StringBuffer sb = new StringBuffer();
              for (byte[] badRecord : monitor.getFailures()) {
                 sb.append(new String(badRecord)).append(System.lineSeparator());
              }

              failureFlowFile = session.write(failureFlowFile, out -> out.write(sb.toString().trim().getBytes()));
              session.putAttribute(failureFlowFile, MSG_COUNT, monitor.getFailureCounter().get() + "");
              session.transfer(failureFlowFile, REL_FAILURE);
           }

        } catch (InterruptedException e) {
          getLogger().error("Pulsar did not receive all async messages", e);
        }
    }
}
