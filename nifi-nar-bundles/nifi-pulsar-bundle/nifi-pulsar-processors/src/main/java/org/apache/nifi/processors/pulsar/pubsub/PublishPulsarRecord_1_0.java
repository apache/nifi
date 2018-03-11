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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
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
import org.apache.nifi.util.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_READER;
import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_WRITER;

@Tags({"Apache", "Pulsar", "Record", "csv", "json", "avro", "logs", "Put", "Send", "Message", "PubSub", "1.0"})
@CapabilityDescription("Sends the contents of a FlowFile as individual records to Apache Pulsar using the Pulsar 1.x client API. "
    + "The contents of the FlowFile are expected to be record-oriented data that can be read by the configured Record Reader. "
    + "The complementary NiFi processor for fetching messages is ConsumePulsarRecord_1_0.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Pulsar for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
public class PublishPulsarRecord_1_0 extends AbstractPulsarProducerProcessor {

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
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

        final AtomicLong messagesSent = new AtomicLong(0L);

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

            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                    .asControllerService(RecordSetWriterFactory.class);

            final Map<String, String> attributes = flowFile.getAttributes();
            boolean error = false;

            try {
             session.read(flowFile, new InputStreamCallback() {
                 @Override
                 public void process(final InputStream rawIn) throws IOException {
                     try (final InputStream in = new BufferedInputStream(rawIn)) {
                         final RecordReader reader = readerFactory.createRecordReader(attributes, in, getLogger());
                         final RecordSet recordSet = reader.createRecordSet();
                         final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());
                         final Producer producer = getWrappedProducer(topic, context).getProducer();

                         if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
                               messagesSent.addAndGet(sendAsync(producer, writerFactory, schema, recordSet));
                         } else {
                               messagesSent.addAndGet(send(producer, writerFactory, schema, recordSet));
                         }

                      } catch (final SchemaNotFoundException | MalformedRecordException e) {
                         throw new ProcessException(e);
                      }
                }
             });
         } catch (final Exception e) {
            error = true;
         }

             if (error) {
                 session.transfer(flowFile, REL_FAILURE);
             } else {
               session.putAttribute(flowFile, MSG_COUNT, messagesSent.get() + "");
               session.adjustCounter("Messages Sent", messagesSent.get(), true);
               session.getProvenanceReporter().send(flowFile, "Sent " + messagesSent.get() + " records to " + topic );
               session.transfer(flowFile, REL_SUCCESS);
             }

    }

    private int send(final Producer producer, final RecordSetWriterFactory writerFactory, final RecordSchema schema, final RecordSet recordSet) throws IOException, SchemaNotFoundException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        int recordCount = 0;

        try {
            while ((record = recordSet.next()) != null) {
                recordCount++;
                baos.reset();

                try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos)) {
                    writer.write(record);
                    writer.flush();
                }

                    producer.send(baos.toByteArray());
            }
        } catch (final PulsarClientException ex) {
                producer.close();
        }

        return recordCount;
    }

    private int sendAsync(final Producer producer, final RecordSetWriterFactory writerFactory, final RecordSchema schema, final RecordSet recordSet) throws IOException, SchemaNotFoundException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        int recordCount = 0;

        try {
            while ((record = recordSet.next()) != null) {
                recordCount++;
                baos.reset();

                try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos)) {
                    writer.write(record);
                    writer.flush();
                }

                producer.sendAsync(baos.toByteArray()).handle((msgId, ex) -> {
                    if (msgId != null) {
                       return msgId;
                    } else {
                      try {
                        producer.close();
                      } catch (PulsarClientException e) {
                        e.printStackTrace();
                      }
                      return null;
                   }
               });
           }
        } catch (final PulsarClientException ex) {
           producer.close();
        }

        return recordCount;
    }
}
