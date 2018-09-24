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


import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;

@CapabilityDescription("Consumes messages from Apache Pulsar"
        + "The complementary NiFi processor for sending messages is PublishPulsarRecord. Please note that, at this time, "
        + "the Processor assumes that all records that are retrieved have the same schema. If any of the Pulsar messages "
        + "that are pulled but cannot be parsed or written with the configured Record Reader or Record Writer, the contents "
        + "of the message will be written to a separate FlowFile, and that FlowFile will be transferred to the 'parse.failure' "
        + "relationship. Otherwise, each FlowFile is sent to the 'success' relationship and may contain many individual "
        + "messages within the single FlowFile. A 'record.count' attribute is added to indicate how many messages are contained in the "
        + "FlowFile. No two Pulsar messages will be placed into the same FlowFile if they have different schemas.")
@Tags({"Pulsar", "Get", "Record", "csv", "avro", "json", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@WritesAttributes({
    @WritesAttribute(attribute = "record.count", description = "The number of records received")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SeeAlso({PublishPulsar.class, ConsumePulsar.class, PublishPulsarRecord.class})
public class ConsumePulsarRecord extends AbstractPulsarConsumerProcessor<byte[]> {

    public static final String MSG_COUNT = "record.count";

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot read the data it receives from Pulsar will be routed to this Relationship")
            .build();


    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to Pulsar")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Async Requests")
            .description("The number of records to combine into a single flow file.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a Pulsar consumer to poll a subscription for data "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("2 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse_failure")
            .description("FlowFiles for which the content was not prasable")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(BATCH_SIZE);
        properties.add(MAX_WAIT_TIME);
        properties.addAll(AbstractPulsarConsumerProcessor.PROPERTIES);
        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_PARSE_FAILURE);
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
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        try {
            Consumer<byte[]> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
               consumeAsync(consumer, context, session);
               handleAsync(context, session, consumer, readerFactory, writerFactory);
            } else {
               consumeMessage(session, consumer, consumer.receive(), readerFactory, writerFactory);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

    private void consumeMessage(ProcessSession session, final Consumer<byte[]> consumer, final Message<byte[]> msg,
            final RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory) throws PulsarClientException {
        consumeMessage(session, consumer, msg, false, readerFactory, writerFactory);
    }

    /**
     * Consumes a single message, parses it, and returns.
     */
    private void consumeMessage(ProcessSession session, final Consumer<byte[]> consumer, final Message<byte[]> msg, boolean async,
         final RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory) throws PulsarClientException {

       final AtomicLong messagesReceived = new AtomicLong(0L);
       RecordSetWriter writer = null;
       FlowFile flowFile = session.create();
       OutputStream rawOut = null;
       RecordReader reader = getRecordReader(msg, readerFactory, session);
       Record firstRecord = getFirstRecord(msg, reader, session);

       // We have a message that we cannot parse.
       if (firstRecord == null) {
            if (!async) {
              consumer.acknowledge(msg);
            } else {
             getAckService().submit(new Callable<Object>() {
                 @Override
                 public Object call() throws Exception {
                    return consumer.acknowledgeAsync(msg).get();
                 }
              });
            }
          session.transfer(flowFile, REL_PARSE_FAILURE);
          return;
       }

       // Create the Record Writer
       try {
          rawOut = session.write(flowFile);
          writer = getRecordWriter(writerFactory, firstRecord.getSchema(), rawOut);
          writer.beginRecordSet();
       } catch (SchemaNotFoundException | IOException ex) {
          getLogger().error("Failed to obtain Schema for FlowFile.", ex);
          throw new ProcessException(ex);
       }

       // Read all the records from this message, as it may contain several
       try {
          for (Record record = firstRecord; record != null; record = reader.nextRecord()) {
              writer.write(record);
              messagesReceived.incrementAndGet();
          }
       } catch (MalformedRecordException | IOException mEx) {
          handleParseFailure(msg, mEx, session);
       } finally {
            if (!async) {
               consumer.acknowledge(msg);
            } else {
               getAckService().submit(new Callable<Object>() {
                   @Override
                   public Object call() throws Exception {
                      return consumer.acknowledgeAsync(msg).get();
                   }
               });
            }
       }

       // Clean-up and transfer session
       try {
          if (rawOut != null) {
            rawOut.close();
          }

          if (writer != null) {
            WriteResult result = writer.finishRecordSet();
            session.putAllAttributes(flowFile, result.getAttributes());
          }

       } catch (IOException e1) {
         getLogger().error("Error cleaning up", e1);
       }

       if (flowFile != null) {
          session.putAttribute(flowFile, MSG_COUNT, messagesReceived.toString());
          session.transfer(flowFile, REL_SUCCESS);
       }
    }

    /**
     * Pull messages off of the CompletableFuture's held in the consumerService, parse them
     * and create a new Flowfile for each one.
     */
    protected void handleAsync(ProcessContext context, ProcessSession session, final Consumer<byte[]> consumer,
         final RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory) throws PulsarClientException {

        final Integer queryTimeout = context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();

        try {
             Future<Message<byte[]>> done = null;
             do {
                 done = getConsumerService().poll(queryTimeout, TimeUnit.SECONDS);

                 if (done != null) {
                    Message<byte[]> msg = done.get();
                    if (msg != null) {
                      consumeMessage(session, consumer, msg, true, readerFactory, writerFactory);
                    }
                 }
             } while (done != null);

        } catch (InterruptedException | ExecutionException e) {
            getLogger().error("Trouble consuming messages ", e);
        }
    }

    private Record getFirstRecord(Message<byte[]> msg, RecordReader reader, ProcessSession session) {
        Record firstRecord = null;

        try {
            firstRecord = reader.nextRecord();
        } catch (IOException | MalformedRecordException ex) {
            handleParseFailure(msg, ex, session);
        }
        return firstRecord;
    }

    private RecordReader getRecordReader(Message<byte[]> msg, RecordReaderFactory readerFactory, ProcessSession session) {
        RecordReader reader = null;
        final byte[] recordBytes = msg.getData() == null ? new byte[0] : msg.getData();

        try (final InputStream in = new ByteArrayInputStream(recordBytes)) {
            reader = readerFactory.createRecordReader(Collections.emptyMap(), in, getLogger());
        } catch (MalformedRecordException | IOException | SchemaNotFoundException ex) {
            handleParseFailure(msg, ex, session);
        }
        return reader;
    }

    private RecordSetWriter getRecordWriter(RecordSetWriterFactory writerFactory, RecordSchema srcSchema, OutputStream out) throws SchemaNotFoundException, IOException {
        RecordSchema writeSchema = writerFactory.getSchema(Collections.emptyMap(), srcSchema);
        return writerFactory.createWriter(getLogger(), writeSchema, out);
    }

    private void handleParseFailure(Message<byte[]> msg, Exception ex, ProcessSession session) {
        FlowFile failureFlowFile = session.create();
        if (msg.getData() != null) {
           failureFlowFile = session.write(failureFlowFile, out -> out.write(msg.getData()));
        }
        session.transfer(failureFlowFile, REL_PARSE_FAILURE);
    }
}