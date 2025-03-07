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
package org.apache.nifi.processors.gcp.pubsub.consume;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.gcp.pubsub.ConsumeGCPubSub;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ACK_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SERIALIZED_SIZE_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE;

public abstract class AbstractPubSubMessageConverter implements PubSubMessageConverter {

    protected final RecordReaderFactory readerFactory;
    protected final RecordSetWriterFactory writerFactory;
    protected final ComponentLog logger;

    protected AbstractPubSubMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final List<ReceivedMessage> messages, final List<String> ackIds, final String subscriptionName) {
        try {
            final Map<RecordSchema, RecordGroup> recordGroups = new HashMap<>();
            final Map<String, String> attributes = new HashMap<>();

            for (ReceivedMessage message : messages) {

                if (message.hasMessage()) {
                    byte[] payload = message.getMessage().getData().toByteArray();
                    try (final InputStream in = new ByteArrayInputStream(payload);
                            final RecordReader valueRecordReader = readerFactory.createRecordReader(attributes, in, payload.length, logger)) {

                        Record record;
                        while ((record = valueRecordReader.nextRecord()) != null) {

                            final RecordSchema recordSchema = record.getSchema();
                            final RecordSchema writeSchema = writerFactory.getSchema(attributes, getRecordSchema(recordSchema));

                            // Get/Register the Record Group is associated with the schema for this message
                            final RecordGroup recordGroup = recordGroups.computeIfAbsent(writeSchema, schema -> {
                                final FlowFile flowFile = session.create();
                                try {
                                    final OutputStream out = session.write(flowFile);
                                    final RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, out, attributes);
                                    writer.beginRecordSet();
                                    return new RecordGroup(flowFile, writer);
                                } catch (Exception e) {
                                    session.remove(flowFile);
                                    throw new ProcessException("Failed to create RecordSetWriter", e);
                                }
                            });

                            // Create the Record object and write it to the Record Writer.
                            recordGroup.writer().write(getRecord(record, message));
                        }

                    } catch (final MalformedRecordException | IOException | SchemaNotFoundException e) {
                        logger.error("Failed to parse the record. Transfer to a 'parse.failure' relationship", e);
                        handleParseFailure(session, message, payload, ackIds, subscriptionName);
                        continue;
                    }
                }

                // Track the ack ID for the message
                ackIds.add(message.getAckId());
            }

            // Finish writing the records
            finishRecordGroups(session, recordGroups, subscriptionName);

        } catch (final Exception e) {
            throw new ProcessException("FlowFile Record conversion failed", e);
        }
    }

    protected abstract Record getRecord(Record record, ReceivedMessage message);

    protected abstract RecordSchema getRecordSchema(RecordSchema recordSchema);

    private void handleParseFailure(final ProcessSession session, final ReceivedMessage message, final byte[] payload,
            final List<String> ackIds, final String subscriptionName) {
        // Failed to parse the record. Transfer to a 'parse.failure' relationship
        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, message.getMessage().getAttributesMap());
        flowFile = session.putAttribute(flowFile, ACK_ID_ATTRIBUTE, message.getAckId());
        flowFile = session.putAttribute(flowFile, SERIALIZED_SIZE_ATTRIBUTE, String.valueOf(message.getSerializedSize()));
        flowFile = session.putAttribute(flowFile, MESSAGE_ID_ATTRIBUTE, message.getMessage().getMessageId());
        flowFile = session.putAttribute(flowFile, MSG_ATTRIBUTES_COUNT_ATTRIBUTE, String.valueOf(message.getMessage().getAttributesCount()));
        flowFile = session.putAttribute(flowFile, MSG_PUBLISH_TIME_ATTRIBUTE, String.valueOf(message.getMessage().getPublishTime().getSeconds()));
        flowFile = session.putAttribute(flowFile, SUBSCRIPTION_NAME_ATTRIBUTE, subscriptionName);
        flowFile = session.write(flowFile, out -> out.write(payload));
        session.transfer(flowFile, ConsumeGCPubSub.REL_PARSE_FAILURE);
        session.adjustCounter("Records Received from " + subscriptionName, 1, false);

        // Track the ack ID for the message
        ackIds.add(message.getAckId());
    }

    private void finishRecordGroups(ProcessSession session, Map<RecordSchema, RecordGroup> recordGroups, String subscriptionName) {
        for (final RecordGroup recordGroup : recordGroups.values()) {
            final Map<String, String> newAttributes;
            final int recordCount;
            try (final RecordSetWriter writer = recordGroup.writer()) {
                final WriteResult writeResult = writer.finishRecordSet();
                newAttributes = new HashMap<>(writeResult.getAttributes());
                newAttributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                newAttributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                newAttributes.put(SUBSCRIPTION_NAME_ATTRIBUTE, subscriptionName);
                recordCount = writeResult.getRecordCount();
            } catch (IOException e) {
                throw new ProcessException("Failed to finish writing records", e);
            }

            FlowFile flowFile = recordGroup.flowFile();
            flowFile = session.putAllAttributes(flowFile, newAttributes);
            final ProvenanceReporter provenanceReporter = session.getProvenanceReporter();
            provenanceReporter.receive(flowFile, subscriptionName);
            session.transfer(flowFile, ConsumeGCPubSub.REL_SUCCESS);
            session.adjustCounter("Records Received from " + subscriptionName, recordCount, false);
        }
    }

    private record RecordGroup(FlowFile flowFile, RecordSetWriter writer) {
    }

}
