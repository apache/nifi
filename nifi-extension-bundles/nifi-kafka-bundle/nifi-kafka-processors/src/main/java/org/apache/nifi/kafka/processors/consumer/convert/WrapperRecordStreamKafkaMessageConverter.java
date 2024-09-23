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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.wrapper.ConsumeWrapperRecord;
import org.apache.nifi.kafka.processors.consumer.wrapper.WrapperRecordKeyReader;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class WrapperRecordStreamKafkaMessageConverter implements KafkaMessageConverter {
    private static final RecordSchema EMPTY_SCHEMA = new SimpleRecordSchema(List.of());

    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory keyReaderFactory;
    private final Charset headerEncoding;
    private final Pattern headerNamePattern;
    private final KeyFormat keyFormat;
    private final KeyEncoding keyEncoding;
    private final boolean commitOffsets;
    private final OffsetTracker offsetTracker;
    private final Runnable onSuccess;
    private final ComponentLog logger;

    public WrapperRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final RecordReaderFactory keyReaderFactory,
            final Charset headerEncoding,
            final Pattern headerNamePattern,
            final KeyFormat keyFormat,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final Runnable onSuccess,
            final ComponentLog logger) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyReaderFactory = keyReaderFactory;
        this.headerEncoding = headerEncoding;
        this.headerNamePattern = headerNamePattern;
        this.keyFormat = keyFormat;
        this.keyEncoding = keyEncoding;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.onSuccess = onSuccess;
        this.logger = logger;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        try {
            final Map<RecordSchema, RecordGroup> recordGroups = new HashMap<>();

            String topic = null;
            int partition = 0;
            while (consumerRecords.hasNext()) {
                final ByteRecord consumerRecord = consumerRecords.next();
                if (topic == null) {
                    partition = consumerRecord.getPartition();
                    topic = consumerRecord.getTopic();
                }

                final byte[] value = consumerRecord.getValue();
                final Map<String, String> attributes = KafkaUtils.toAttributes(
                        consumerRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);

                try (final InputStream in = new ByteArrayInputStream(value);
                     final RecordReader valueRecordReader = readerFactory.createRecordReader(attributes, in, value.length, logger)) {

                    int recordCount = 0;
                    while (true) {
                        final Record record = valueRecordReader.nextRecord();
                        // If we get a KafkaRecord that has no value, we still need to process it.
                        if (recordCount++ > 0 && record == null) {
                            break;
                        }

                        final WrapperRecordKeyReader keyReader = new WrapperRecordKeyReader(keyFormat, keyReaderFactory, keyEncoding, logger);
                        final Tuple<RecordField, Object> recordKey = keyReader.toWrapperRecordKey(consumerRecord.getKey().orElse(null), attributes);
                        final RecordSchema recordSchema = record == null ? EMPTY_SCHEMA : record.getSchema();
                        final RecordSchema fullSchema = WrapperRecord.toWrapperSchema(recordKey.getKey(), recordSchema);
                        final RecordSchema writeSchema = writerFactory.getSchema(attributes, fullSchema);

                        // Get/Register the Record Group that is associated with the schema for this Kafka Record
                        RecordGroup recordGroup = recordGroups.get(writeSchema);
                        if (recordGroup == null) {
                            FlowFile flowFile = session.create();
                            final Map<String, String> groupAttributes = Map.of(
                                KafkaFlowFileAttribute.KAFKA_TOPIC, consumerRecord.getTopic(),
                                KafkaFlowFileAttribute.KAFKA_PARTITION, Long.toString(consumerRecord.getPartition())
                            );

                            flowFile = session.putAllAttributes(flowFile, groupAttributes);

                            final OutputStream out = session.write(flowFile);
                            final RecordSetWriter writer;
                            try {
                                writer = writerFactory.createWriter(logger, writeSchema, out, attributes);
                                writer.beginRecordSet();
                            } catch (final Exception e) {
                                out.close();
                                throw e;
                            }

                            recordGroup = new RecordGroup(flowFile, writer, topic, partition);
                            recordGroups.put(writeSchema, recordGroup);
                        }

                        // Create the Record object and write it to the Record Writer.
                        final ConsumeWrapperRecord consumeWrapperRecord = new ConsumeWrapperRecord(headerEncoding);
                        final MapRecord wrapperRecord = consumeWrapperRecord.toWrapperRecord(consumerRecord, record, recordKey);
                        recordGroup.writer().write(wrapperRecord);
                    }
                } catch (final MalformedRecordException e) {
                    // Failed to parse the record. Transfer to a 'parse.failure' relationship
                    FlowFile flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    flowFile = session.write(flowFile, out -> out.write(value));
                    session.transfer(flowFile, ConsumeKafka.PARSE_FAILURE);
                    session.adjustCounter("Records Received from " + consumerRecord.getTopic(), 1, false);

                    // Track the offsets for the Kafka Record
                    offsetTracker.update(consumerRecord);
                    continue;
                }

                // Track the offsets for the Kafka Record
                offsetTracker.update(consumerRecord);
            }

            // Finish writing the records
            for (final RecordGroup recordGroup : recordGroups.values()) {
                final Map<String, String> attributes;
                final int recordCount;
                try (final RecordSetWriter writer = recordGroup.writer()) {
                    final WriteResult writeResult = writer.finishRecordSet();
                    attributes = new HashMap<>(writeResult.getAttributes());
                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    attributes.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));

                    recordCount = writeResult.getRecordCount();
                }

                FlowFile flowFile = recordGroup.flowFile();
                flowFile = session.putAllAttributes(flowFile, attributes);
                final ProvenanceReporter provenanceReporter = session.getProvenanceReporter();
                final String transitUri = String.format(TRANSIT_URI_FORMAT, topic, partition);
                provenanceReporter.receive(flowFile, transitUri);
                session.adjustCounter("Records Received from " + topic, recordCount, false);
                session.transfer(flowFile, ConsumeKafka.SUCCESS);
            }

            onSuccess.run();
        } catch (final SchemaNotFoundException | IOException e) {
            throw new ProcessException("FlowFile Record conversion failed", e);
        }
    }

    private record RecordGroup(FlowFile flowFile, RecordSetWriter writer, String topic, int partition) {
    }
}
