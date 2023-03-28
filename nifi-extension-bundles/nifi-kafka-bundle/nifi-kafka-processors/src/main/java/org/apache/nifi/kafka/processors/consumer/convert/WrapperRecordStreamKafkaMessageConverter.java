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
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.wrapper.ConsumeWrapperRecord;
import org.apache.nifi.kafka.processors.consumer.wrapper.WrapperRecordKeyReader;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
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
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class WrapperRecordStreamKafkaMessageConverter implements KafkaMessageConverter {
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
    public void toFlowFiles(final ProcessSession session,
                            final Iterator<ByteRecord> consumerRecords) {
        try {
            while (consumerRecords.hasNext()) {
                final ByteRecord consumerRecord = consumerRecords.next();
                final byte[] value = consumerRecord.getValue();
                if (value.length > 0) {
                    final InputStream in = new ByteArrayInputStream(value);
                    final Map<String, String> attributes = KafkaUtils.toAttributes(
                            consumerRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);

                    final WrapperRecordKeyReader keyReader = new WrapperRecordKeyReader(
                            keyFormat, keyReaderFactory, keyEncoding, logger);
                    final Tuple<RecordField, Object> recordKey = keyReader.toWrapperRecordKey(
                            consumerRecord.getKey().orElse(null), attributes);

                    final RecordReader reader = readerFactory.createRecordReader(attributes, in, value.length, logger);
                    toFlowFile(session, attributes, reader, consumerRecord, recordKey);
                    offsetTracker.update(consumerRecord);
                }
            }
            onSuccess.run();
        } catch (MalformedRecordException | SchemaNotFoundException | IOException e) {
            throw new ProcessException("FlowFile Record conversion failed", e);
        }
    }

    private void toFlowFile(final ProcessSession session,
                            final Map<String, String> attributes,
                            final RecordReader reader,
                            final ByteRecord consumerRecord,
                            Tuple<RecordField, Object> recordKey)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        int recordCount = 0;
        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);
        try (final OutputStream rawOut = session.write(flowFile)) {
            final RecordSet recordSet = reader.createRecordSet();
            final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());
            final RecordSchema schemaWrapper = WrapperRecord.toWrapperSchema(recordKey.getKey(), schema);
            final RecordSetWriter writer = writerFactory.createWriter(logger, schemaWrapper, rawOut, attributes);
            Record record;
            writer.beginRecordSet();
            while ((record = recordSet.next()) != null) {
                ++recordCount;
                final ConsumeWrapperRecord consumeWrapperRecord = new ConsumeWrapperRecord(headerEncoding);
                final MapRecord wrapperRecord = consumeWrapperRecord.toWrapperRecord(consumerRecord, record, recordKey);
                writer.write(wrapperRecord);
            }
            writer.finishRecordSet();
            writer.flush();
            final ProvenanceReporter provenanceReporter = session.getProvenanceReporter();
            final String transitUri = String.format(TRANSIT_URI_FORMAT, consumerRecord.getTopic(), consumerRecord.getPartition());
            provenanceReporter.receive(flowFile, transitUri);
        }
        flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(recordCount));
        session.transfer(flowFile, ConsumeKafka.SUCCESS);
    }
}
