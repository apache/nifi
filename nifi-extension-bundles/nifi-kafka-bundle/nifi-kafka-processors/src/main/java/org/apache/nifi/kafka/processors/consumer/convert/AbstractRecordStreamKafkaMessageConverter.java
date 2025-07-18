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
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public abstract class AbstractRecordStreamKafkaMessageConverter implements KafkaMessageConverter {
    private static final RecordSchema EMPTY_SCHEMA = new SimpleRecordSchema(List.of());

    protected final RecordReaderFactory readerFactory;
    protected final RecordSetWriterFactory writerFactory;
    protected final Charset headerEncoding;
    protected final Pattern headerNamePattern;
    protected final KeyEncoding keyEncoding;
    protected final boolean commitOffsets;
    protected final OffsetTracker offsetTracker;
    protected final ComponentLog logger;
    protected final String brokerUri;

    public AbstractRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final Charset headerEncoding,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.headerEncoding = headerEncoding;
        this.headerNamePattern = headerNamePattern;
        this.keyEncoding = keyEncoding;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.logger = logger;
        this.brokerUri = brokerUri;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        final Map<RecordGroupCriteria, RecordGroup> recordGroups = new HashMap<>();

        while (consumerRecords.hasNext()) {
            final ByteRecord consumerRecord = consumerRecords.next();
            final String topic = consumerRecord.getTopic();
            final int partition = consumerRecord.getPartition();
            final byte[] value = consumerRecord.getValue();

            // shared attribute extraction
            final Map<String, String> attributes = KafkaUtils.toAttributes(
                    consumerRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);

            // hook for subclasses to expose headers (if desired)
            final Map<String, String> extraAttrs = extractHeaders(consumerRecord);

            try (final InputStream in = new ByteArrayInputStream(value);
                    final RecordReader reader = readerFactory.createRecordReader(attributes, in, value.length, logger)) {

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    // delegate the actual grouping & writing
                    processSingleRecord(session, recordGroups, consumerRecord, record, attributes, extraAttrs, topic, partition);
                }
            } catch (final MalformedRecordException | IOException | SchemaNotFoundException e) {
                logger.debug("Reader or Writer failed to process Kafka Record with Topic [{}] Partition [{}] Offset [{}]",
                             consumerRecord.getTopic(), consumerRecord.getPartition(), consumerRecord.getOffset(), e);
                handleParseFailure(session, consumerRecord, attributes, value);
                offsetTracker.update(consumerRecord);
                continue;
            } catch (Exception e) {
                throw new RuntimeException("Failed to process Kafka message", e);
            }

            offsetTracker.update(consumerRecord);
        }

        finishAllGroups(session, recordGroups);
    }

    private void processSingleRecord(final ProcessSession session,
            final Map<RecordGroupCriteria, RecordGroup> recordGroups,
            final ByteRecord consumerRecord,
            final Record record,
            final Map<String, String> attributes,
            final Map<String, String> extraAttrs,
            final String topic,
            final int partition) throws Exception {
        // pick the “bare” schema if the record is null
        final RecordSchema inputSchema = record == null ? EMPTY_SCHEMA : record.getSchema();
        // let subclass decide how to wrap/transform that into the final schema
        final RecordSchema writeSchema = getWriteSchema(inputSchema, consumerRecord, attributes);

        final RecordGroupCriteria criteria = new RecordGroupCriteria(writeSchema, extraAttrs, topic, partition);
        RecordGroup group = recordGroups.get(criteria);
        if (group == null) {
            FlowFile ff = session.create();
            ff = session.putAllAttributes(ff, Map.of(
                    KafkaFlowFileAttribute.KAFKA_TOPIC, topic,
                    KafkaFlowFileAttribute.KAFKA_PARTITION, String.valueOf(partition)));

            final OutputStream out = session.write(ff);
            final RecordSetWriter writer;
            try {
                writer = writerFactory.createWriter(logger, writeSchema, out, attributes);
                writer.beginRecordSet();
            } catch (final Exception ex) {
                out.close();
                throw ex;
            }

            final long offset = consumerRecord.getOffset();
            final AtomicLong maxOffset = new AtomicLong(offset);
            final AtomicLong minOffset = new AtomicLong(offset);
            group = new RecordGroup(ff, writer, maxOffset, minOffset);
            recordGroups.put(criteria, group);
        } else {
            final long recordOffset = consumerRecord.getOffset();
            final AtomicLong maxOffset = group.maxOffset();
            if (recordOffset > maxOffset.get()) {
                maxOffset.set(recordOffset);
            }

            final AtomicLong minOffset = group.minOffset();
            if (recordOffset < minOffset.get()) {
                minOffset.set(recordOffset);
            }
        }

        // let subclass convert into the thing to write
        final Record toWrite = convertRecord(consumerRecord, record, attributes);
        if (toWrite != null) {
            group.writer().write(toWrite);
        }
    }

    private void finishAllGroups(final ProcessSession session, final Map<RecordGroupCriteria, RecordGroup> recordGroups) {
        for (final Map.Entry<RecordGroupCriteria, RecordGroup> e : recordGroups.entrySet()) {
            final RecordGroupCriteria criteria = e.getKey();
            final RecordGroup group = e.getValue();

            final Map<String, String> resultAttrs = new HashMap<>();
            final int recordCount;
            try (final RecordSetWriter writer = group.writer()) {
                final WriteResult wr = writer.finishRecordSet();
                resultAttrs.putAll(wr.getAttributes());
                resultAttrs.put("record.count", String.valueOf(wr.getRecordCount()));
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_COUNT, String.valueOf(wr.getRecordCount()));
                resultAttrs.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

                final long maxOffset = group.maxOffset().get();
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(maxOffset));

                final long minOffset = group.minOffset().get();
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(minOffset));

                // add any extra header‐derived attributes
                resultAttrs.putAll(criteria.extraAttributes());
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));
                recordCount = wr.getRecordCount();
            } catch (final Exception ex) {
                throw new ProcessException("Failed to write Kafka records to FlowFile", ex);
            }

            FlowFile ff = group.flowFile();
            ff = session.putAllAttributes(ff, resultAttrs);

            session.getProvenanceReporter().receive(ff, brokerUri + "/" + criteria.topic());
            session.adjustCounter("Records Received from " + criteria.topic(), recordCount, false);
            session.transfer(ff, ConsumeKafka.SUCCESS);
        }
    }

    protected void handleParseFailure(final ProcessSession session, final ByteRecord consumerRecord, final Map<String, String> attributes, final byte[] value) {
        FlowFile ff = session.create();
        ff = session.putAllAttributes(ff, attributes);
        ff = session.write(ff, out -> out.write(value));
        session.transfer(ff, ConsumeKafka.PARSE_FAILURE);
        session.adjustCounter("Records Received from " + consumerRecord.getTopic(), 1, false);
    }

    /**
     * By default we do *not* promote any headers to FlowFile attributes.
     **/
    protected Map<String, String> extractHeaders(final ByteRecord consumerRecord) {
        return Map.of();
    }

    protected abstract RecordSchema getWriteSchema(RecordSchema inputSchema, ByteRecord consumerRecord, Map<String, String> attributes) throws IOException;

    protected abstract Record convertRecord(ByteRecord consumerRecord, Record record, Map<String, String> attributes) throws IOException;

    private record RecordGroupCriteria(RecordSchema schema, Map<String, String> extraAttributes, String topic, int partition) {
    }

    private record RecordGroup(FlowFile flowFile, RecordSetWriter writer, AtomicLong maxOffset, AtomicLong minOffset) {
    }
}
