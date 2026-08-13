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
import org.apache.nifi.kafka.processors.common.HeaderValueConverter;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Shared reader loop, parse-failure handling, and record conversion for record-stream converters.
 * FlowFile grouping and finalization are delegated to a {@link RecordGroupingStrategy}.
 */
public abstract class AbstractRecordStreamKafkaMessageConverter implements KafkaMessageConverter {

    protected final RecordReaderFactory readerFactory;
    protected final RecordSetWriterFactory writerFactory;
    protected final HeaderValueConverter headerValueConverter;
    protected final Pattern headerNamePattern;
    protected final KeyEncoding keyEncoding;
    protected final boolean commitOffsets;
    protected final OffsetTracker offsetTracker;
    protected final ComponentLog logger;
    protected final String brokerUri;
    private final RecordGroupingStrategy recordGroupingStrategy;

    protected AbstractRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final HeaderValueConverter headerValueConverter,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri,
            final RecordGroupingStrategy recordGroupingStrategy) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.headerValueConverter = headerValueConverter;
        this.headerNamePattern = headerNamePattern;
        this.keyEncoding = keyEncoding;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.logger = logger;
        this.brokerUri = brokerUri;
        this.recordGroupingStrategy = recordGroupingStrategy;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        while (consumerRecords.hasNext()) {
            final ByteRecord consumerRecord = consumerRecords.next();
            final byte[] value = consumerRecord.getValue();

            final Map<String, String> attributes = KafkaUtils.toAttributes(
                    consumerRecord, keyEncoding, headerNamePattern, headerValueConverter, commitOffsets);

            final Map<String, String> groupingAttributes = extractHeaders(consumerRecord);

            try (final InputStream in = new ByteArrayInputStream(value);
                    final RecordReader reader = readerFactory.createRecordReader(attributes, in, value.length, logger)) {

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    final RecordSchema writeSchema = getWriteSchema(record.getSchema(), consumerRecord, attributes);
                    final Record toWrite = convertRecord(consumerRecord, record, attributes);
                    recordGroupingStrategy.addRecord(session, consumerRecord, toWrite, writeSchema, attributes, groupingAttributes);
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

        recordGroupingStrategy.finishAllGroups(session);
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
}
