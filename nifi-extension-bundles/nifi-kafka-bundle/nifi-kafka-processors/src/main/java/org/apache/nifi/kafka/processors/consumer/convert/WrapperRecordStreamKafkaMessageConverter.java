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

import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.wrapper.ConsumeWrapperRecord;
import org.apache.nifi.kafka.processors.consumer.wrapper.WrapperRecordKeyReader;
import org.apache.nifi.kafka.processors.producer.wrapper.InjectMetadataRecord;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

public class WrapperRecordStreamKafkaMessageConverter extends AbstractRecordStreamKafkaMessageConverter {

    private final RecordReaderFactory keyReaderFactory;
    private final KeyFormat keyFormat;
    private final OutputStrategy outputStrategy;

    public WrapperRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final RecordReaderFactory keyReaderFactory,
            final Charset headerEncoding,
            final java.util.regex.Pattern headerNamePattern,
            final KeyFormat keyFormat,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri,
            final OutputStrategy outputStrategy) {
        super(readerFactory, writerFactory, headerEncoding, headerNamePattern, keyEncoding, commitOffsets, offsetTracker, logger, brokerUri);
        this.keyReaderFactory = keyReaderFactory;
        this.keyFormat = keyFormat;
        this.outputStrategy = outputStrategy;
    }

    @Override
    protected RecordSchema getWriteSchema(final RecordSchema inputSchema, final ByteRecord consumerRecord, final Map<String, String> attributes) throws IOException {
        try {
            final WrapperRecordKeyReader keyReader = new WrapperRecordKeyReader(keyFormat, keyReaderFactory, keyEncoding, logger);
            final Tuple<RecordField, Object> recordKey = keyReader.toWrapperRecordKey(consumerRecord.getKey().orElse(null), attributes);
            final RecordSchema fullSchema = outputStrategy == OutputStrategy.USE_WRAPPER
                    ? WrapperRecord.toWrapperSchema(recordKey.getKey(), inputSchema)
                    : InjectMetadataRecord.toWrapperSchema(recordKey.getKey(), inputSchema);
            return writerFactory.getSchema(attributes, fullSchema);
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new IOException("Unable to get schema for wrapper record", e);
        }
    }

    @Override
    protected Record convertRecord(final ByteRecord consumerRecord, final Record record, final Map<String, String> attributes) throws IOException {
        try {
            final WrapperRecordKeyReader keyReader = new WrapperRecordKeyReader(keyFormat, keyReaderFactory, keyEncoding, logger);
            final Tuple<RecordField, Object> recordKey = keyReader.toWrapperRecordKey(consumerRecord.getKey().orElse(null), attributes);
            return outputStrategy == OutputStrategy.USE_WRAPPER
                    ? new ConsumeWrapperRecord(headerEncoding).toWrapperRecord(consumerRecord, record, recordKey)
                    : InjectMetadataRecord.toWrapperRecord(headerEncoding, consumerRecord, record, recordKey);
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new IOException("Unable to convert record", e);
        }
    }

}
