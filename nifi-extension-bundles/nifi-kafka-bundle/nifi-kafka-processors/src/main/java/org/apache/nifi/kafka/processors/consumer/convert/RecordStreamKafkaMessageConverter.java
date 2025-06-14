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
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RecordStreamKafkaMessageConverter extends AbstractRecordStreamKafkaMessageConverter {

    public RecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final Charset headerEncoding,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri) {
        super(readerFactory, writerFactory, headerEncoding, headerNamePattern, keyEncoding, commitOffsets, offsetTracker, logger, brokerUri);
    }

    @Override
    protected RecordSchema getWriteSchema(final RecordSchema inputSchema, final ByteRecord consumerRecord, final Map<String, String> attributes) throws IOException {
        try {
            return writerFactory.getSchema(attributes, inputSchema);
        } catch (IOException | SchemaNotFoundException e) {
            throw new IOException("Unable to get schema for wrapper record", e);
        }
    }

    @Override
    protected Record convertRecord(final ByteRecord consumerRecord, final Record record, final Map<String, String> attributes) {
        return record;
    }

    @Override
    protected Map<String, String> extractHeaders(final ByteRecord consumerRecord) {
        if (headerNamePattern == null || consumerRecord == null) {
            return Map.of();
        }

        final Map<String, String> headers = new HashMap<>();
        for (final RecordHeader h : consumerRecord.getHeaders()) {
            final String name = h.key();
            if (headerNamePattern.matcher(name).matches()) {
                headers.put(name, new String(h.value(), headerEncoding));
            }
        }
        return headers;
    }
}
