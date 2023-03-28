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
package org.apache.nifi.kafka.processors.consumer.wrapper;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class WrapperRecordKeyReader {
    private final KeyFormat keyFormat;
    private final RecordReaderFactory keyReaderFactory;
    private final KeyEncoding keyEncoding;
    private final ComponentLog logger;

    public WrapperRecordKeyReader(
            final KeyFormat keyFormat,
            final RecordReaderFactory keyReaderFactory,
            final KeyEncoding keyEncoding,
            final ComponentLog logger) {
        this.keyReaderFactory = keyReaderFactory;
        this.keyFormat = keyFormat;
        this.keyEncoding = keyEncoding;
        this.logger = logger;
    }

    public Tuple<RecordField, Object> toWrapperRecordKey(final byte[] key, final Map<String, String> attributes)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final Tuple<RecordField, Object> wrapperRecordKey;
        if (KeyFormat.RECORD.equals(keyFormat)) {
            wrapperRecordKey = toKeyRecord(key, attributes);
        } else if (KeyFormat.STRING.equals(keyFormat)) {
            wrapperRecordKey = toKeyString(key);
        } else if (KeyFormat.BYTE_ARRAY.equals(keyFormat)) {
            wrapperRecordKey = toKeyBytes(key);
        } else {
            throw new ProcessException(String.format("KeyFormat has an unknown value %s", keyFormat));
        }
        return wrapperRecordKey;
    }

    private Tuple<RecordField, Object> toKeyRecord(final byte[] key, final Map<String, String> attributes)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final Tuple<RecordField, Object> keyRecord;
        if (key.length == 0) {
            keyRecord = new Tuple<>(EMPTY_SCHEMA_KEY_RECORD_FIELD, null);
        } else {
            try (final InputStream is = new ByteArrayInputStream(key);
                final RecordReader reader = keyReaderFactory.createRecordReader(attributes, is, key.length, logger)) {
                final Record record = reader.nextRecord();
                final RecordField recordField = new RecordField(WrapperRecord.KEY, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                keyRecord = new Tuple<>(recordField, record);
            }
        }
        return keyRecord;
    }

    private Tuple<RecordField, Object> toKeyString(final byte[] key) {
        final RecordField recordField = new RecordField(WrapperRecord.KEY, RecordFieldType.STRING.getDataType());
        return new Tuple<>(recordField, KafkaUtils.toKeyString(key, keyEncoding));
    }

    private Tuple<RecordField, Object> toKeyBytes(final byte[] key) {
        final DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
        final RecordField recordField = new RecordField(WrapperRecord.KEY, dataType);
        return new Tuple<>(recordField, ArrayUtils.toObject(key));
    }

    private static final RecordField EMPTY_SCHEMA_KEY_RECORD_FIELD =
            new RecordField(WrapperRecord.KEY, RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Collections.emptyList())));
}
