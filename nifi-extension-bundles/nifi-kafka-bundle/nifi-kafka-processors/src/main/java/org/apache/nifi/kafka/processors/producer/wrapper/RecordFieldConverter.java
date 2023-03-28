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
package org.apache.nifi.kafka.processors.producer.wrapper;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RecordFieldConverter {
    private final Record record;
    private final FlowFile flowFile;
    private final ComponentLog logger;

    public RecordFieldConverter(final Record record, final FlowFile flowFile, final ComponentLog logger) {
        this.record = record;
        this.flowFile = flowFile;
        this.logger = logger;
    }

    public byte[] toBytes(final String fieldName, final RecordSetWriterFactory writerFactory) throws IOException {
        final byte[] bytes;

        try {
            final Object field = record.getValue(fieldName);
            if (field == null) {
                bytes = null;
            } else if (field instanceof Record) {
                bytes = toBytes((Record) field, writerFactory);
            } else if (field instanceof Byte[]) {
                bytes = toBytes((Byte[]) field);
            } else if (field instanceof Object[]) {
                bytes = toBytes((Object[]) field);
            } else if (field instanceof String) {
                bytes = toBytes((String) field);
            } else {
                throw new MalformedRecordException(String.format("Failed to convert [%s] record data to byte array", fieldName));
            }
        } catch (final SchemaNotFoundException | MalformedRecordException e) {
            throw new IOException("Field conversion failed", e);
        }

        return bytes;
    }

    private byte[] toBytes(final Record field, final RecordSetWriterFactory writerFactory)
            throws MalformedRecordException, SchemaNotFoundException, IOException {
        if (writerFactory == null) {
            throw new MalformedRecordException("Record has a key that is itself a record, but the 'Record Key Writer' "
                    + "of the processor was not configured. If Records are expected to have a Record as the key, the "
                    + "'Record Key Writer' property must be set.");
        }
        final RecordSchema schema = writerFactory.getSchema(flowFile.getAttributes(), field.getSchema());
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowFile)) {
            writer.write(field);
            writer.flush();
            return baos.toByteArray();
        }
    }

    private byte[] toBytes(final Byte[] bytesSource) {
        final byte[] bytesTarget = new byte[bytesSource.length];
        for (int i = 0; (i < bytesSource.length); ++i) {
            bytesTarget[i] = bytesSource[i];
        }
        return bytesTarget;
    }

    private byte[] toBytes(final Object[] objects) {
        final byte[] bytesTarget = new byte[objects.length];
        for (int i = 0; (i < objects.length); ++i) {
            bytesTarget[i] = ((Integer) objects[i]).byteValue();
        }
        return bytesTarget;
    }

    private byte[] toBytes(final String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }
}
