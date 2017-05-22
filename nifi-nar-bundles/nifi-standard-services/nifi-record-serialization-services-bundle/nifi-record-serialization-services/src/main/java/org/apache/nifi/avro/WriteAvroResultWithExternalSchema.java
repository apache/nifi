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

package org.apache.nifi.avro;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

public class WriteAvroResultWithExternalSchema extends AbstractRecordSetWriter {
    private final SchemaAccessWriter schemaAccessWriter;
    private final RecordSchema recordSchema;
    private final Schema avroSchema;
    private final BinaryEncoder encoder;
    private final OutputStream buffered;
    private final DatumWriter<GenericRecord> datumWriter;

    public WriteAvroResultWithExternalSchema(final Schema avroSchema, final RecordSchema recordSchema,
        final SchemaAccessWriter schemaAccessWriter, final OutputStream out) throws IOException {
        super(out);
        this.recordSchema = recordSchema;
        this.schemaAccessWriter = schemaAccessWriter;
        this.avroSchema = avroSchema;
        this.buffered = new BufferedOutputStream(out);

        datumWriter = new GenericDatumWriter<>(avroSchema);
        encoder = EncoderFactory.get().blockingBinaryEncoder(buffered, null);
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaAccessWriter.writeHeader(recordSchema, buffered);
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        flush();
        return schemaAccessWriter.getAttributes(recordSchema);
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            flush();
            schemaAccessWriter.writeHeader(recordSchema, getOutputStream());
        }

        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, avroSchema);
        datumWriter.write(rec, encoder);
        return schemaAccessWriter.getAttributes(recordSchema);
    }

    @Override
    public void flush() throws IOException {
        encoder.flush();
        buffered.flush();
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }
}
