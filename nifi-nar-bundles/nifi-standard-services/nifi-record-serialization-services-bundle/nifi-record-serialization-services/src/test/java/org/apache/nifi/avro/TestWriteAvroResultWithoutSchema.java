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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.WriteAvroSchemaAttributeStrategy;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestWriteAvroResultWithoutSchema extends TestWriteAvroResult {

    private final BlockingQueue<BinaryEncoder> encoderPool = new LinkedBlockingQueue<>(32);

    @Override
    protected RecordSetWriter createWriter(final Schema schema, final OutputStream out) throws IOException {
        return new WriteAvroResultWithExternalSchema(schema, AvroTypeUtil.createSchema(schema), new WriteAvroSchemaAttributeStrategy(), out, encoderPool,
            new MockComponentLog("id", new Object()));
    }

    @Override
    protected GenericRecord readRecord(final InputStream in, final Schema schema) throws IOException {
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        return reader.read(null, decoder);
    }

    @Override
    protected void verify(final WriteResult writeResult) {
        final Map<String, String> attributes = writeResult.getAttributes();

        final String schemaText = attributes.get("avro.schema");
        Assert.assertNotNull(schemaText);
        new Schema.Parser().parse(schemaText);
    }


    @Test
    @Ignore("This test takes many seconds to run and is only really useful for comparing performance of the writer before and after changes, so it is @Ignored, but left in place to be run manually " +
        "for performance comparisons before & after changes are made.")
    public void testPerf() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final OutputStream out = new NullOutputStream();

        final Record record = new MapRecord(recordSchema, Collections.singletonMap("name", "John Doe"));
        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);

        final ComponentLog logger = new MockComponentLog("id", new Object());

        final long start = System.nanoTime();
        for (int i=0; i < 10_000_000; i++) {
            try (final RecordSetWriter writer = new WriteAvroResultWithExternalSchema(avroSchema, recordSchema, new NopSchemaAccessWriter(), out, encoderPool, logger)) {
                writer.write(RecordSet.of(record.getSchema(), record));
            }
        }

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println(millis);
    }
}
