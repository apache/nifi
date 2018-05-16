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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.schema.access.WriteAvroSchemaAttributeStrategy;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.junit.Assert;

public class TestWriteAvroResultWithoutSchema extends TestWriteAvroResult {

    @Override
    protected RecordSetWriter createWriter(final Schema schema, final OutputStream out) throws IOException {
        return new WriteAvroResultWithExternalSchema(schema, AvroTypeUtil.createSchema(schema), new WriteAvroSchemaAttributeStrategy(), out);
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

}
