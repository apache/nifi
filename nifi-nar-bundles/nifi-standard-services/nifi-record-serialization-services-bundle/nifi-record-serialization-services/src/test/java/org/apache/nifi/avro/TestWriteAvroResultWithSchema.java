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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.RecordSetWriter;

public class TestWriteAvroResultWithSchema extends TestWriteAvroResult {

    @Override
    protected RecordSetWriter createWriter(final Schema schema, final OutputStream out) throws IOException {
        return new WriteAvroResultWithSchema(schema, out, CodecFactory.nullCodec());
    }

    @Override
    protected GenericRecord readRecord(final InputStream in, final Schema schema) throws IOException {
        final DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
        final Schema avroSchema = dataFileStream.getSchema();
        GenericData.setStringType(avroSchema, StringType.String);
        final GenericRecord avroRecord = dataFileStream.next();

        return avroRecord;
    }
}
