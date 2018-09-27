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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class AvroReaderWithExplicitSchema extends AvroRecordReader {
    private final InputStream in;
    private final RecordSchema recordSchema;
    private final DatumReader<GenericRecord> datumReader;
    private final BinaryDecoder decoder;
    private GenericRecord genericRecord;

    public AvroReaderWithExplicitSchema(final InputStream in, final RecordSchema recordSchema, final Schema avroSchema) {
        this.in = in;
        this.recordSchema = recordSchema;

        datumReader = new NonCachingDatumReader<>(avroSchema);
        decoder = DecoderFactory.get().binaryDecoder(in, null);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    protected GenericRecord nextAvroRecord() throws IOException {
        if (decoder.isEnd()) {
            return null;
        }

        try {
            genericRecord = datumReader.read(genericRecord, decoder);
        } catch (final EOFException eof) {
            return null;
        }

        return genericRecord;
    }

    @Override
    public RecordSchema getSchema() {
        return recordSchema;
    }
}
