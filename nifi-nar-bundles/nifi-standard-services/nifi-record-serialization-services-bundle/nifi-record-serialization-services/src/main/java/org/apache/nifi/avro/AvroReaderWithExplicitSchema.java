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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.nifi.serialization.record.RecordSchema;

public class AvroReaderWithExplicitSchema extends AvroRecordReader {
    private final InputStream in;
    private final RecordSchema recordSchema;
    private final DatumReader<GenericRecord> datumReader;
    private BinaryDecoder decoder;
    private GenericRecord genericRecord;
    private DataFileStream<GenericRecord> dataFileStream;

    public AvroReaderWithExplicitSchema(final InputStream in, final RecordSchema recordSchema, final Schema avroSchema) throws IOException {
        this.in = in;
        this.recordSchema = recordSchema;

        datumReader = new NonCachingDatumReader<>(avroSchema);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TeeInputStream teeInputStream = new TeeInputStream(in, baos);
        // Try to parse as a DataFileStream, if it works, glue the streams back together and delegate calls to the DataFileStream
        try {
            dataFileStream = new DataFileStream<>(teeInputStream, new NonCachingDatumReader<>());
        } catch (IOException ioe) {
            // Carry on, hopefully a raw Avro file
            // Need to be able to re-read the bytes read so far, and the InputStream passed in doesn't support reset. Use the TeeInputStream in
            // conjunction with SequenceInputStream to glue the two streams back together for future reading
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            SequenceInputStream sis = new SequenceInputStream(bais, in);
            decoder = DecoderFactory.get().binaryDecoder(sis, null);
        }
        if (dataFileStream != null) {
            // Verify the schemas are the same
            Schema embeddedSchema = dataFileStream.getSchema();
            if (!embeddedSchema.equals(avroSchema)) {
                throw new IOException("Explicit schema does not match embedded schema");
            }
            // Need to be able to re-read the bytes read so far, but we don't want to copy the input to a byte array anymore, so get rid of the TeeInputStream
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            SequenceInputStream sis = new SequenceInputStream(bais, in);
            dataFileStream = new DataFileStream<>(sis, new NonCachingDatumReader<>());
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    protected GenericRecord nextAvroRecord() throws IOException {
        // If the avro file had an embedded schema that matched the explicit schema, delegate to the DataFileStream for reading records
        if (dataFileStream != null) {
            return dataFileStream.hasNext() ? dataFileStream.next() : null;
        }

        if (decoder.isEnd()) {
            return null;
        }

        try {
            genericRecord = datumReader.read(null, decoder);
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
