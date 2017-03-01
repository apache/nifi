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
package org.apache.nifi.schemaregistry.processors;

import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.nifi.flowfile.FlowFile;

/**
 * Various Json related utility operations relevant to transforming contents of
 * the {@link FlowFile} between JSON and AVRO formats.
 */
class JsonUtils {

    /**
     * Writes provided {@link GenericRecord} into the provided
     * {@link OutputStream} as JSON.
     */
    public static void write(GenericRecord record, OutputStream out) {
        try {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            writer.write(record, encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read GenericRecord", e);
        }
    }

    /**
     * Reads provided {@link InputStream} as ISON into Avro
     * {@link GenericRecord} applying provided {@link Schema} returning the
     * resulting GenericRecord.
     */
    public static GenericRecord read(InputStream jsonIs, Schema schema) {
        DataInputStream din = new DataInputStream(jsonIs);
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse incoming Json input stream into Avro GenericRecord. "
                    + "Possible reason: the value may not be a valid JSON or incompatible schema is provided. Schema was '"
                    + schema.toString(true) + "'.", e);
        }
    }
}
