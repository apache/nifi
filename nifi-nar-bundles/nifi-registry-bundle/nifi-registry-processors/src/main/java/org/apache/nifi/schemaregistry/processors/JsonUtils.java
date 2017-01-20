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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

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
import org.apache.commons.io.IOUtils;

class JsonUtils {

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

    public static GenericRecord read(InputStream jsonIs, Schema schema) {
        String jsonString = null;
        try {
            jsonString = IOUtils.toString(jsonIs, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read content represented as InputStream into a String", e);
        }
        return read(jsonString, schema);
    }

    public static GenericRecord read(String json, Schema schema) {
        InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        DataInputStream din = new DataInputStream(in);
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse '" + json
                    + "' into Avro GenericRecord. Possible reason: the value may not be a valid JSON or incompatible schema is provided. Schema was '"
                    + schema.toString(true) + "'.", e);
        }
    }
}
