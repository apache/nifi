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

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.flowfile.FlowFile;

/**
 * Various Avro related utility operations relevant to transforming contents of
 * the {@link FlowFile} between Avro formats.
 */
class AvroUtils {

    /**
     * Reads provided {@link InputStream} into Avro {@link GenericRecord}
     * applying provided {@link Schema} returning the resulting GenericRecord.
     */
    public static GenericRecord read(InputStream in, Schema schema) {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        GenericRecord avroRecord = null;
        try {
            avroRecord = datumReader.read(null, DecoderFactory.get().binaryDecoder(in, null));
            return avroRecord;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read AVRO record", e);
        }
    }

    /**
     * Writes provided {@link GenericRecord} into the provided
     * {@link OutputStream}.
     */
    public static void write(GenericRecord record, OutputStream out) {
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write AVRO record", e);
        }
    }
}
