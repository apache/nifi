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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;

public abstract class WriteAvroResult implements RecordSetWriter {
    private final Schema schema;

    public WriteAvroResult(final Schema schema) {
        this.schema = schema;
    }

    protected Schema getSchema() {
        return schema;
    }

    @Override
    public WriteResult write(final Record record, final OutputStream out) throws IOException {
        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, out);
            dataFileWriter.append(rec);
        }

        return WriteResult.of(1, Collections.emptyMap());
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }

    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }
}
