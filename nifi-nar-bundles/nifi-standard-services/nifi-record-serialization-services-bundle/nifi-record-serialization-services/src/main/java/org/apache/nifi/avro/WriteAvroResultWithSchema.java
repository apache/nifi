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
import java.io.OutputStream;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

public class WriteAvroResultWithSchema extends WriteAvroResult {

    public WriteAvroResultWithSchema(final Schema schema) {
        super(schema);
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream outStream) throws IOException {
        Record record = rs.next();
        if (record == null) {
            return WriteResult.of(0, Collections.emptyMap());
        }

        int nrOfRows = 0;
        final Schema schema = getSchema();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            do {
                final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, schema);
                dataFileWriter.append(rec);
                nrOfRows++;
            } while ((record = rs.next()) != null);
        }

        return WriteResult.of(nrOfRows, Collections.emptyMap());
    }

    @Override
    public WriteResult write(final Record record, final OutputStream out) throws IOException {
        if (record == null) {
            return WriteResult.of(0, Collections.emptyMap());
        }

        final Schema schema = getSchema();
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, out);

            final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, schema);
            dataFileWriter.append(rec);
        }

        return WriteResult.of(1, Collections.emptyMap());
    }
}
