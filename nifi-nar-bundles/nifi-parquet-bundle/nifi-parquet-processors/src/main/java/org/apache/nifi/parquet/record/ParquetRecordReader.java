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
package org.apache.nifi.parquet.record;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class ParquetRecordReader implements RecordReader {

    private GenericRecord lastParquetRecord;
    private RecordSchema recordSchema;

    private final InputStream inputStream;
    private final InputFile inputFile;
    private final ParquetReader<GenericRecord> parquetReader;

    public ParquetRecordReader(final InputStream inputStream, final long inputLength, final Configuration configuration) throws IOException {
        if (inputLength < 0) {
            throw new IllegalArgumentException("Invalid input length of '" + inputLength + "'. This record reader requires knowing " +
                    "the length of the InputStream and cannot be used in some cases where the length may not be known.");
        }

        this.inputStream = inputStream;

        inputFile = new NifiParquetInputFile(inputStream, inputLength);
        parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).withConf(configuration).build();

        // Read the first record so that we can extract the schema
        lastParquetRecord = parquetReader.read();
        if (lastParquetRecord == null) {
            throw new EOFException("Unable to obtain schema because no records were available");
        }

        // Convert Avro schema to RecordSchema
        recordSchema = AvroTypeUtil.createSchema(lastParquetRecord.getSchema());
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields) throws IOException {
        // If null then no more records are available
        if (lastParquetRecord == null) {
            return null;
        }

        // Convert the last Parquet GenericRecord to NiFi Record
        final Map<String, Object> values = AvroTypeUtil.convertAvroRecordToMap(lastParquetRecord, recordSchema);
        final Record record = new MapRecord(recordSchema, values);

        // Read the next record and store for next time
        lastParquetRecord = parquetReader.read();

        // Return the converted record
        return record;
    }

    @Override
    public RecordSchema getSchema() {
        return recordSchema;
    }

    @Override
    public void close() throws IOException {
        try {
            parquetReader.close();
        } finally {
            // ensure the input stream still gets closed
            inputStream.close();
        }
    }
}
