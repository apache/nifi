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
package org.apache.nifi.processors.parquet.record;

import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Map;

/**
 * HDFSRecordReader that reads Parquet files using Avro.
 */
public class AvroParquetHDFSRecordReader implements HDFSRecordReader {

    private GenericRecord lastRecord;
    private RecordSchema recordSchema;
    private boolean initialized = false;

    private final ParquetReader<GenericRecord> parquetReader;

    public AvroParquetHDFSRecordReader(final ParquetReader<GenericRecord> parquetReader) {
        this.parquetReader = parquetReader;
    }

    @Override
    public Record nextRecord() throws IOException {
        if (initialized && lastRecord == null) {
            return null;
        }

        lastRecord = parquetReader.read();
        initialized = true;

        if (lastRecord == null) {
            return null;
        }

        if (recordSchema == null) {
            recordSchema = AvroTypeUtil.createSchema(lastRecord.getSchema());
        }

        final Map<String, Object> values = AvroTypeUtil.convertAvroRecordToMap(lastRecord, recordSchema);
        return new MapRecord(recordSchema, values);
    }


    @Override
    public void close() throws IOException {
        parquetReader.close();
    }

}
