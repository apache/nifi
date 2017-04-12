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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

/**
 * HDFSRecordWriter that writes Parquet files using Avro as the schema representation.
 */
public class AvroParquetHDFSRecordWriter implements HDFSRecordWriter {

    private final Schema avroSchema;
    private final ParquetWriter<GenericRecord> parquetWriter;

    public AvroParquetHDFSRecordWriter(final ParquetWriter<GenericRecord> parquetWriter, final Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.parquetWriter = parquetWriter;
    }

    @Override
    public void write(final Record record) throws IOException {
        final GenericRecord genericRecord = AvroTypeUtil.createAvroRecord(record, avroSchema);
        parquetWriter.write(genericRecord);
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }

}
