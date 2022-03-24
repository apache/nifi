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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parquet.stream.NifiParquetOutputFile;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;

public class WriteParquetResult extends AbstractRecordSetWriter {

    private final Schema schema;
    private final ParquetWriter<GenericRecord> parquetWriter;
    private final ComponentLog componentLogger;

    public WriteParquetResult(final Schema schema, final OutputStream out, final ParquetConfig parquetConfig, final ComponentLog componentLogger) throws IOException {
        super(out);
        this.schema = schema;
        this.componentLogger = componentLogger;

        final Configuration conf = new Configuration();
        final OutputFile outputFile = new NifiParquetOutputFile(out);

        final AvroParquetWriter.Builder<GenericRecord> writerBuilder =
                AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(schema);
        applyCommonConfig(writerBuilder, conf, parquetConfig);
        parquetWriter = writerBuilder.build();
    }

    @Override
    protected Map<String, String> writeRecord(final Record record) throws IOException {
        final GenericRecord genericRecord = AvroTypeUtil.createAvroRecord(record, schema);
        parquetWriter.write(genericRecord);
        return Collections.emptyMap();
    }

    @Override
    public void close() throws IOException {
        try {
            parquetWriter.close();
        } finally {
            // ensure the output stream still gets closed
            super.close();
        }
    }

    @Override
    public String getMimeType() {
        return "application/parquet";
    }

}
