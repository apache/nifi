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
package org.apache.nifi.processors.iceberg;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.nifi.processors.iceberg.appender.IcebergFileAppenderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

/**
 * Factory class to create the suitable {@link TaskWriter} based on the {@link Table}'s properties
 */
public class IcebergTaskWriterFactory {
    private final Table table;
    private final Schema schema;
    private final PartitionSpec spec;
    private final FileIO io;
    private long targetFileSize;
    private FileFormat format;

    private FileAppenderFactory<Record> appenderFactory;
    private OutputFileFactory outputFileFactory;
    private RecordSchema recordSchema;

    public IcebergTaskWriterFactory(Table table, RecordSchema recordSchema) {
        this.table = table;
        this.schema = table.schema();
        this.spec = table.spec();
        this.io = table.io();
        this.recordSchema = recordSchema;
    }

    public void initialize(int partitionId, long attemptId) {
        Map<String, String> properties = table.properties();

        String dataFileFormatName = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        this.format = FileFormat.valueOf(dataFileFormatName.toUpperCase(Locale.ENGLISH));

        this.targetFileSize = PropertyUtil.propertyAsLong(properties, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        this.outputFileFactory = OutputFileFactory.builderFor(table, partitionId, attemptId).build();
        this.appenderFactory = new IcebergFileAppenderFactory(schema, table, spec, recordSchema);
    }

    public TaskWriter<Record> create() {
        checkNotNull(outputFileFactory, "The outputFileFactory shouldn't be null if we have invoked the initialize()");
        checkNotNull(appenderFactory, "The appenderFactory shouldn't be null if we have invoked the initialize()");

        if (spec.isUnpartitioned()) {
            return new UnpartitionedWriter<>(spec, format, appenderFactory, outputFileFactory, io, targetFileSize);
        } else {
            return new IcebergPartitionedWriter(spec, format, appenderFactory, outputFileFactory, io, targetFileSize, schema, recordSchema);
        }
    }
}
