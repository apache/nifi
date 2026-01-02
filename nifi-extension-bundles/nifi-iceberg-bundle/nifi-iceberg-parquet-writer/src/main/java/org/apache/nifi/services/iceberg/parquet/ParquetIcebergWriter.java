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
package org.apache.nifi.services.iceberg.parquet;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.services.iceberg.IcebergRowWriter;
import org.apache.nifi.services.iceberg.IcebergWriter;
import org.apache.nifi.services.iceberg.parquet.io.ParquetFileAppenderFactory;
import org.apache.nifi.services.iceberg.parquet.io.ParquetIcebergRowWriter;
import org.apache.nifi.services.iceberg.parquet.io.ParquetPartitionedWriter;

import java.util.Objects;

@Tags({"parquet", "iceberg", "record"})
@CapabilityDescription("Provides record serialization for Apache Iceberg using Apache Parquet formatting")
public class ParquetIcebergWriter extends AbstractControllerService implements IcebergWriter {

    private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;

    @Override
    public IcebergRowWriter getRowWriter(final Table table) {
        Objects.requireNonNull(table, "Table required");
        final TaskWriter<Record> taskWriter = getTaskWriter(table);
        return new ParquetIcebergRowWriter(taskWriter);
    }

    private TaskWriter<Record> getTaskWriter(final Table table) {
        final PartitionSpec spec = table.spec();
        final Schema schema = table.schema();
        final MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        final FileAppenderFactory<Record> appenderFactory = new ParquetFileAppenderFactory(schema, spec, metricsConfig);

        final int partitionId = spec.specId();
        final long taskId = System.currentTimeMillis();
        final OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId).format(FILE_FORMAT).build();
        final FileIO io = table.io();

        final long writeTargetFileSize;
        final String targetFileSize = table.properties().get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES);
        if (targetFileSize == null || targetFileSize.isEmpty()) {
            writeTargetFileSize = TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
        } else {
            writeTargetFileSize = Long.parseLong(targetFileSize);
        }

        final TaskWriter<Record> taskWriter;
        if (spec.isUnpartitioned()) {
            taskWriter = new UnpartitionedWriter<>(spec, FILE_FORMAT, appenderFactory, outputFileFactory, io, writeTargetFileSize);
        } else {
            taskWriter = new ParquetPartitionedWriter(spec, appenderFactory, outputFileFactory, io, writeTargetFileSize, schema);
        }
        return taskWriter;
    }
}
