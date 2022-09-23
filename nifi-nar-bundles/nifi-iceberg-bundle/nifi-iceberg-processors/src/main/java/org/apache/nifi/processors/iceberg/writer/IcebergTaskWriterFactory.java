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
package org.apache.nifi.processors.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Factory class to create the suitable {@link TaskWriter} based on the {@link Table}'s properties
 */
public class IcebergTaskWriterFactory {

    private final Schema schema;
    private final PartitionSpec spec;
    private final FileIO io;
    private final long targetFileSize;
    private final FileFormat fileFormat;
    private final FileAppenderFactory<Record> appenderFactory;
    private final OutputFileFactory outputFileFactory;

    public IcebergTaskWriterFactory(Table table, long taskId, FileFormat fileFormat, String targetFileSize) {
        this.schema = table.schema();
        this.spec = table.spec();
        this.io = table.io();
        this.fileFormat = fileFormat;

        this.targetFileSize = targetFileSize != null ? Long.parseLong(targetFileSize) :
                PropertyUtil.propertyAsLong(table.properties(), TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        this.outputFileFactory = OutputFileFactory.builderFor(table, table.spec().specId(), taskId).format(fileFormat).build();
        this.appenderFactory = new GenericAppenderFactory(schema, spec);
    }

    public TaskWriter<Record> create() {
        if (spec.isUnpartitioned()) {
            return new UnpartitionedWriter<>(spec, fileFormat, appenderFactory, outputFileFactory, io, targetFileSize);
        } else {
            return new IcebergPartitionedWriter(spec, fileFormat, appenderFactory, outputFileFactory, io, targetFileSize, schema);
        }
    }
}
