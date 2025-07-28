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
package org.apache.nifi.services.iceberg.parquet.io;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;

/**
 * Parquet implementation of Partition Writer with Partition Key derived from configured Schema definition
 */
public class ParquetPartitionedWriter extends PartitionedFanoutWriter<Record> {

    private final PartitionKey partitionKey;

    public ParquetPartitionedWriter(
            final PartitionSpec spec,
            final FileAppenderFactory<Record> appenderFactory,
            final OutputFileFactory fileFactory,
            final FileIO io,
            final long targetFileSize,
            final Schema schema
    ) {
        super(spec, FileFormat.PARQUET, appenderFactory, fileFactory, io, targetFileSize);
        this.partitionKey = new PartitionKey(spec, schema);
    }

    @Override
    protected PartitionKey partition(final Record record) {
        partitionKey.partition(record);
        return partitionKey;
    }
}
