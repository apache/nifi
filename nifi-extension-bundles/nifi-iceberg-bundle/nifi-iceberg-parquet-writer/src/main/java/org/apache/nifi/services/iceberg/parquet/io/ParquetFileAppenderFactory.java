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
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Parquet implementation of Iceberg File Appender Factory instead of GenericAppenderFactory from iceberg-data library
 */
public class ParquetFileAppenderFactory implements FileAppenderFactory<Record> {
    private final Schema schema;

    private final PartitionSpec spec;

    private final MetricsConfig metricsConfig;

    public ParquetFileAppenderFactory(final Schema schema, final PartitionSpec spec, final MetricsConfig metricsConfig) {
        this.schema = Objects.requireNonNull(schema, "Schema required");
        this.spec = Objects.requireNonNull(spec, "Partition Spec required");
        this.metricsConfig = Objects.requireNonNull(metricsConfig, "Metrics Configuration required");
    }

    @Override
    public FileAppender<Record> newAppender(final OutputFile outputFile, final FileFormat fileFormat) {
        try {
            return Parquet.write(outputFile)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .metricsConfig(metricsConfig)
                    .overwrite()
                    .build();
        } catch (final IOException e) {
            throw new UncheckedIOException("Parquet Appender build failed", e);
        }
    }

    @Override
    public DataWriter<Record> newDataWriter(final EncryptedOutputFile encryptedOutputFile, final FileFormat fileFormat, final StructLike partition) {
        final FileAppender<Record> appender = newAppender(encryptedOutputFile, fileFormat);
        final String location = encryptedOutputFile.encryptingOutputFile().location();
        final EncryptionKeyMetadata keyMetadata = encryptedOutputFile.keyMetadata();
        return new DataWriter<>(appender, fileFormat, location, spec, partition, keyMetadata);
    }

    @Override
    public EqualityDeleteWriter<Record> newEqDeleteWriter(final EncryptedOutputFile encryptedOutputFile, final FileFormat fileFormat, final StructLike partition) {
        throw new UnsupportedOperationException("Equality Delete Writer not supported");
    }

    @Override
    public PositionDeleteWriter<Record> newPosDeleteWriter(final EncryptedOutputFile encryptedOutputFile, final FileFormat fileFormat, final StructLike partition) {
        throw new UnsupportedOperationException("Position Delete Writer not supported");
    }
}
