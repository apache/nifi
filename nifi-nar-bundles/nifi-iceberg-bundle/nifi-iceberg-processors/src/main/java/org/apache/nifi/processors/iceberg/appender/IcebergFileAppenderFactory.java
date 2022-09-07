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
package org.apache.nifi.processors.iceberg.appender;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.nifi.processors.iceberg.appender.avro.IcebergAvroWriter;
import org.apache.nifi.processors.iceberg.appender.orc.IcebergOrcWriter;
import org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.UncheckedIOException;

public class IcebergFileAppenderFactory implements FileAppenderFactory<Record> {

    private final Schema schema;
    private final Table table;
    private final PartitionSpec spec;
    private final RecordSchema recordSchema;

    public IcebergFileAppenderFactory(Schema schema, Table table, PartitionSpec spec, RecordSchema recordSchema) {
        this.schema = schema;
        this.table = table;
        this.spec = spec;
        this.recordSchema = recordSchema;
    }

    @Override
    public FileAppender<Record> newAppender(OutputFile outputFile, FileFormat fileFormat) {
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        try {
            switch (fileFormat) {
                case AVRO:
                    return Avro.write(outputFile)
                            .schema(schema)
                            .createWriterFunc(ignore -> new IcebergAvroWriter(recordSchema))
                            .setAll(table.properties())
                            .metricsConfig(metricsConfig)
                            .overwrite()
                            .build();

                case PARQUET:
                    return Parquet.write(outputFile)
                            .schema(schema)
                            .createWriterFunc(messageType -> IcebergParquetWriter.buildWriter(recordSchema, messageType))
                            .setAll(table.properties())
                            .metricsConfig(metricsConfig)
                            .overwrite()
                            .build();

                case ORC:
                    return ORC.write(outputFile)
                            .schema(schema)
                            .createWriterFunc((schema, typDesc) -> IcebergOrcWriter.buildWriter(recordSchema, schema))
                            .setAll(table.properties())
                            .metricsConfig(metricsConfig)
                            .overwrite()
                            .build();

                default:
                    throw new UnsupportedOperationException("Cannot write unknown file format: " + fileFormat);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public DataWriter<Record> newDataWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        return new DataWriter<>(
                newAppender(outputFile.encryptingOutputFile(), format), format, outputFile.encryptingOutputFile().location(), spec, partition, outputFile.keyMetadata());
    }

    @Override
    public EqualityDeleteWriter<Record> newEqDeleteWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        return null;    //Delete write is not supported.
    }

    @Override
    public PositionDeleteWriter<Record> newPosDeleteWriter(EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
        return null;    //Delete write is not supported.
    }
}
