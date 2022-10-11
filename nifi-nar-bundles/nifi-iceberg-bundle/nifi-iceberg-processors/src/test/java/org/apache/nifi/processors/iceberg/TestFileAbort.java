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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.apache.nifi.processors.iceberg.catalog.TestHadoopCatalogService;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.writer.IcebergTaskWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TestFileAbort {

    private static final Namespace NAMESPACE = Namespace.of("default");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "abort");

    private static final Schema ABORT_SCHEMA = new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get())
    );

    @DisabledOnOs(WINDOWS)
    @Test
    public void abortUncommittedFiles() throws IOException {
        Table table = initCatalog();

        List<RecordField> recordFields = Collections.singletonList(new RecordField("id", RecordFieldType.INT.getDataType()));
        RecordSchema abortSchema = new SimpleRecordSchema(recordFields);

        List<MapRecord> recordList = new ArrayList<>();
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 1)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 2)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 3)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 4)));
        recordList.add(new MapRecord(abortSchema, Collections.singletonMap("id", 5)));

        IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(table, new Random().nextLong(), FileFormat.PARQUET, null);
        TaskWriter<Record> taskWriter = taskWriterFactory.create();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(table.schema(), abortSchema, FileFormat.PARQUET);

        for (MapRecord record : recordList) {
            taskWriter.write(recordConverter.convert(record));
        }

        DataFile[] dataFiles = taskWriter.dataFiles();

        // DataFiles written by the taskWriter should exist
        for (DataFile dataFile : dataFiles) {
            Assertions.assertTrue(Files.exists(Paths.get(dataFile.path().toString())));
        }

        PutIceberg icebergProcessor = new PutIceberg();
        icebergProcessor.abort(taskWriter.dataFiles(), table);

        // DataFiles shouldn't exist after aborting them
        for (DataFile dataFile : dataFiles) {
            Assertions.assertFalse(Files.exists(Paths.get(dataFile.path().toString())));
        }
    }

    private Table initCatalog() throws IOException {
        TestHadoopCatalogService catalogService = new TestHadoopCatalogService();
        Catalog catalog = catalogService.getCatalog();

        return catalog.createTable(TABLE_IDENTIFIER, ABORT_SCHEMA, PartitionSpec.unpartitioned());
    }
}
