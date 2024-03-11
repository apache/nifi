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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.processors.iceberg.catalog.TestHadoopCatalogService;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.writer.IcebergTaskWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.nifi.processors.iceberg.PutIceberg.MAXIMUM_COMMIT_DURATION;
import static org.apache.nifi.processors.iceberg.PutIceberg.MAXIMUM_COMMIT_WAIT_TIME;
import static org.apache.nifi.processors.iceberg.PutIceberg.MINIMUM_COMMIT_WAIT_TIME;
import static org.apache.nifi.processors.iceberg.PutIceberg.NUMBER_OF_COMMIT_RETRIES;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.condition.OS.WINDOWS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDataFileActions {

    private static final Namespace NAMESPACE = Namespace.of("default");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "abort");

    private static final Schema ABORT_SCHEMA = new Schema(
            Types.NestedField.required(0, "id", Types.IntegerType.get())
    );

    private PutIceberg icebergProcessor;
    private ComponentLog logger;

    @BeforeEach
    public void setUp() {
        icebergProcessor = new PutIceberg();
        logger = new MockComponentLog("id", "TestDataFileActions");
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testAbortUncommittedFiles() throws IOException {
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

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(table.schema(), abortSchema, FileFormat.PARQUET, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);

        for (MapRecord record : recordList) {
            taskWriter.write(recordConverter.convert(record));
        }

        DataFile[] dataFiles = taskWriter.dataFiles();

        // DataFiles written by the taskWriter should exist
        for (DataFile dataFile : dataFiles) {
            Assertions.assertTrue(Files.exists(Paths.get(dataFile.path().toString())));
        }

        icebergProcessor.abort(taskWriter.dataFiles(), table);

        // DataFiles shouldn't exist after aborting them
        for (DataFile dataFile : dataFiles) {
            Assertions.assertFalse(Files.exists(Paths.get(dataFile.path().toString())));
        }
    }

    @Test
    public void testAppenderCommitRetryExceeded() {
        ProcessContext context = Mockito.mock(ProcessContext.class);
        when(context.getProperty(NUMBER_OF_COMMIT_RETRIES)).thenReturn(new MockPropertyValue("3", null));
        when(context.getProperty(MINIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_DURATION)).thenReturn(new MockPropertyValue("1 min", null));

        FlowFile mockFlowFile = new MockFlowFile(1234567890L);
        AppendFiles appender = Mockito.mock(AppendFiles.class);
        doThrow(CommitFailedException.class).when(appender).commit();

        Table table = Mockito.mock(Table.class);
        when(table.newAppend()).thenReturn(appender);

        // assert the commit action eventually fails after exceeding the number of retries
        assertThrows(CommitFailedException.class, () -> icebergProcessor.appendDataFiles(context, mockFlowFile, table, WriteResult.builder().build()));

        // verify the commit action was called the configured number of times
        verify(appender, times(4)).commit();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAppenderCommitSucceeded() {
        ProcessContext context = Mockito.mock(ProcessContext.class);
        when(context.getProperty(NUMBER_OF_COMMIT_RETRIES)).thenReturn(new MockPropertyValue("3", null));
        when(context.getProperty(MINIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("1 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_DURATION)).thenReturn(new MockPropertyValue("1 min", null));

        FlowFile mockFlowFile = new MockFlowFile(1234567890L);
        AppendFiles appender = Mockito.mock(AppendFiles.class);
        // the commit action should throw exception 2 times before succeeding
        doThrow(CommitFailedException.class, CommitFailedException.class).doNothing().when(appender).commit();

        Table table = Mockito.mock(Table.class);
        when(table.newAppend()).thenReturn(appender);

        // the method call shouldn't throw exception since the configured number of retries is higher than the number of failed commit actions
        icebergProcessor.appendDataFiles(context, mockFlowFile, table, WriteResult.builder().build());

        // verify the proper number of commit action was called
        verify(appender, times(3)).commit();
    }

    @Test
    public void testMaxCommitDurationExceeded() {
        ProcessContext context = Mockito.mock(ProcessContext.class);
        when(context.getProperty(NUMBER_OF_COMMIT_RETRIES)).thenReturn(new MockPropertyValue("5", null));
        when(context.getProperty(MINIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("2 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_WAIT_TIME)).thenReturn(new MockPropertyValue("2 ms", null));
        when(context.getProperty(MAXIMUM_COMMIT_DURATION)).thenReturn(new MockPropertyValue("1 ms", null));

        FlowFile mockFlowFile = new MockFlowFile(1234567890L);
        AppendFiles appender = Mockito.mock(AppendFiles.class);
        doThrow(CommitFailedException.class).when(appender).commit();

        Table table = Mockito.mock(Table.class);
        when(table.newAppend()).thenReturn(appender);

        // assert the commit action eventually fails after exceeding duration of maximum retries
        assertThrows(CommitFailedException.class, () -> icebergProcessor.appendDataFiles(context, mockFlowFile, table, WriteResult.builder().build()));

        // verify the commit action was called only 2 times instead of the configured 5
        verify(appender, times(2)).commit();
    }

    private Table initCatalog() throws IOException {
        TestHadoopCatalogService catalogService = new TestHadoopCatalogService();
        IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
        Catalog catalog = catalogFactory.create();

        return catalog.createTable(TABLE_IDENTIFIER, ABORT_SCHEMA, PartitionSpec.unpartitioned());
    }
}
