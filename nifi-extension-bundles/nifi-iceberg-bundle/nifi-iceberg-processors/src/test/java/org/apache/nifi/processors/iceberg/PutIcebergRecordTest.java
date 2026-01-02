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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.iceberg.IcebergCatalog;
import org.apache.nifi.services.iceberg.IcebergRowWriter;
import org.apache.nifi.services.iceberg.IcebergWriter;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PutIcebergRecordTest {
    private static final String ICEBERG_CATALOG_ID = IcebergCatalog.class.getName();

    private static final String ICEBERG_WRITER_ID = IcebergWriter.class.getName();

    private static final String RECORD_READER_ID = RecordReaderFactory.class.getName();

    private static final String NAMESPACE = PutIcebergRecord.class.getSimpleName();

    private static final String TABLE_NAME = TableIdentifier.class.getSimpleName();

    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    private static final byte[] EMPTY = new byte[0];

    private static final String FIELD_NAME = "standardField";

    private static final String FIELD_VALUE = String.class.getName();

    private static final RecordSchema RECORD_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField(FIELD_NAME, RecordFieldType.STRING.getDataType())
    ));

    private static final Schema TABLE_SCHEMA = new Schema(List.of(
            Types.NestedField.required(1, FIELD_NAME, Types.StringType.get())
    ));

    private static final Record STANDARD_RECORD = new MapRecord(RECORD_SCHEMA, Map.of(FIELD_NAME, FIELD_VALUE));

    @Mock
    private IcebergCatalog icebergCatalog;

    @Mock
    private Catalog catalog;

    @Mock
    private Table table;

    @Mock
    private AppendFiles appendFiles;

    @Mock
    private IcebergWriter icebergWriter;

    @Mock
    private IcebergRowWriter icebergRowWriter;

    @Mock
    private DataFile dataFile;

    @Mock
    private RecordReaderFactory recordReaderFactory;

    @Mock
    private RecordReader recordReader;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(PutIcebergRecord.class);

        setProperties();
    }

    @Test
    void testRunNoFlowFiles() {
        runner.run();
    }

    @Test
    void testRunNoDataFiles() throws Exception {
        runner.enqueue(EMPTY);

        setLoadTable();
        setRecordReader();
        setWriter();

        runner.run();

        runner.assertAllFlowFilesTransferred(PutIcebergRecord.SUCCESS);
    }

    @Test
    void testRunWriteFailed() throws Exception {
        runner.enqueue(EMPTY);

        setLoadTable();
        setRecordReader();

        when(recordReader.nextRecord()).thenReturn(STANDARD_RECORD).thenReturn(null);
        when(icebergWriter.getRowWriter(any(Table.class))).thenReturn(icebergRowWriter);
        doThrow(new IOException()).when(icebergRowWriter).write(any());

        runner.run();

        runner.assertAllFlowFilesTransferred(PutIcebergRecord.FAILURE);
    }

    @Test
    void testRunSuccess() throws Exception {
        runner.enqueue(EMPTY);

        setLoadTable();
        setRecordReader();
        setWriter();

        when(recordReader.nextRecord()).thenReturn(STANDARD_RECORD, STANDARD_RECORD, null);

        final DataFile[] dataFiles = new DataFile[]{dataFile};
        when(icebergRowWriter.dataFiles()).thenReturn(dataFiles);

        runner.run();

        runner.assertAllFlowFilesTransferred(PutIcebergRecord.SUCCESS);

        final Long recordsProcessed = runner.getCounterValue(PutIcebergRecord.RECORDS_PROCESSED_COUNTER);
        assertEquals(2, recordsProcessed);

        final Long dataFilesProcessed = runner.getCounterValue(PutIcebergRecord.DATA_FILES_PROCESSED_COUNTER);
        assertEquals(dataFiles.length, dataFilesProcessed);
    }

    private void setLoadTable() {
        when(catalog.tableExists(eq(TABLE_IDENTIFIER))).thenReturn(true);
        when(catalog.loadTable(eq(TABLE_IDENTIFIER))).thenReturn(table);
        when(table.schema()).thenReturn(TABLE_SCHEMA);
    }

    private void setRecordReader() throws Exception {
        when(recordReaderFactory.createRecordReader(any(FlowFile.class), any(), any())).thenReturn(recordReader);
    }

    private void setWriter() throws Exception {
        when(icebergWriter.getRowWriter(any(Table.class))).thenReturn(icebergRowWriter);
        when(icebergRowWriter.dataFiles()).thenReturn(new DataFile[0]);
        when(table.newAppend()).thenReturn(appendFiles);
        when(table.location()).thenReturn(TABLE_IDENTIFIER.toString());
    }

    private void setProperties() throws InitializationException {
        when(icebergCatalog.getCatalog()).thenReturn(catalog);

        when(icebergCatalog.getIdentifier()).thenReturn(ICEBERG_CATALOG_ID);
        runner.addControllerService(ICEBERG_CATALOG_ID, icebergCatalog);
        runner.enableControllerService(icebergCatalog);
        runner.setProperty(PutIcebergRecord.ICEBERG_CATALOG, ICEBERG_CATALOG_ID);

        when(icebergWriter.getIdentifier()).thenReturn(ICEBERG_WRITER_ID);
        runner.addControllerService(ICEBERG_WRITER_ID, icebergWriter);
        runner.enableControllerService(icebergWriter);
        runner.setProperty(PutIcebergRecord.ICEBERG_WRITER, ICEBERG_WRITER_ID);

        when(recordReaderFactory.getIdentifier()).thenReturn(RECORD_READER_ID);
        runner.addControllerService(RECORD_READER_ID, recordReaderFactory);
        runner.enableControllerService(recordReaderFactory);
        runner.setProperty(PutIcebergRecord.RECORD_READER, RECORD_READER_ID);

        runner.setProperty(PutIcebergRecord.NAMESPACE, NAMESPACE);
        runner.setProperty(PutIcebergRecord.TABLE_NAME, TABLE_NAME);
    }
}
