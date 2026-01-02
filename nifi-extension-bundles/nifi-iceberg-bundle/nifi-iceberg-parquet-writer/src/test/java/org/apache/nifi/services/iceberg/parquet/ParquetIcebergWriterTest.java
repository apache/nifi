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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.iceberg.IcebergRowWriter;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParquetIcebergWriterTest {
    private static final String SERVICE_ID = ParquetIcebergWriter.class.getSimpleName();

    private static final String LOCATION = "iceberg://output.parquet";

    private static final int FIRST_FIELD_ID = 0;

    private static final String FIRST_FIELD_NAME = "label";

    private static final String FIRST_FIELD_VALUE = "value";

    private ParquetIcebergWriter parquetIcebergWriter;

    private TestRunner runner;

    @Mock
    private Table table;

    @Mock
    private PartitionSpec spec;

    @Mock
    private FileIO io;

    @Mock
    private LocationProvider locationProvider;

    @Mock
    private EncryptionManager encryptionManager;

    @Mock
    private EncryptedOutputFile encryptedOutputFile;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        parquetIcebergWriter = new ParquetIcebergWriter();
        runner.addControllerService(SERVICE_ID, parquetIcebergWriter);
    }

    @Test
    void testEnabledDisabled() {
        runner.enableControllerService(parquetIcebergWriter);

        runner.disableControllerService(parquetIcebergWriter);
    }

    @Test
    void testGetRowWriter() {
        runner.enableControllerService(parquetIcebergWriter);

        final Schema schema = getSchema();
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        setTable(schema, outputFile);
        when(spec.isUnpartitioned()).thenReturn(true);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);

        assertNotNull(rowWriter);
    }

    @Test
    void testWriteDataFiles() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Schema schema = getSchema();
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        setTable(schema, outputFile);
        when(spec.isUnpartitioned()).thenReturn(true);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);

        final GenericRecord row = GenericRecord.create(schema);
        row.setField(FIRST_FIELD_NAME, FIRST_FIELD_VALUE);
        rowWriter.write(row);

        final DataFile[] dataFiles = rowWriter.dataFiles();
        assertNotNull(dataFiles);
        assertEquals(1, dataFiles.length);

        final DataFile dataFile = dataFiles[0];
        assertNotNull(dataFile);
        assertEquals(FileFormat.PARQUET, dataFile.format());
        assertEquals(1, dataFile.recordCount());

        final byte[] serialized = outputFile.toByteArray();
        assertEquals(serialized.length, dataFile.fileSizeInBytes());
    }

    private Schema getSchema() {
        final Types.NestedField nestedField = Types.NestedField.required(FIRST_FIELD_ID, FIRST_FIELD_NAME, Types.StringType.get());
        return new Schema(nestedField);
    }

    private void setTable(final Schema schema, final OutputFile outputFile) {
        when(table.schema()).thenReturn(schema);
        when(table.spec()).thenReturn(spec);
        when(table.io()).thenReturn(io);
        when(table.locationProvider()).thenReturn(locationProvider);
        when(table.encryption()).thenReturn(encryptionManager);
        when(locationProvider.newDataLocation(anyString())).thenReturn(LOCATION);
        when(io.newOutputFile(eq(LOCATION))).thenReturn(outputFile);
        when(encryptionManager.encrypt(eq(outputFile))).thenReturn(encryptedOutputFile);
        when(encryptedOutputFile.encryptingOutputFile()).thenReturn(outputFile);
    }
}
