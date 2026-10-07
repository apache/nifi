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
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParquetIcebergWriterTest {
    private static final String SERVICE_ID = ParquetIcebergWriter.class.getSimpleName();

    private static final String LOCATION = "iceberg://output.parquet";

    private static final int FIRST_FIELD_ID = 0;

    private static final String FIRST_FIELD_NAME = "label";

    private static final String FIRST_FIELD_VALUE = "value";

    private static final int CREATED_FIELD_ID = 1;

    private static final String CREATED_FIELD_NAME = "created";

    private static final LocalDateTime CREATED_FIELD_VALUE = LocalDateTime.of(
            LocalDate.ofEpochDay(1), LocalTime.ofSecondOfDay(0)
    );

    private static final String ID_FIELD_NAME = "id";

    private static final String TAGS_FIELD_NAME = "tags";

    private static final String ADDRESS_FIELD_NAME = "address";

    private static final String CITY_FIELD_NAME = "city";

    private static final String ATTRIBUTES_FIELD_NAME = "attributes";

    private static final String ID_FIELD_VALUE = "row-1";

    private static final String CITY_FIELD_VALUE = "Berlin";

    private static final List<String> TAGS_FIELD_VALUE = List.of("a", "b");

    private static final Map<String, String> ATTRIBUTES_FIELD_VALUE = Map.of("k", "v");

    private static final int DATA_FIELD_ID = 1;

    private static final String DATA_FIELD_NAME = "data";

    private static final int DIGEST_FIELD_ID = 2;

    private static final String DIGEST_FIELD_NAME = "digest";

    private static final int IDENTIFIER_FIELD_ID = 3;

    private static final String IDENTIFIER_FIELD_NAME = "identifier";

    private static final int GEOMETRY_FIELD_ID = 1;

    private static final String GEOMETRY_FIELD_NAME = "geometry";

    private static final int GEOGRAPHY_FIELD_ID = 2;

    private static final String GEOGRAPHY_FIELD_NAME = "geography";

    private static final byte[] DATA_FIELD_VALUE = new byte[] {1, 2, 3, 4};

    private static final UUID IDENTIFIER_FIELD_VALUE = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");

    private static final double POINT_LONGITUDE = 24.9384;

    private static final double POINT_LATITUDE = 60.1699;

    // Well-Known Binary encoding of a Point with little-endian byte order, Point geometry type, and X and Y coordinates
    private static final byte[] POINT_FIELD_VALUE = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1)
            .putInt(1)
            .putDouble(POINT_LONGITUDE)
            .putDouble(POINT_LATITUDE)
            .array();

    private static final String CATALOG_NAME = "memory";

    private static final TableIdentifier GEOSPATIAL_TABLE_IDENTIFIER = TableIdentifier.of(Namespace.of("default"), "geospatial");

    private static final String FORMAT_VERSION_3 = "3";

    private ParquetIcebergWriter parquetIcebergWriter;

    private TestRunner runner;

    @Mock
    private Table table;

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
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        setTable(schema, partitionSpec, outputFile);
        when(locationProvider.newDataLocation(anyString())).thenReturn(LOCATION);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);

        assertNotNull(rowWriter);
    }

    @Test
    void testWriteDataFiles() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Schema schema = getSchema();
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        setTable(schema, partitionSpec, outputFile);
        when(locationProvider.newDataLocation(anyString())).thenReturn(LOCATION);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);
        writeRow(schema, rowWriter);

        final DataFile[] dataFiles = rowWriter.dataFiles();
        final byte[] serialized = outputFile.toByteArray();
        assertDataFilesFound(dataFiles, serialized);
    }

    @Test
    void testWriteDataFilesPartitioned() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Schema schema = getSchema();
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        final PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity(FIRST_FIELD_NAME).build();
        setTable(schema, partitionSpec, outputFile);
        when(locationProvider.newDataLocation(eq(partitionSpec), isA(StructLike.class), anyString())).thenReturn(LOCATION);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);
        writeRow(schema, rowWriter);

        final DataFile[] dataFiles = rowWriter.dataFiles();
        final byte[] serialized = outputFile.toByteArray();
        assertDataFilesFound(dataFiles, serialized);
    }

    @Test
    void testWriteDataFilesPartitionedTimestamp() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Types.NestedField firstNestedField = Types.NestedField.required(FIRST_FIELD_ID, FIRST_FIELD_NAME, Types.StringType.get());
        final Types.NestedField createdNestedField = Types.NestedField.required(CREATED_FIELD_ID, CREATED_FIELD_NAME, Types.TimestampType.withoutZone());
        final Schema schema = new Schema(firstNestedField, createdNestedField);
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        final PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity(CREATED_FIELD_NAME).build();

        setTable(schema, partitionSpec, outputFile);
        when(locationProvider.newDataLocation(eq(partitionSpec), isA(StructLike.class), anyString())).thenReturn(LOCATION);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);
        final GenericRecord row = GenericRecord.create(schema);
        row.setField(FIRST_FIELD_NAME, FIRST_FIELD_VALUE);
        row.setField(CREATED_FIELD_NAME, CREATED_FIELD_VALUE);
        rowWriter.write(row);

        final DataFile[] dataFiles = rowWriter.dataFiles();
        final byte[] serialized = outputFile.toByteArray();
        assertDataFilesFound(dataFiles, serialized);

        final DataFile dataFile = dataFiles[0];
        final StructLike partition = dataFile.partition();
        assertInstanceOf(PartitionData.class, partition);
        final PartitionData partitionData = (PartitionData) partition;
        final Object partitionField = partitionData.get(0);

        final long microsecondsExpected = DateTimeUtil.microsFromTimestamp(CREATED_FIELD_VALUE);
        assertEquals(microsecondsExpected, partitionField);
    }

    @Test
    void testWriteDataFilesComplexTypes() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Types.StructType nestedStruct = Types.StructType.of(
                Types.NestedField.optional(10, CITY_FIELD_NAME, Types.StringType.get())
        );
        final Schema schema = new Schema(
                Types.NestedField.required(1, ID_FIELD_NAME, Types.StringType.get()),
                Types.NestedField.optional(2, TAGS_FIELD_NAME,
                        Types.ListType.ofOptional(3, Types.StringType.get())),
                Types.NestedField.optional(4, ADDRESS_FIELD_NAME, nestedStruct),
                Types.NestedField.optional(5, ATTRIBUTES_FIELD_NAME,
                        Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.StringType.get()))
        );
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        setTable(schema, partitionSpec, outputFile);
        when(locationProvider.newDataLocation(anyString())).thenReturn(LOCATION);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);

        final GenericRecord address = GenericRecord.create(nestedStruct);
        address.setField(CITY_FIELD_NAME, CITY_FIELD_VALUE);

        final GenericRecord row = GenericRecord.create(schema);
        row.setField(ID_FIELD_NAME, ID_FIELD_VALUE);
        row.setField(TAGS_FIELD_NAME, TAGS_FIELD_VALUE);
        row.setField(ADDRESS_FIELD_NAME, address);
        row.setField(ATTRIBUTES_FIELD_NAME, ATTRIBUTES_FIELD_VALUE);
        rowWriter.write(row);

        final DataFile[] dataFiles = rowWriter.dataFiles();
        final byte[] serialized = outputFile.toByteArray();
        assertDataFilesFound(dataFiles, serialized);
    }

    /**
     * Iceberg Parquet writers require ByteBuffer values for binary columns, byte arrays for fixed columns, and UUID values
     * for uuid columns, and record the written values as column bounds.
     */
    @Test
    void testWriteDataFilesBinaryFixedUuidTypes() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Types.FixedType fixedType = Types.FixedType.ofLength(DATA_FIELD_VALUE.length);
        final Schema schema = new Schema(
                Types.NestedField.optional(DATA_FIELD_ID, DATA_FIELD_NAME, Types.BinaryType.get()),
                Types.NestedField.optional(DIGEST_FIELD_ID, DIGEST_FIELD_NAME, fixedType),
                Types.NestedField.optional(IDENTIFIER_FIELD_ID, IDENTIFIER_FIELD_NAME, Types.UUIDType.get())
        );
        final InMemoryOutputFile outputFile = new InMemoryOutputFile();
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        setTable(schema, partitionSpec, outputFile);
        when(locationProvider.newDataLocation(anyString())).thenReturn(LOCATION);

        final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(table);

        final GenericRecord row = GenericRecord.create(schema);
        row.setField(DATA_FIELD_NAME, ByteBuffer.wrap(DATA_FIELD_VALUE));
        row.setField(DIGEST_FIELD_NAME, DATA_FIELD_VALUE);
        row.setField(IDENTIFIER_FIELD_NAME, IDENTIFIER_FIELD_VALUE);
        rowWriter.write(row);

        final DataFile[] dataFiles = rowWriter.dataFiles();
        final byte[] serialized = outputFile.toByteArray();
        assertDataFilesFound(dataFiles, serialized);

        final Map<Integer, ByteBuffer> lowerBounds = dataFiles[0].lowerBounds();
        assertEquals(ByteBuffer.wrap(DATA_FIELD_VALUE), Conversions.fromByteBuffer(Types.BinaryType.get(), lowerBounds.get(DATA_FIELD_ID)));
        assertEquals(ByteBuffer.wrap(DATA_FIELD_VALUE), Conversions.fromByteBuffer(fixedType, lowerBounds.get(DIGEST_FIELD_ID)));
        assertEquals(IDENTIFIER_FIELD_VALUE, Conversions.fromByteBuffer(Types.UUIDType.get(), lowerBounds.get(IDENTIFIER_FIELD_ID)));
    }

    /**
     * Iceberg geometry and geography types require table format version 3, and Iceberg Parquet writers require Well-Known
     * Binary values in ByteBuffers for both types, recording the bounding box of geometry values as column bounds.
     */
    @Test
    void testWriteDataFilesGeospatialTypes() throws IOException {
        runner.enableControllerService(parquetIcebergWriter);

        final Types.GeometryType geometryType = Types.GeometryType.crs84();
        final Schema schema = new Schema(
                Types.NestedField.optional(GEOMETRY_FIELD_ID, GEOMETRY_FIELD_NAME, geometryType),
                Types.NestedField.optional(GEOGRAPHY_FIELD_ID, GEOGRAPHY_FIELD_NAME, Types.GeographyType.crs84())
        );

        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize(CATALOG_NAME, Map.of());
            catalog.createNamespace(GEOSPATIAL_TABLE_IDENTIFIER.namespace());
            final Table geospatialTable = catalog.buildTable(GEOSPATIAL_TABLE_IDENTIFIER, schema)
                    .withProperty(TableProperties.FORMAT_VERSION, FORMAT_VERSION_3)
                    .create();

            final IcebergRowWriter rowWriter = parquetIcebergWriter.getRowWriter(geospatialTable);

            final GenericRecord row = GenericRecord.create(schema);
            row.setField(GEOMETRY_FIELD_NAME, ByteBuffer.wrap(POINT_FIELD_VALUE));
            row.setField(GEOGRAPHY_FIELD_NAME, ByteBuffer.wrap(POINT_FIELD_VALUE));
            rowWriter.write(row);

            final DataFile[] dataFiles = rowWriter.dataFiles();
            assertEquals(1, dataFiles.length);

            final DataFile dataFile = dataFiles[0];
            final GeospatialBound pointBound = GeospatialBound.createXY(POINT_LONGITUDE, POINT_LATITUDE);
            assertEquals(pointBound, Conversions.fromByteBuffer(geometryType, dataFile.lowerBounds().get(GEOMETRY_FIELD_ID)));
            assertEquals(pointBound, Conversions.fromByteBuffer(geometryType, dataFile.upperBounds().get(GEOMETRY_FIELD_ID)));

            geospatialTable.newAppend().appendFile(dataFile).commit();
            assertEquals("1", geospatialTable.currentSnapshot().summary().get(SnapshotSummary.ADDED_RECORDS_PROP));
        }
    }

    private void writeRow(final Schema schema, final IcebergRowWriter rowWriter) throws IOException {
        final GenericRecord row = GenericRecord.create(schema);
        row.setField(FIRST_FIELD_NAME, FIRST_FIELD_VALUE);
        rowWriter.write(row);
    }

    private void assertDataFilesFound(final DataFile[] dataFiles, final byte[] serialized) {
        assertNotNull(dataFiles);
        assertEquals(1, dataFiles.length);

        final DataFile dataFile = dataFiles[0];
        assertNotNull(dataFile);
        assertEquals(FileFormat.PARQUET, dataFile.format());
        assertEquals(1, dataFile.recordCount());

        assertEquals(serialized.length, dataFile.fileSizeInBytes());
    }

    private Schema getSchema() {
        final Types.NestedField nestedField = Types.NestedField.required(FIRST_FIELD_ID, FIRST_FIELD_NAME, Types.StringType.get());
        return new Schema(nestedField);
    }

    private void setTable(final Schema schema, final PartitionSpec spec, final OutputFile outputFile) {
        when(table.schema()).thenReturn(schema);
        when(table.spec()).thenReturn(spec);
        when(table.io()).thenReturn(io);
        when(table.locationProvider()).thenReturn(locationProvider);
        when(table.encryption()).thenReturn(encryptionManager);
        when(io.newOutputFile(eq(LOCATION))).thenReturn(outputFile);
        when(encryptionManager.encrypt(eq(outputFile))).thenReturn(encryptedOutputFile);
        when(encryptedOutputFile.encryptingOutputFile()).thenReturn(outputFile);
    }
}
