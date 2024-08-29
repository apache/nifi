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

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.iceberg.HadoopCatalogService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.iceberg.PutIceberg.ICEBERG_RECORD_COUNT;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.CATALOG_NAME;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.CATALOG_SERVICE;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.NAMESPACE;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.RECORD_READER_SERVICE;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.createTemporaryDirectory;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validatePartitionFolders;
import static org.apache.nifi.services.iceberg.HadoopCatalogService.WAREHOUSE_PATH;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TestPutIcebergWithHadoopCatalog {

    protected static final String TABLE_NAME = "date_test";

    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, TABLE_NAME);

    private static final org.apache.iceberg.Schema DATE_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "timeMicros", Types.TimeType.get()),
            Types.NestedField.required(2, "timestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "date", Types.DateType.get())
    );

    private TestRunner runner;
    private PutIceberg processor;
    private static Schema inputSchema;
    private Catalog catalog;


    @BeforeAll
    public static void initSchema() throws IOException {
        final String avroSchema = IOUtils.toString(Files.newInputStream(Paths.get("src/test/resources/date.avsc")), StandardCharsets.UTF_8);
        inputSchema = new Schema.Parser().parse(avroSchema);
    }

    @BeforeEach
    public void setUp() {
        processor = new PutIceberg();
    }

    private void initRecordReader() throws InitializationException {
        final MockRecordParser readerFactory = new MockRecordParser();
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(inputSchema);

        for (RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2015-02-02 15:30:30.800"), Date.valueOf("2015-02-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2015-02-02 15:30:30.800"), Date.valueOf("2015-02-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2016-01-02 15:30:30.800"), Date.valueOf("2016-01-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2017-01-10 15:30:30.800"), Date.valueOf("2017-01-10"));

        runner.addControllerService(RECORD_READER_SERVICE, readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutIceberg.RECORD_READER, RECORD_READER_SERVICE);
    }

    private void initCatalog(PartitionSpec spec, FileFormat fileFormat) throws InitializationException {
        final File warehousePath = createTemporaryDirectory();
        final HadoopCatalogService catalogService = new HadoopCatalogService();
        runner.addControllerService(CATALOG_SERVICE, catalogService);
        runner.setProperty(catalogService, WAREHOUSE_PATH, warehousePath.getAbsolutePath());
        runner.enableControllerService(catalogService);

        final IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
        catalog = catalogFactory.create();

        final Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());

        catalog.createTable(TABLE_IDENTIFIER, DATE_SCHEMA, spec, tableProperties);

        runner.setProperty(PutIceberg.CATALOG, CATALOG_SERVICE);
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void onTriggerYearTransform() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .year("date")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        initCatalog(spec, FileFormat.PARQUET);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        final Table table = catalog.loadTable(TABLE_IDENTIFIER);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).getFirst();

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList("date_year=2015", "date_year=2016", "date_year=2017"));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void onTriggerMonthTransform() throws Exception {
        final PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .month("timestampMicros")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        initCatalog(spec, FileFormat.ORC);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        final Table table = catalog.loadTable(TABLE_IDENTIFIER);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).getFirst();

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList(
                "timestampMicros_month=2015-02", "timestampMicros_month=2016-01", "timestampMicros_month=2017-01"));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void onTriggerDayTransform() throws Exception {
        final PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .day("timestampMicros")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        initCatalog(spec, FileFormat.AVRO);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAME);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        final Table table = catalog.loadTable(TABLE_IDENTIFIER);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).getFirst();

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList(
                "timestampMicros_day=2015-02-02", "timestampMicros_day=2016-01-02", "timestampMicros_day=2017-01-10"));
    }
}
