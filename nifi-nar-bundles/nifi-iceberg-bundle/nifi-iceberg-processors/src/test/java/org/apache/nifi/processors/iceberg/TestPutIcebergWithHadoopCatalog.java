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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.iceberg.catalog.TestHadoopCatalogService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validatePartitionFolders;

public class TestPutIcebergWithHadoopCatalog {

    private TestRunner runner;
    private PutIceberg processor;
    private Schema inputSchema;

    public static Namespace namespace = Namespace.of("test_metastore");

    public static TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "date");

    public static org.apache.iceberg.Schema DATE_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "timeMicros", Types.TimeType.get()),
            Types.NestedField.required(2, "timestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "date", Types.DateType.get())
    );

    @BeforeEach
    public void setUp() throws Exception {
        final String avroSchema = IOUtils.toString(Files.newInputStream(Paths.get("src/test/resources/date.avsc")), StandardCharsets.UTF_8);
        inputSchema = new Schema.Parser().parse(avroSchema);

        processor = new PutIceberg();
    }

    private void initRecordReader() throws InitializationException {
        MockRecordParser readerFactory = new MockRecordParser();
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(inputSchema);

        for (final RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2015-02-02 15:30:30.800"), Date.valueOf("2015-02-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2015-02-02 15:30:30.800"), Date.valueOf("2015-02-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2016-01-02 15:30:30.800"), Date.valueOf("2016-01-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2017-01-10 15:30:30.800"), Date.valueOf("2017-01-10"));

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutIceberg.RECORD_READER, "mock-reader-factory");
    }

    private Catalog initCatalog(PartitionSpec spec, String fileFormat) throws InitializationException, IOException {
        final TestHadoopCatalogService catalogService = new TestHadoopCatalogService();
        Catalog catalog = catalogService.getCatalog();

        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat);

        catalog.createTable(tableIdentifier, DATE_SCHEMA, spec, tableProperties);

        runner.addControllerService("catalog-service", catalogService);
        runner.enableControllerService(catalogService);

        runner.setProperty(PutIceberg.CATALOG, "catalog-service");

        return catalog;
    }

    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerYearTransform(String fileFormat) throws Exception {
        final PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .year("date")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(spec, fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "date");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(tableIdentifier);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList("date_year=2015", "date_year=2016", "date_year=2017"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerMonthTransform(String fileFormat) throws Exception {
        final PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .month("timestampMicros")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(spec, fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "date");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(tableIdentifier);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList(
                "timestampMicros_month=2015-02", "timestampMicros_month=2016-01", "timestampMicros_month=2017-01"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerDayTransform(String fileFormat) throws Exception {
        final PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .day("timestampMicros")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(spec, fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "date");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(tableIdentifier);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList(
                "timestampMicros_day=2015-02-02", "timestampMicros_day=2016-01-02", "timestampMicros_day=2017-01-10"));
    }
}
