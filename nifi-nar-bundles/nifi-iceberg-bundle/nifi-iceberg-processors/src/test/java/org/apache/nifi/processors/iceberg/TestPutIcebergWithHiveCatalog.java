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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.hive.metastore.ThriftMetastore;
import org.apache.nifi.processors.iceberg.catalog.TestHiveCatalogService;
import org.apache.nifi.processors.iceberg.util.IcebergTestUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.iceberg.PutIceberg.ICEBERG_RECORD_COUNT;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateData;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validatePartitionFolders;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TestPutIcebergWithHiveCatalog {

    private TestRunner runner;
    private PutIceberg processor;
    private Schema inputSchema;

    @RegisterExtension
    public ThriftMetastore metastore = new ThriftMetastore();

    private static final Namespace NAMESPACE = Namespace.of("test_metastore");

    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "users");

    private static final org.apache.iceberg.Schema USER_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "department", Types.StringType.get())
    );

    @BeforeEach
    public void setUp() throws Exception {
        String avroSchema = IOUtils.toString(Files.newInputStream(Paths.get("src/test/resources/user.avsc")), StandardCharsets.UTF_8);
        inputSchema = new Schema.Parser().parse(avroSchema);

        processor = new PutIceberg();
    }

    private void initRecordReader() throws InitializationException {
        MockRecordParser readerFactory = new MockRecordParser();
        RecordSchema recordSchema = AvroTypeUtil.createSchema(inputSchema);

        for (RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        readerFactory.addRecord(0, "John", "Finance");
        readerFactory.addRecord(1, "Jill", "Finance");
        readerFactory.addRecord(2, "James", "Marketing");
        readerFactory.addRecord(3, "Joana", "Sales");

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutIceberg.RECORD_READER, "mock-reader-factory");
    }

    private Catalog initCatalog(PartitionSpec spec, String fileFormat) throws InitializationException {
        TestHiveCatalogService catalogService = new TestHiveCatalogService(metastore.getThriftConnectionUri(), metastore.getWarehouseLocation());
        Catalog catalog = catalogService.getCatalog();

        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat);

        catalog.createTable(TABLE_IDENTIFIER, USER_SCHEMA, spec, tableProperties);

        runner.addControllerService("catalog-service", catalogService);
        runner.enableControllerService(catalogService);

        runner.setProperty(PutIceberg.CATALOG, "catalog-service");

        return catalog;
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerPartitioned(String fileFormat) throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(USER_SCHEMA)
                .bucket("department", 3)
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(spec, fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "users");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        List<Record> expectedRecords = IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
                .add(0, "John", "Finance")
                .add(1, "Jill", "Finance")
                .add(2, "James", "Marketing")
                .add(3, "Joana", "Sales")
                .build();

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        String tableLocation = new URI(table.location()).getPath();
        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateData(table, expectedRecords, 0);
        validateNumberOfDataFiles(tableLocation, 3);
        validatePartitionFolders(tableLocation, Arrays.asList(
                "department_bucket=0", "department_bucket=1", "department_bucket=2"));
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerIdentityPartitioned(String fileFormat) throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(USER_SCHEMA)
                .identity("department")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(spec, fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "users");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        List<Record> expectedRecords = IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
                .add(0, "John", "Finance")
                .add(1, "Jill", "Finance")
                .add(2, "James", "Marketing")
                .add(3, "Joana", "Sales")
                .build();

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        String tableLocation = new URI(table.location()).getPath();
        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateData(table, expectedRecords, 0);
        validateNumberOfDataFiles(tableLocation, 3);
        validatePartitionFolders(tableLocation, Arrays.asList(
                "department=Finance", "department=Marketing", "department=Sales"));
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerMultiLevelIdentityPartitioned(String fileFormat) throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(USER_SCHEMA)
                .identity("name")
                .identity("department")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(spec, fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "users");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        List<Record> expectedRecords = IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
                .add(0, "John", "Finance")
                .add(1, "Jill", "Finance")
                .add(2, "James", "Marketing")
                .add(3, "Joana", "Sales")
                .build();

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        String tableLocation = new URI(table.location()).getPath();
        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateData(table, expectedRecords, 0);
        validateNumberOfDataFiles(tableLocation, 4);
        validatePartitionFolders(tableLocation, Arrays.asList(
                "name=James/department=Marketing/",
                "name=Jill/department=Finance/",
                "name=Joana/department=Sales/",
                "name=John/department=Finance/"
        ));
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @ValueSource(strings = {"avro", "orc", "parquet"})
    public void onTriggerUnPartitioned(String fileFormat) throws Exception {
        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        Catalog catalog = initCatalog(PartitionSpec.unpartitioned(), fileFormat);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "test_metastore");
        runner.setProperty(PutIceberg.TABLE_NAME, "users");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        List<Record> expectedRecords = IcebergTestUtils.RecordsBuilder.newInstance(USER_SCHEMA)
                .add(0, "John", "Finance")
                .add(1, "Jill", "Finance")
                .add(2, "James", "Marketing")
                .add(3, "Joana", "Sales")
                .build();

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isUnpartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateData(table, expectedRecords, 0);
        validateNumberOfDataFiles(new URI(table.location()).getPath(), 1);
    }
}
