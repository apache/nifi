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
package org.apache.nifi.processors.hudi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORDKEY_FIELDS;
import static org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_CLASS_NAME;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE;
import static org.apache.nifi.processors.hudi.PutHudi.HUDI_RECORD_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPutHudi {

    private static final String TABLE_NAME = "hudi_table";
    private static final String DATABASE_NAME = "default";

    @TempDir
    private java.nio.file.Path tempDir;

    private final Configuration hadoopConf = new Configuration();
    private TestRunner runner;
    private PutHudi processor;
    private Schema inputSchema;
    private String basePath;

    @BeforeEach
    public void setUp() throws Exception {
        final String avroSchema = IOUtils.toString(Files.newInputStream(Paths.get("src/test/resources/user.avsc")), StandardCharsets.UTF_8);
        inputSchema = new Schema.Parser().parse(avroSchema);
        basePath = tempDir.resolve("hudi_tests" + System.currentTimeMillis()).toAbsolutePath().toUri().getPath();
        processor = new PutHudi();
    }

    private void initRecordReader(List<TestUser> records) throws InitializationException {
        final MockRecordParser readerFactory = new MockRecordParser();
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(inputSchema);

        for (RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        for (TestUser testUser : records) {
            readerFactory.addRecord(testUser.getId(), testUser.getName(), testUser.getDepartment());
        }

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);
        runner.setProperty(PutHudi.RECORD_READER, "mock-reader-factory");
    }

    private HoodieTableMetaClient initMetaClient(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                                 Properties properties, String schema) throws IOException {
        final HoodieTableMetaClient.PropertyBuilder builder =
                HoodieTableMetaClient.withPropertyBuilder()
                        .setDatabaseName(DATABASE_NAME)
                        .setTableName(TABLE_NAME)
                        .setTableType(tableType)
                        .setTableCreateSchema(schema)
                        .setPayloadClass(HoodieAvroPayload.class);

        final Properties processedProperties = builder.fromProperties(properties).build();
        return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, processedProperties);
    }

    @Test
    public void testHiveStylePartitionedTableWithSimpleKeyGenerator() throws Exception {
        //init incoming records
        final List<TestUser> records = new ArrayList<>();
        records.add(new TestUser(0, "John", "Finance"));
        records.add(new TestUser(1, "Jill", "Finance"));
        records.add(new TestUser(2, "James", "Finance"));
        records.add(new TestUser(3, "Joana", "Sales"));

        //init table properties
        final TypedProperties properties = new TypedProperties();
        properties.put(KEYGENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.SimpleAvroKeyGenerator");
        properties.put(RECORDKEY_FIELDS.key(), "id");
        properties.put(PARTITION_FIELDS.key(), "department");
        properties.put(HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");

        //init table meta client
        final HoodieTableMetaClient metaClient = initMetaClient(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE, new TypedProperties(properties), inputSchema.toString());
        final BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

        runner = TestRunners.newTestRunner(processor);
        initRecordReader(records);
        runner.setProperty(PutHudi.TABLE_PATH, basePath);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHudi.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHudi.REL_SUCCESS).get(0);

        assertEquals("4", flowFile.getAttribute(HUDI_RECORD_COUNT));

        // assert 'Finance' partition path and files
        List<GenericRecord> fileRecords = fileUtils.readAvroRecords(hadoopConf, new Path(basePath + "/department=Finance"));
        assertEquals(3, fileRecords.size());

        for (GenericRecord genericRecord : fileRecords) {
            assertEquals("Finance", genericRecord.get("department").toString());
        }

        // assert 'Sales' partition path and files
        fileRecords = fileUtils.readAvroRecords(hadoopConf, new Path(basePath + "/department=Sales"));
        assertEquals(1, fileRecords.size());
        assertEquals("Sales", fileRecords.get(0).get("department").toString());
    }

    @Test
    public void testUnpartitionedTable() throws Exception {
        //init incoming records
        final List<TestUser> records = new ArrayList<>();
        records.add(new TestUser(0, "John", "Finance"));
        records.add(new TestUser(1, "Jill", "Finance"));
        records.add(new TestUser(2, "James", "Marketing"));
        records.add(new TestUser(3, "Joana", "Sales"));

        //init table meta client
        final HoodieTableMetaClient metaClient = initMetaClient(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, new TypedProperties(), inputSchema.toString());
        final BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

        runner = TestRunners.newTestRunner(processor);
        initRecordReader(records);
        runner.setProperty(PutHudi.TABLE_PATH, basePath);
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHudi.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHudi.REL_SUCCESS).get(0);

        assertEquals("4", flowFile.getAttribute(HUDI_RECORD_COUNT));

        //assert created files
        final List<GenericRecord> fileRecords = fileUtils.readAvroRecords(hadoopConf, new Path(basePath));
        assertEquals(4, fileRecords.size());

        int index = 0;
        for (GenericRecord genericRecord : fileRecords) {
            assertEquals(records.get(index).getId(), genericRecord.get("id"));
            assertEquals(records.get(index).getName(), genericRecord.get("name").toString());
            assertEquals(records.get(index).getDepartment(), genericRecord.get("department").toString());
            index++;
        }
    }

    @Test
    public void testPartitionedTableWithComplexKeyGeneratorAndDynamicFields() throws Exception {
        //init incoming records
        final List<TestUser> records = new ArrayList<>();
        records.add(new TestUser(0, "John", "Finance"));
        records.add(new TestUser(1, "James", "Finance"));
        records.add(new TestUser(2, "James", "Finance"));

        //init table meta client
        final HoodieTableMetaClient metaClient = initMetaClient(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE, new TypedProperties(), inputSchema.toString());
        final BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

        runner = TestRunners.newTestRunner(processor);
        initRecordReader(records);
        runner.setProperty(PutHudi.TABLE_PATH, basePath);
        runner.setProperty("hoodie.datasource.write.partitionpath.field", "department,name");
        runner.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexAvroKeyGenerator");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutHudi.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHudi.REL_SUCCESS).get(0);

        assertEquals("3", flowFile.getAttribute(HUDI_RECORD_COUNT));

        // assert '/Finance/James' partition path and files
        List<GenericRecord> fileRecords = fileUtils.readAvroRecords(hadoopConf, new Path(basePath + "/Finance/James"));
        assertEquals(2, fileRecords.size());

        for (GenericRecord genericRecord : fileRecords) {
            assertEquals("Finance", genericRecord.get("department").toString());
            assertEquals("James", genericRecord.get("name").toString());
        }

        // assert '/Finance/John' partition path and files
        fileRecords = fileUtils.readAvroRecords(hadoopConf, new Path(basePath + "/Finance/John"));
        assertEquals(1, fileRecords.size());
        assertEquals("Finance", fileRecords.get(0).get("department").toString());
        assertEquals("John", fileRecords.get(0).get("name").toString());
    }

    private class TestUser {
        private final long id;
        private final String name;
        private final String department;

        public TestUser(long id, String name, String department) {
            this.id = id;
            this.name = name;
            this.department = department;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getDepartment() {
            return department;
        }
    }

}
