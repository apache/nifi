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
package org.apache.nifi.cdc.postgresql.processors;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.Driver;
import org.postgresql.replication.LogSequenceNumber;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;


public class ITCaptureChangePostgreSQL {

    private static final DockerImageName POSTGRES_TEST_IMAGE = DockerImageName.parse("postgres:10");

    private static final String JSON_PATH_TYPE = "$.type";
    // simple messages
    private static final String JSON_PATH_TEST_COLUMN = "$.tupleData.test_column";
    private static final String JSON_PATH_RELATION_NAME = "$.relationName";
    // snapshot messages
    private static final String JSON_PATH_SNAPSHOT_RELATION_NAME = "$.snapshot.relationName";
    private static final String JSON_PATH_SNAPSHOT_TEST_COLUM = "$.snapshot.tupleData[0].test_column";
    // complex messages
    private static final String JSON_PATH_TUPLE_TYPE = "$.tupleType";
    private static final String JSON_PATH_TUPLE_DATA_VALUES = "$.tupleData.values";
    private static final String JSON_PATH_TUPLE_TYPE_1 = "$.tupleType1";
    private static final String JSON_PATH_TUPLE_DATA_1_VALUES = "$.tupleData1.values";
    private static final String JSON_PATH_TUPLE_TYPE_2 = "$.tupleType2";
    private static final String JSON_PATH_TUPLE_DATA_2_VALUES = "$.tupleData2.values";

    private TestRunner testRunner;

    @Before
    public void init() {
        // Docker is not available for MacOS en Windows NiFi Github actions jobs
        assumeTrue("Docker is not available", DockerClientFactory.instance().isDockerAvailable());
        testRunner = TestRunners.newTestRunner(CaptureChangePostgreSQL.class);
    }

    @Test
    public void cdcProcessorWithExistingSlotProducingSimpleEvents() throws Exception {
        try (PostgreSQLContainer<?> postgres = createPostgresContainerForLogicalReplication()) {
            postgres.start();

            createTestTable(postgres);
            createPublication(postgres);
            assertThat(createReplicationSlot(postgres), equalTo(true));

            initTestRunnerWithDefaultProperties(postgres);

            testRunner.assertValid();

            assertThat(insertTestData(postgres), equalTo(1));
            assertThat(updateTestData(postgres), equalTo(1));
            assertThat(deleteTestData(postgres), equalTo(1));

            testRunner.run();


            // assert resulting flow files
            List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
            assertEquals(3, flowFiles.size());

            MockFlowFile insertFlowFile = flowFiles.get(0);
            DocumentContext insertJson = JsonPath.parse(insertFlowFile.getContent());
            assertThat(insertJson.read(JSON_PATH_RELATION_NAME), containsString("test_table"));
            assertThat(insertJson.read(JSON_PATH_TYPE), equalTo("insert"));
            assertThat(insertJson.read(JSON_PATH_TEST_COLUMN), equalTo("test_data"));

            MockFlowFile updateFlowFile = flowFiles.get(1);
            DocumentContext updateJson = JsonPath.parse(updateFlowFile.getContent());
            assertThat(updateJson.read(JSON_PATH_RELATION_NAME), containsString("test_table"));
            assertThat(updateJson.read(JSON_PATH_TYPE), equalTo("update"));
            assertThat(updateJson.read(JSON_PATH_TEST_COLUMN), equalTo("test_data_update"));

            MockFlowFile deleteFlowFile = flowFiles.get(2);
            DocumentContext deleteJson = JsonPath.parse(deleteFlowFile.getContent());
            assertThat(deleteJson.read(JSON_PATH_RELATION_NAME), containsString("test_table"));
            assertThat(deleteJson.read(JSON_PATH_TYPE), equalTo("delete"));
            assertThat(deleteJson.read(JSON_PATH_TEST_COLUMN), nullValue());
        }
    }

    @Test
    public void cdcProcessorWithExistingSlotProducingComplexEvents() throws Exception {
        try (PostgreSQLContainer<?> postgres = createPostgresContainerForLogicalReplication()) {
            postgres.start();

            createTestTable(postgres);
            createPublication(postgres);
            assertThat(createReplicationSlot(postgres), equalTo(true));

            initTestRunnerWithDefaultProperties(postgres);
            testRunner.setProperty(CaptureChangePostgreSQL.SIMPLE_EVENTS, Boolean.FALSE.toString());

            testRunner.assertValid();

            assertThat(insertTestData(postgres), equalTo(1));
            assertThat(updateTestData(postgres), equalTo(1));
            assertThat(deleteTestData(postgres), equalTo(1));

            testRunner.run();


            // assert resulting flow files
            List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
            assertEquals(4, flowFiles.size()); // 3 DML + 1 relation

            MockFlowFile relationFlowFile = flowFiles.get(0);
            DocumentContext relationJson = JsonPath.parse(relationFlowFile.getContent());
            assertThat(relationJson.read(JSON_PATH_RELATION_NAME), containsString("test_table"));
            assertThat(relationJson.read(JSON_PATH_TYPE), equalTo("relation"));

            MockFlowFile insertFlowFile = flowFiles.get(1);
            DocumentContext insertJson = JsonPath.parse(insertFlowFile.getContent());
            assertThat(insertJson.read(JSON_PATH_TUPLE_TYPE), equalTo("N")); // New
            assertThat(insertJson.read(JSON_PATH_TUPLE_DATA_VALUES), containsString("test_data,"));
            assertThat(insertJson.read(JSON_PATH_TYPE), equalTo("insert"));

            MockFlowFile updateFlowFile = flowFiles.get(2);
            DocumentContext updateJson = JsonPath.parse(updateFlowFile.getContent());
            assertThat(updateJson.read(JSON_PATH_TUPLE_TYPE_1), equalTo("N")); // New
            assertThat(updateJson.read(JSON_PATH_TUPLE_DATA_1_VALUES), containsString("test_data_update,"));
            assertThat(updateJson.read(JSON_PATH_TYPE), equalTo("update"));

            MockFlowFile deleteFlowFile = flowFiles.get(3);
            DocumentContext deleteJson = JsonPath.parse(deleteFlowFile.getContent());
            assertThat(deleteJson.read(JSON_PATH_TUPLE_TYPE), equalTo("K")); // Key
            assertThat(deleteJson.read(JSON_PATH_TYPE), equalTo("delete"));
        }
    }

    @Test
    public void cdcProcessorWithExistingSlotReplicaFullProducingCommitMessagesAndComplexEvents() throws Exception {
        try (PostgreSQLContainer<?> postgres = createPostgresContainerForLogicalReplication()) {
            postgres.start();

            createTestTable(postgres);
            setTestTableReplicaIdentityFull(postgres);
            createPublication(postgres);
            assertThat(createReplicationSlot(postgres), equalTo(true));

            initTestRunnerWithDefaultProperties(postgres);
            testRunner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());
            testRunner.setProperty(CaptureChangePostgreSQL.SIMPLE_EVENTS, Boolean.FALSE.toString());

            testRunner.assertValid();

            assertThat(insertTestData(postgres), equalTo(1));
            assertThat(updateTestData(postgres), equalTo(1));
            assertThat(deleteTestData(postgres), equalTo(1));

            testRunner.run();


            // assert resulting flow files
            List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
            assertEquals(10, flowFiles.size()); // 3 DML operations each having being / commit message -> 9 + 1 relation message

            MockFlowFile updateFlowFile = flowFiles.get(5);
            DocumentContext updateJson = JsonPath.parse(updateFlowFile.getContent());
            assertThat(updateJson.read(JSON_PATH_TUPLE_TYPE_1), equalTo("O")); // Old
            assertThat(updateJson.read(JSON_PATH_TUPLE_DATA_1_VALUES), containsString("test_data,"));
            assertThat(updateJson.read(JSON_PATH_TUPLE_TYPE_2), equalTo("N")); // New
            assertThat(updateJson.read(JSON_PATH_TUPLE_DATA_2_VALUES), containsString("test_data_update,"));
        }
    }

    @Test
    public void cdcProcessorWithExistingSlotProducingSimpleEventsAndCommitMessagesFromLsn() throws Exception {
        try (PostgreSQLContainer<?> postgres = createPostgresContainerForLogicalReplication()) {
            postgres.start();

            createTestTable(postgres);
            createPublication(postgres);
            assertThat(createReplicationSlot(postgres), equalTo(true));
            // do some DML
            assertThat(insertTestData(postgres), equalTo(1));
            assertThat(insertTestData(postgres), equalTo(1));
            assertThat(deleteTestData(postgres), equalTo(2));
            assertThat(insertTestData(postgres), equalTo(1));
            long lsn = getCurrentLsn(postgres);
            assertThat(updateTestData(postgres), equalTo(1));

            testRunner.setProperty(CaptureChangePostgreSQL.INIT_LSN, String.valueOf(lsn));
            initTestRunnerWithDefaultProperties(postgres);
            testRunner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, Boolean.TRUE.toString());

            testRunner.assertValid();

            testRunner.run();


            // assert resulting flow files
            List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
            assertEquals(3, flowFiles.size());

            MockFlowFile beginFlowFile = flowFiles.get(0);
            DocumentContext beginJson = JsonPath.parse(beginFlowFile.getContent());
            assertThat(beginJson.read(JSON_PATH_TYPE), equalTo("begin"));

            MockFlowFile updateFlowFile = flowFiles.get(1);
            DocumentContext updateJson = JsonPath.parse(updateFlowFile.getContent());
            assertThat(updateJson.read(JSON_PATH_RELATION_NAME), containsString("test_table"));
            assertThat(updateJson.read(JSON_PATH_TYPE), equalTo("update"));
            assertThat(updateJson.read(JSON_PATH_TEST_COLUMN), equalTo("test_data_update"));

            MockFlowFile commitFlowFile = flowFiles.get(2);
            DocumentContext commitJson = JsonPath.parse(commitFlowFile.getContent());
            assertThat(commitJson.read(JSON_PATH_TYPE), equalTo("commit"));
        }
    }

    @Test
    public void cdcProcessorWithDriverLocationWithDropsAndRecreatesSlotProducingSnapshot() throws Exception {
        try (PostgreSQLContainer<?> postgres = createPostgresContainerForLogicalReplication()) {
            postgres.start();

            createTestTable(postgres);
            createPublication(postgres);
            assertThat(createReplicationSlot(postgres), equalTo(true));

            initTestRunnerWithDefaultProperties(postgres);
            testRunner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, Boolean.TRUE.toString());
            testRunner.setProperty(CaptureChangePostgreSQL.SNAPSHOT, Boolean.TRUE.toString());
            // determine JAR file location of Postgres JDBC driver
            URL location = Driver.class.getClassLoader().getResource(Driver.class.getCanonicalName().replace(".", "/") + ".class");
            assertThat(location, notNullValue());
            assertThat(location.toString(), startsWith("jar:"));
            assertThat(location.toString(), containsString("!"));
            testRunner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, location.toString().substring(location.toString().indexOf("jar:") + 4, location.toString().indexOf("!")));

            testRunner.assertValid();

            assertThat(insertTestData(postgres), equalTo(1));
            assertThat(updateTestData(postgres), equalTo(1));

            testRunner.run();


            // assert resulting flow files
            List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
            assertEquals(1, flowFiles.size());

            MockFlowFile snapshotFlowFile = flowFiles.get(0);
            DocumentContext snapshotJson = JsonPath.parse(snapshotFlowFile.getContent());
            assertThat(snapshotJson.read(JSON_PATH_SNAPSHOT_RELATION_NAME), containsString("test_table"));
            assertThat(snapshotJson.read(JSON_PATH_SNAPSHOT_TEST_COLUM), equalTo("test_data_update"));
        }
    }

    private PostgreSQLContainer<?> createPostgresContainerForLogicalReplication() {
        return new PostgreSQLContainer<>(POSTGRES_TEST_IMAGE)
                .withDatabaseName("test_db")
                .withUsername("unit")
                .withPassword("test")
                //.withLogConsumer(out -> System.out.println(out.getUtf8String()))
                .withCommand("postgres -c wal_level=logical");
    }

    private void initTestRunnerWithDefaultProperties(PostgreSQLContainer<?> postgres) {
        testRunner.setProperty(CaptureChangePostgreSQL.DATABASE_NAME, postgres.getDatabaseName());
        testRunner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, postgres.getDriverClassName());
        testRunner.setProperty(CaptureChangePostgreSQL.USERNAME, postgres.getUsername());
        testRunner.setProperty(CaptureChangePostgreSQL.PASSWORD, postgres.getPassword());
        testRunner.setProperty(CaptureChangePostgreSQL.HOST, postgres.getHost() + ":" + postgres.getFirstMappedPort());
        testRunner.setProperty(CaptureChangePostgreSQL.SLOT_NAME, "test_slot");
        testRunner.setProperty(CaptureChangePostgreSQL.PUBLICATION, "test_publication");
    }

    private Connection getConnection(PostgreSQLContainer<?> postgres) throws SQLException {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private int executeUpdate(PostgreSQLContainer<?> postgres, String query) throws SQLException {
        try (Connection connection = getConnection(postgres);
             PreparedStatement stmt = connection.prepareStatement(query)) {
            return stmt.executeUpdate();
        }
    }

    private void createTestTable(PostgreSQLContainer<?> postgres) throws SQLException {
        executeUpdate(postgres, "CREATE TABLE test_table (id bigserial primary key, test_column text, ts timestamp default current_timestamp);");
    }

    private void createPublication(PostgreSQLContainer<?> postgres) throws SQLException {
        executeUpdate(postgres, "CREATE PUBLICATION test_publication FOR TABLE test_table;");
    }

    private void setTestTableReplicaIdentityFull(PostgreSQLContainer<?> postgres) throws SQLException {
        executeUpdate(postgres, "ALTER TABLE test_table REPLICA IDENTITY FULL;");
    }

    private boolean createReplicationSlot(PostgreSQLContainer<?> postgres) throws SQLException {
        try (Connection connection = getConnection(postgres);
             PreparedStatement stmt = connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput');");
             ResultSet rs = stmt.executeQuery()) {
            return rs.next();
        }
    }

    private int insertTestData(PostgreSQLContainer<?> postgres) throws SQLException {
        return executeUpdate(postgres, "INSERT INTO test_table(test_column) VALUES ('test_data');");
    }

    private int updateTestData(PostgreSQLContainer<?> postgres) throws SQLException {
        return executeUpdate(postgres, "UPDATE test_table SET test_column = 'test_data_update';");
    }

    private long getCurrentLsn(PostgreSQLContainer<?> postgres) throws SQLException {
        try (Connection connection = getConnection(postgres);
             PreparedStatement stmt = connection.prepareStatement("SELECT pg_current_wal_insert_lsn();");
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return LogSequenceNumber.valueOf(rs.getString(1)).asLong();
            }
            throw new IllegalStateException("Unable to obtain current lsn");
        }
    }

    private int deleteTestData(PostgreSQLContainer<?> postgres) throws SQLException {
        return executeUpdate(postgres, "DELETE FROM test_table;");
    }

}
