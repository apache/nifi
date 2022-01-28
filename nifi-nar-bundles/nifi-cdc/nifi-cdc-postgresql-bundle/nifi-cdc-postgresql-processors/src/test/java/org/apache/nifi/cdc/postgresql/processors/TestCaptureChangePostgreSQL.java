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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.apache.nifi.cdc.postgresql.event.MockReader;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Unit Test for PostgreSQL CDC Processor.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestCaptureChangePostgreSQL {

    private static final String DRIVER_NAME = "org.postgresql.Driver";
    private static final String HOST = "localhost";
    private static final String PORT = "5432";
    private static final String DATABASE = "db_test";
    private static final String USERNAME = "nifi";
    private static final String PASSWORD = "Change1t!";
    private static final String CONNECTION_TIMEOUT = "30 seconds";
    private static final String PUBLICATION = "pub_city";
    private static final String REPLICATION_SLOT = "slot_city";

    TestRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(new MockCaptureChangePostgreSQL());
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void test01RequiredProperties() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);
        runner.assertValid();
    }

    @Test
    public void test02CDCBeginCommitNotIncludedAllMetadataNotIncluded() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        runner.run();
        // Expected only 1 flow file (INSERT event).
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);

        final MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeEquals("cdc.lsn", "101");
        insert.assertContentEquals(
                "{\"tupleData\":{\"id\":8},\"relationName\":\"public.tb_city\",\"lsn\":101,\"type\":\"insert\"}",
                StandardCharsets.UTF_8);
    }

    @Test
    public void test03CDCBeginCommitIncludedAllMetadataNotIncluded() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        runner.setValidateExpressionUsage(false);

        runner.run();
        // Expected 3 flow files (BEGIN, INSERT and COMMIT events).
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 3);

        // BEGIN
        final MockFlowFile begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeEquals("cdc.lsn", "101");

        String beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2000-01-01 00:00:00 BRT -0200\",\"type\""); // Replaced to skip different TZ.
        assertTrue(beginContent.equals(
                "{\"xid\":14,\"lsn\":101,\"xCommitTime\":\"2000-01-01 00:00:00 BRT -0200\",\"type\":\"begin\",\"xLSNFinal\":101}"));

        // INSERT
        final MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(1);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeEquals("cdc.lsn", "101");
        insert.assertContentEquals(
                "{\"tupleData\":{\"id\":8},\"relationName\":\"public.tb_city\",\"lsn\":101,\"type\":\"insert\"}",
                StandardCharsets.UTF_8);

        // COMMIT
        final MockFlowFile commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(2);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeEquals("cdc.lsn", "102");

        String commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2000-01-01 00:00:01 BRT -0200\",\"type\""); // Replaced to skip different TZ.
        assertTrue(commitContent.equals(
                "{\"lsn\":102,\"flags\":0,\"xCommitTime\":\"2000-01-01 00:00:01 BRT -0200\",\"type\":\"commit\",\"commitLSN\":102,\"xLSNEnd\":102}"));
    }

    @Test
    public void test04CDCBeginCommitIncludedAllMetadataIncluded() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "true");

        runner.run();
        // Expected 4 flow files (BEGIN, RELATION, INSERT and COMMIT events).
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 4);

        // BEGIN
        final MockFlowFile begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeEquals("cdc.lsn", "101");

        String beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2000-01-01 00:00:00 BRT -0200\",\"type\""); // Replaced to skip different TZ.
        assertTrue(beginContent.equals(
                "{\"xid\":14,\"lsn\":101,\"xCommitTime\":\"2000-01-01 00:00:00 BRT -0200\",\"type\":\"begin\",\"xLSNFinal\":101}"));

        // RELATION
        final MockFlowFile relation = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(1);
        assertNotNull(relation);
        relation.assertAttributeEquals("cdc.type", "relation");
        relation.assertAttributeNotExists("cdc.lsn");
        relation.assertContentEquals(
                "{\"columns\":{\"1\":{\"position\":1,\"isKey\":false,\"name\":\"id\",\"dataTypeId\":23,\"dataTypeName\":\"int4\","
                        + "\"typeModifier\":-1}},\"name\":\"tb_city\",\"objectName\":\"public.tb_city\",\"replicaIdentity\":\"f\",\"id\":14,"
                        + "\"type\":\"relation\",\"numColumns\":1}",
                StandardCharsets.UTF_8);

        // INSERT
        final MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(2);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeEquals("cdc.lsn", "101");
        insert.assertContentEquals(
                "{\"tupleData\":{\"id\":8},\"relationName\":\"public.tb_city\",\"lsn\":101,\"relationId\":14,\"tupleType\":\"N\",\"type\":\"insert\"}",
                StandardCharsets.UTF_8);

        // COMMIT
        final MockFlowFile commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(3);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeEquals("cdc.lsn", "102");

        String commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2000-01-01 00:00:01 BRT -0200\",\"type\""); // Replaced to skip different TZ.
        assertTrue(commitContent.equals(
                "{\"lsn\":102,\"flags\":0,\"xCommitTime\":\"2000-01-01 00:00:01 BRT -0200\",\"type\":\"commit\",\"commitLSN\":102,\"xLSNEnd\":102}"));
    }

    @Test
    public void test05State() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        runner.getStateManager().assertStateNotSet(Scope.CLUSTER);

        runner.run();
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);

        runner.getStateManager().assertStateEquals("last.received.lsn", "103", Scope.CLUSTER);
    }

    @Test
    public void test06DataProvenance() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        runner.run();
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);

        runner.getProvenanceEvents().forEach(event -> assertEquals(event.getEventType(), ProvenanceEventType.CREATE));
    }

    /**
     * Mock CaptureChangePostgreSQL class
     */
    class MockCaptureChangePostgreSQL extends CaptureChangePostgreSQL {
        protected Long maxFlowFileListSizeTest = 4L;

        @Override
        protected void connect(String host, String port, String database, String username, String password,
                String driverLocation, String driverName, long timeout)
                throws TimeoutException, IOException {
        }

        @Override
        protected Connection getQueryConnection() throws SQLException {
            return mock(Connection.class);
        }

        @Override
        protected Connection getReplicationConnection() throws SQLException {
            return mock(Connection.class);
        }

        @Override
        protected void closeQueryConnection() {
        }

        @Override
        protected void closeReplicationConnection() {
        }

        @Override
        protected void createReplicationReader(String slot, boolean dropSlotIfExists, String publication, Long lsn,
                boolean includeBeginCommit, boolean includeAllMetadata, Connection replicationConn,
                Connection queryConn) throws SQLException {

            this.replicationReader = new MockReader(lsn, includeBeginCommit, includeAllMetadata);
        }

        @Override
        protected Long getMaxFlowFileListSize() {
            return maxFlowFileListSizeTest;
        }
    }

}
