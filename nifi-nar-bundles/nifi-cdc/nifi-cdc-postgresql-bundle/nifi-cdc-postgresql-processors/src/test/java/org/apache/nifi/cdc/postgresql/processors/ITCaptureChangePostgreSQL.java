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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PSQLException;

/**
 * Integration Test for PostgreSQL CDC Processor.
 *
 * The PostgreSQL Cluster should be configured to enable logical replication:
 *
 * - Property listen_addresses should be set to the hostname to listen to;
 *
 * - Property max_wal_senders should be the number of replication consumers;
 *
 * - Property wal_keep_size (or wal_keep_segments in PostgreSQL versions
 * before 13) specifies the minimum size of past WAL segments kept in the WAL
 * directory;
 *
 * - Property wal_level should be logical;
 *
 * - Property max_replication_slots should be greater than zero.
 *
 * For example:
 *
 * <code>
 * [postgresql.conf]
 *
 * listen_addresses = '*'
 * max_wal_senders = 4
 * wal_keep_size = 4
 * wal_level = logical
 * max_replication_slots = 4
 * </code>
 *
 * This Integration Tests requires two database users, but only one with
 * superuser privilege.
 *
 * For example:
 *
 * <code>
 * CREATE USER nifi SUPERUSER PASSWORD 'justForT3sts';
 * CREATE USER intern PASSWORD 'justForT3sts';
 * </code>
 *
 * Authorize the users for query and replication connections.
 *
 * For example:
 *
 * <code>
 * [pg_hba.conf]
 *
 * host db_test nifi 192.168.56.0/24 md5
 * host db_test intern 192.168.56.0/24 md5
 * host replication nifi 192.168.56.0/24 md5
 * host replication intern 192.168.56.0/24 md5
 * </code>
 *
 * These settings in postgresql.conf and pg_hba.conf files require restarting
 * the PostgreSQL service.
 *
 * Finally, export the environment variables used by the tests.
 *
 * For example:
 *
 * <code>
 * $ export PG_JDBC_DRIVER_LOCATION="/usr/share/java/postgresql-42.3.1.jar";
 * $ export PG_JDBC_DRIVER_NAME="org.postgresql.Driver";
 * $ export PG_HOST="192.168.56.101";
 * $ export PG_PORT="5432";
 * $ export PG_DATABASE="db_test";
 * $ export PG_SUPER_USER="nifi";
 * $ export PG_SUPER_USER_PASS="justForT3sts";
 * $ export PG_ORDINARY_USER="intern";
 * $ export PG_ORDINARY_USER_PASS="justForT3sts";
 * $ export PG_REPLICATION_SLOT="slot_city";
 * </code>
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITCaptureChangePostgreSQL {

    private String DRIVER_LOCATION;
    private String DRIVER_NAME;
    private String HOST;
    private String PORT;
    private String DATABASE;
    private String SUPER_USER;
    private String SUPER_USER_PASSWORD;
    private String ORDINARY_USER;
    private String ORDINARY_USER_PASSWORD;
    private String REPLICATION_SLOT;

    // This publication is created through the Integratin Tests by
    // ITPostgreSQLClient
    private static final String PUBLICATION = "pub_city";

    ITPostgreSQLClient client;
    TestRunner runner;

    public ITCaptureChangePostgreSQL() {
        // Reading enviornment variables
        DRIVER_LOCATION = System.getenv("PG_JDBC_DRIVER_LOCATION");
        DRIVER_NAME = System.getenv("PG_JDBC_DRIVER_NAME");
        HOST = System.getenv("PG_HOST");
        PORT = System.getenv("PG_PORT");
        DATABASE = System.getenv("PG_DATABASE");
        SUPER_USER = System.getenv("PG_SUPER_USER");
        SUPER_USER_PASSWORD = System.getenv("PG_SUPER_USER_PASS");
        ORDINARY_USER = System.getenv("PG_ORDINARY_USER");
        ORDINARY_USER_PASSWORD = System.getenv("PG_ORDINARY_USER_PASS");
        REPLICATION_SLOT = System.getenv("PG_REPLICATION_SLOT");
    }

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(new CaptureChangePostgreSQL());
        client = new ITPostgreSQLClient();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;

        if (client != null) {
            if (client.isConnected()) {
                client.dropTable();
                client.closeConnection();
            }
            client = null;
        }
    }

    @Test
    public void test01RequiredProperties() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);
        runner.assertValid();
    }

    @Test
    public void test02RegisterDriverFailure() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);
        runner.assertValid();

        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, "/tmp/postgrez.jar"); // Incorrect driver
                                                                                          // location
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.assertValid();

        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, "org.postgrez.Driver"); // Incorrect driver name

        AssertionError ae = assertThrows(AssertionError.class, () -> runner.run());
        Throwable pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        Throwable rue = pe.getCause();
        assertTrue(rue instanceof RuntimeException);
        assertEquals(
                "Failed to register JDBC driver. Ensure PostgreSQL Driver Location(s) and "
                        + "PostgreSQL Driver Class Name are configured correctly",
                rue.getMessage());
        Throwable ine = rue.getCause();
        assertTrue(ine instanceof InitializationException);
        assertEquals("Can't load Database Driver", ine.getMessage());

        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
    }

    @Test
    public void test03ConnectionFailures() {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        /* -------------------- Incorrect IP -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.HOST, "192.168.56.199");

        AssertionError ae = assertThrows(AssertionError.class, () -> runner.run());
        Throwable pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        Throwable ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create SQL connection to specified host and port", ioe.getMessage());
        Throwable psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);

        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);

        /* -------------------- Incorrect Port -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.PORT, "5199");

        ae = assertThrows(AssertionError.class, () -> runner.run());
        pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create SQL connection to specified host and port", ioe.getMessage());
        psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);
        assertEquals(
                "refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
                psqle.getMessage().substring(34, 144));

        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);

        /* -------------------- Access not authorized -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.DATABASE, "postgres");

        ae = assertThrows(AssertionError.class, () -> runner.run());
        pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create SQL connection to specified host and port", ioe.getMessage());
        psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);
        assertEquals("FATAL: no pg_hba.conf entry for host", psqle.getMessage().substring(0, 36));

        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);

        /* -------------------- Database doesn't exists -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.DATABASE, "db_fake");

        ae = assertThrows(AssertionError.class, () -> runner.run());
        pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create SQL connection to specified host and port", ioe.getMessage());
        psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);
        assertEquals("FATAL: no pg_hba.conf entry for host", psqle.getMessage().substring(0, 36));

        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);

        /* -------------------- User doesn't exists -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.USERNAME, "test");

        ae = assertThrows(AssertionError.class, () -> runner.run());
        pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create SQL connection to specified host and port", ioe.getMessage());
        psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);
        assertEquals("FATAL: no pg_hba.conf entry for host", psqle.getMessage().substring(0, 36));

        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);

        /* -------------------- Incorrect password -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, "123123");

        ae = assertThrows(AssertionError.class, () -> runner.run());
        pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create SQL connection to specified host and port", ioe.getMessage());
        psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);
        assertEquals("FATAL: password authentication failed for user",
                psqle.getMessage().substring(0, 46));

        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);

        /* -------------------- User has not replication role -------------------- */

        runner.setProperty(CaptureChangePostgreSQL.USERNAME, ORDINARY_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, ORDINARY_USER_PASSWORD);

        ae = assertThrows(AssertionError.class, () -> runner.run());
        pe = ae.getCause();
        assertTrue(pe instanceof ProcessException);
        ioe = pe.getCause();
        assertTrue(ioe instanceof IOException);
        assertEquals("Failed to create Replication connection to specified host and port",
                ioe.getMessage());
        psqle = ioe.getCause();
        assertTrue(psqle instanceof PSQLException);
        assertEquals("FATAL: must be superuser or replication role to start walsender",
                psqle.getMessage());

        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
    }

    @Test
    public void test04CDCBeginCommitNotIncludedAllMetadataNotIncluded() throws SQLException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        client.createTable();

        /* -------------------- INSERT OPERATION -------------------- */

        // Expected BEGIN, INSERT and COMMIT events.
        int enqueuedFlowFiles = 1;

        runner.run(1, false, true);
        client.insertRow();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        final MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(0);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeExists("cdc.lsn");

        String insertContent = insert.getContent();
        insertContent = insertContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        assertTrue(insertContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"RIO DE JANEIRO\",\"founded\":\"1565-03-01\",\"id\":1}"
                        + ",\"relationName\":\"public.tb_city\",\"lsn\":123456,\"type\":\"insert\"}"));

        /* -------------------- UPDATE OPERATION -------------------- */

        // Expected INSERT and UPDATE events.
        enqueuedFlowFiles = enqueuedFlowFiles + client.countRows("RIO DE JANEIRO");

        runner.run(1, false, true);
        client.updateRows();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        final MockFlowFile update = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(1);
        assertNotNull(update);
        update.assertAttributeEquals("cdc.type", "update");
        update.assertAttributeExists("cdc.lsn");

        String udpateContent = update.getContent();
        udpateContent = udpateContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        assertTrue(udpateContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"WONDERFUL CITY\",\"founded\":\"1565-03-01\",\"id\":1}"
                        + ",\"relationName\":\"public.tb_city\",\"lsn\":123456,\"type\":\"update\"}"));

        /* -------------------- DELETE OPERATION -------------------- */

        // Expected INSERT, UPDATE and DELETE events.
        enqueuedFlowFiles = enqueuedFlowFiles + client.countRows("WONDERFUL CITY");

        runner.run(1, false, true);
        client.deleteRows();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        final MockFlowFile delete = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(2);
        assertNotNull(delete);
        delete.assertAttributeEquals("cdc.type", "delete");
        delete.assertAttributeExists("cdc.lsn");

        String deleteContent = delete.getContent();
        deleteContent = deleteContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        assertTrue(deleteContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"WONDERFUL CITY\",\"founded\":\"1565-03-01\",\"id\":1}"
                        + ",\"relationName\":\"public.tb_city\",\"lsn\":123456,\"type\":\"delete\"}"));
    }

    @Test
    public void test05CDCBeginCommitIncludedAllMetadataNotIncluded() throws SQLException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        client.createTable();

        /* -------------------- INSERT OPERATION -------------------- */

        // Expected BEGIN, INSERT and COMMIT events.
        int enqueuedFlowFiles = 3;

        runner.run(1, false, true);
        client.insertRow();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        // BEGIN
        MockFlowFile begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeExists("cdc.lsn");

        String beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xid\":[0-9]+,", "\"xid\":123,");
        beginContent = beginContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111111,");
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
        assertTrue(beginContent.equals(
                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"begin\",\"xLSNFinal\":1111112}"));

        // INSERT
        final MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(1);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeExists("cdc.lsn");

        String insertContent = insert.getContent();
        insertContent = insertContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        assertTrue(insertContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"RIO DE JANEIRO\",\"founded\":\"1565-03-01\",\"id\":1},"
                        + "\"relationName\":\"public.tb_city\",\"lsn\":123456,\"type\":\"insert\"}"));

        // COMMIT
        MockFlowFile commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(2);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeExists("cdc.lsn");

        String commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111113,");
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
        commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
        assertTrue(commitContent.equals(
                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"commit\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

        /* -------------------- UPDATE OPERATION -------------------- */

        // Expected 2 x BEGIN, INSERT, UPDATE and 2 x COMMIT events.
        enqueuedFlowFiles = enqueuedFlowFiles + (client.countRows("RIO DE JANEIRO") * 3);

        runner.run(1, false, true);
        client.updateRows();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        // BEGIN
        begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 3);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeExists("cdc.lsn");

        beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xid\":[0-9]+,", "\"xid\":123,");
        beginContent = beginContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111111,");
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
        assertTrue(beginContent.equals(
                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"begin\",\"xLSNFinal\":1111112}"));

        // UPDATE
        final MockFlowFile update = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 2);
        assertNotNull(update);
        update.assertAttributeEquals("cdc.type", "update");
        update.assertAttributeExists("cdc.lsn");

        String udpateContent = update.getContent();
        udpateContent = udpateContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        assertTrue(udpateContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"WONDERFUL CITY\",\"founded\":\"1565-03-01\",\"id\":1}"
                        + ",\"relationName\":\"public.tb_city\",\"lsn\":123456,\"type\":\"update\"}"));

        // COMMIT
        commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 1);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeExists("cdc.lsn");

        commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111113,");
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
        commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
        assertTrue(commitContent.equals(
                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"commit\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

        /* -------------------- DELETE OPERATION -------------------- */

        // Expected 3 x BEGIN, INSERT, UPDATE, DELETE and 3 x COMMIT events.
        enqueuedFlowFiles = enqueuedFlowFiles + (client.countRows("WONDERFUL CITY") * 3);

        runner.run(1, false, true);
        client.deleteRows();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        // BEGIN
        begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 3);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeExists("cdc.lsn");

        beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xid\":[0-9]+,", "\"xid\":123,");
        beginContent = beginContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111111,");
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
        assertTrue(beginContent.equals(
                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"begin\",\"xLSNFinal\":1111112}"));

        // DELETE
        final MockFlowFile delete = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 2);
        assertNotNull(delete);
        delete.assertAttributeEquals("cdc.type", "delete");
        delete.assertAttributeExists("cdc.lsn");

        String deleteContent = delete.getContent();
        deleteContent = deleteContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        assertTrue(deleteContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"WONDERFUL CITY\",\"founded\":\"1565-03-01\",\"id\":1}"
                        + ",\"relationName\":\"public.tb_city\",\"lsn\":123456,\"type\":\"delete\"}"));

        // COMMIT
        commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 1);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeExists("cdc.lsn");

        commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111113,");
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
        commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
        assertTrue(commitContent.equals(
                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"commit\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));
    }

    @Test
    public void test06CDCBeginCommitIncludedAllMetadataIncluded() throws SQLException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "true");

        client.createTable();

        /* -------------------- INSERT OPERATION -------------------- */

        // Expected BEGIN, RELATION, INSERT and COMMIT events.
        int enqueuedFlowFiles = 4;

        runner.run(1, false, true);
        client.insertRow();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        // BEGIN
        MockFlowFile begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeExists("cdc.lsn");

        String beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xid\":[0-9]+,", "\"xid\":123,");
        beginContent = beginContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111111,");
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
        assertTrue(beginContent.equals(
                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"begin\",\"xLSNFinal\":1111112}"));

        // RELATION
        MockFlowFile relation = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(1);
        assertNotNull(relation);
        relation.assertAttributeEquals("cdc.type", "relation");
        relation.assertAttributeNotExists("cdc.lsn");

        String relationContent = relation.getContent();
        relationContent = relationContent.replaceAll("\"id\":[0-9]+,", "\"id\":14888,");
        assertTrue(relationContent.equals(
                "{\"columns\":{\"1\":{\"position\":1,\"isKey\":true,\"name\":\"id\",\"dataTypeId\":23,\"dataTypeName\":\"int4\",\"typeModifier\":-1},"
                        + "\"2\":{\"position\":2,\"isKey\":true,\"name\":\"name\",\"dataTypeId\":1043,\"dataTypeName\":\"varchar\",\"typeModifier\":-1},"
                        + "\"3\":{\"position\":3,\"isKey\":true,\"name\":\"country\",\"dataTypeId\":1043,\"dataTypeName\":\"varchar\",\"typeModifier\":-1},"
                        + "\"4\":{\"position\":4,\"isKey\":true,\"name\":\"founded\",\"dataTypeId\":1082,\"dataTypeName\":\"date\",\"typeModifier\":-1}},"
                        + "\"name\":\"tb_city\",\"objectName\":\"public.tb_city\",\"replicaIdentity\":\"f\",\"id\":14888,\"type\":\"relation\",\"numColumns\":4}"));

        // INSERT
        final MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(2);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeExists("cdc.lsn");

        String insertContent = insert.getContent();
        insertContent = insertContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        insertContent = insertContent.replaceAll("\"relationId\":[0-9]+,", "\"relationId\":14888,");
        assertTrue(insertContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"RIO DE JANEIRO\",\"founded\":\"1565-03-01\",\"id\":1}"
                        + ",\"relationName\":\"public.tb_city\",\"lsn\":123456,\"relationId\":14888,\"tupleType\":\"N\",\"type\":\"insert\"}"));

        // COMMIT
        MockFlowFile commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(3);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeExists("cdc.lsn");

        String commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111113,");
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
        commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
        assertTrue(commitContent.equals(
                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"commit\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

        /* -------------------- UPDATE OPERATION -------------------- */

        // Expected 2 x BEGIN, 2 x RELATION, INSERT, UPDATE and 2 x COMMIT events.
        enqueuedFlowFiles = enqueuedFlowFiles + (client.countRows("RIO DE JANEIRO") * 4);

        runner.run(1, false, true);
        client.updateRows();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        // BEGIN
        begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 4);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeExists("cdc.lsn");

        beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xid\":[0-9]+,", "\"xid\":123,");
        beginContent = beginContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111111,");
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
        assertTrue(beginContent.equals(
                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"begin\",\"xLSNFinal\":1111112}"));

        // RELATION
        relation = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 3);
        assertNotNull(relation);
        relation.assertAttributeEquals("cdc.type", "relation");
        relation.assertAttributeNotExists("cdc.lsn");

        relationContent = relation.getContent();
        relationContent = relationContent.replaceAll("\"id\":[0-9]+,", "\"id\":14888,");
        assertTrue(relationContent.equals(
                "{\"columns\":{\"1\":{\"position\":1,\"isKey\":true,\"name\":\"id\",\"dataTypeId\":23,\"dataTypeName\":\"int4\",\"typeModifier\":-1},"
                        + "\"2\":{\"position\":2,\"isKey\":true,\"name\":\"name\",\"dataTypeId\":1043,\"dataTypeName\":\"varchar\",\"typeModifier\":-1},"
                        + "\"3\":{\"position\":3,\"isKey\":true,\"name\":\"country\",\"dataTypeId\":1043,\"dataTypeName\":\"varchar\",\"typeModifier\":-1},"
                        + "\"4\":{\"position\":4,\"isKey\":true,\"name\":\"founded\",\"dataTypeId\":1082,\"dataTypeName\":\"date\",\"typeModifier\":-1}},"
                        + "\"name\":\"tb_city\",\"objectName\":\"public.tb_city\",\"replicaIdentity\":\"f\",\"id\":14888,\"type\":\"relation\",\"numColumns\":4}"));

        // UPDATE
        final MockFlowFile update = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 2);
        assertNotNull(update);
        update.assertAttributeEquals("cdc.type", "update");
        update.assertAttributeExists("cdc.lsn");

        String udpateContent = update.getContent();
        udpateContent = udpateContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        udpateContent = udpateContent.replaceAll("\"relationId\":[0-9]+,", "\"relationId\":14888,");
        assertTrue(udpateContent.equals(
                "{\"relationName\":\"public.tb_city\",\"tupleType2\":\"N\",\"lsn\":123456,"
                        + "\"tupleData1\":{\"country\":\"BRAZIL\",\"name\":\"RIO DE JANEIRO\",\"founded\":\"1565-03-01\",\"id\":1},"
                        + "\"tupleData2\":{\"country\":\"BRAZIL\",\"name\":\"WONDERFUL CITY\",\"founded\":\"1565-03-01\",\"id\":1},"
                        + "\"relationId\":14888,\"tupleType1\":\"O\",\"type\":\"update\"}"));

        // COMMIT
        commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 1);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeExists("cdc.lsn");

        commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111113,");
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
        commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
        assertTrue(commitContent.equals(
                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"commit\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

        /* -------------------- DELETE OPERATION -------------------- */

        // Expected 3 x BEGIN, 3 x RELATION, INSERT, UPDATE, DELETE and 3 x COMMIT
        // events.
        enqueuedFlowFiles = enqueuedFlowFiles + (client.countRows("WONDERFUL CITY") * 4);

        runner.run(1, false, true);
        client.deleteRows();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, enqueuedFlowFiles);

        // BEGIN
        begin = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 4);
        assertNotNull(begin);
        begin.assertAttributeEquals("cdc.type", "begin");
        begin.assertAttributeExists("cdc.lsn");

        beginContent = begin.getContent();
        beginContent = beginContent.replaceAll("\"xid\":[0-9]+,", "\"xid\":123,");
        beginContent = beginContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111111,");
        beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
        assertTrue(beginContent.equals(
                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"begin\",\"xLSNFinal\":1111112}"));

        // RELATION
        relation = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 3);
        assertNotNull(relation);
        relation.assertAttributeEquals("cdc.type", "relation");
        relation.assertAttributeNotExists("cdc.lsn");

        relation = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 3);
        assertNotNull(relation);
        relation.assertAttributeEquals("cdc.type", "relation");
        relation.assertAttributeNotExists("cdc.lsn");

        relationContent = relation.getContent();
        relationContent = relationContent.replaceAll("\"id\":[0-9]+,", "\"id\":14888,");
        assertTrue(relationContent.equals(
                "{\"columns\":{\"1\":{\"position\":1,\"isKey\":true,\"name\":\"id\",\"dataTypeId\":23,\"dataTypeName\":\"int4\",\"typeModifier\":-1},"
                        + "\"2\":{\"position\":2,\"isKey\":true,\"name\":\"name\",\"dataTypeId\":1043,\"dataTypeName\":\"varchar\",\"typeModifier\":-1},"
                        + "\"3\":{\"position\":3,\"isKey\":true,\"name\":\"country\",\"dataTypeId\":1043,\"dataTypeName\":\"varchar\",\"typeModifier\":-1},"
                        + "\"4\":{\"position\":4,\"isKey\":true,\"name\":\"founded\",\"dataTypeId\":1082,\"dataTypeName\":\"date\",\"typeModifier\":-1}},"
                        + "\"name\":\"tb_city\",\"objectName\":\"public.tb_city\",\"replicaIdentity\":\"f\",\"id\":14888,\"type\":\"relation\",\"numColumns\":4}"));

        // DELETE
        final MockFlowFile delete = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 2);
        assertNotNull(delete);
        delete.assertAttributeEquals("cdc.type", "delete");
        delete.assertAttributeExists("cdc.lsn");

        String deleteContent = delete.getContent();
        deleteContent = deleteContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":123456,");
        deleteContent = deleteContent.replaceAll("\"relationId\":[0-9]+,", "\"relationId\":14888,");
        assertTrue(deleteContent.equals(
                "{\"tupleData\":{\"country\":\"BRAZIL\",\"name\":\"WONDERFUL CITY\",\"founded\":\"1565-03-01\",\"id\":1},"
                        + "\"relationName\":\"public.tb_city\",\"lsn\":123456,\"relationId\":14888,\"tupleType\":\"O\",\"type\":\"delete\"}"));

        // COMMIT
        commit = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)
                .get(enqueuedFlowFiles - 1);
        assertNotNull(commit);
        commit.assertAttributeEquals("cdc.type", "commit");
        commit.assertAttributeExists("cdc.lsn");

        commitContent = commit.getContent();
        commitContent = commitContent.replaceAll("\"lsn\":[0-9]+,", "\"lsn\":1111113,");
        commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",\"type\"",
                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\"");
        commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
        commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
        assertTrue(commitContent.equals(
                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"type\":\"commit\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));
    }

    @Test
    public void test07CDCDropSlotIfExistsFalse() throws SQLException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        client.createTable();

        // INSERT AFTER ONTRIGGER
        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true"); // Drop slot if already exists.
        runner.run(1, false, true);
        client.insertRow();
        runner.run(1, true, false);
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1); // Expected only INSERT
                                                                                      // event.

        MockFlowFile insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeExists("cdc.lsn");

        // INSERT BEFORE ONTRIGGER
        client.insertRow(); // Inserting row before onTrigger.
        runner.run(); // Slot dropped and created again.
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1); // Still one (1). Don't
                                                                                      // get the
                                                                                      // row inserted before
                                                                                      // onTrigger.

        // INSERT BEFORE ONTRIGGER
        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "false"); // Don't drop slot if already
                                                                                  // exists.
        client.insertRow(); // Inserting row before onTrigger.
        runner.run();
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 2); // Get the row inserted
                                                                                      // before
                                                                                      // onTrigger.

        insert = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(1);
        assertNotNull(insert);
        insert.assertAttributeEquals("cdc.type", "insert");
        insert.assertAttributeExists("cdc.lsn");
    }

    @Test
    public void test08CDCStartLSN() throws SQLException, IOException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true"); // Drop slot if already exists.
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        client.createTable();

        runner.run();
        runner.getStateManager().assertStateSet(Scope.CLUSTER);
        runner.getStateManager().assertStateSet("last.received.lsn", Scope.CLUSTER);
        Long lastReceivedLSN = Long
                .valueOf(runner.getStateManager().getState(Scope.CLUSTER).get("last.received.lsn"));

        client.insertRow(); // Inserting row before onTrigger.
        Long nextStartLSN = client.getCurrentLSN() + 1L;
        assertTrue(nextStartLSN > lastReceivedLSN);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "false"); // Don't drop slot if already
                                                                                  // exists.
        runner.setProperty(CaptureChangePostgreSQL.START_LSN, nextStartLSN.toString()); // Set start LSN to
                                                                                        // after
                                                                                        // insert.
        runner.getStateManager().clear(Scope.CLUSTER); // Clear state

        runner.run();
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 0); // The last insert must
                                                                                      // not be
                                                                                      // captured.
    }

    @Test
    public void test09State() throws SQLException, IOException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        client.createTable();

        runner.getStateManager().assertStateNotSet(Scope.CLUSTER);
        runner.run();
        runner.getStateManager().assertStateSet(Scope.CLUSTER);
        runner.getStateManager().assertStateSet("last.received.lsn", Scope.CLUSTER);

        runner.getStateManager().clear(Scope.CLUSTER);

        runner.getStateManager().assertStateNotSet("last.received.lsn", Scope.CLUSTER);
        runner.run();
        runner.getStateManager().assertStateSet(Scope.CLUSTER);
        runner.getStateManager().assertStateSet("last.received.lsn", Scope.CLUSTER);
    }

    @Test
    public void test10DataProvenance() throws SQLException, ClassNotFoundException {
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
        runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
        runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
        runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, SUPER_USER);
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, SUPER_USER_PASSWORD);
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

        runner.setProperty(CaptureChangePostgreSQL.DROP_SLOT_IF_EXISTS, "true");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_BEGIN_COMMIT, "false");
        runner.setProperty(CaptureChangePostgreSQL.INCLUDE_ALL_METADATA, "false");

        client.createTable();

        // INSERT
        runner.run(1, false, true);
        client.insertRow();
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);

        runner.getProvenanceEvents()
                .forEach(event -> assertEquals(event.getEventType(), ProvenanceEventType.CREATE));
    }

    /**
     * PostgreSQL Client for Integration Tests
     */
    class ITPostgreSQLClient {

        private final String connectionUrl;
        private final Properties connectionProps = new Properties();
        private Connection connection;

        protected ITPostgreSQLClient() {
            this.connectionUrl = "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DATABASE;
            this.connectionProps.put("user", SUPER_USER);
            this.connectionProps.put("password", SUPER_USER_PASSWORD);
        }

        protected void setConnection() throws SQLException, ClassNotFoundException {
            if (connection != null && connection.isValid(30)) {
                return;
            }

            closeConnection();

            Class.forName(DRIVER_NAME);
            connection = DriverManager.getConnection(connectionUrl, connectionProps);
            connection.setAutoCommit(true);
        }

        protected boolean isConnected() throws SQLException {
            if (connection != null && connection.isValid(30)) {
                return true;
            }

            return false;
        }

        protected void closeConnection() throws SQLException {
            if (connection != null) {
                connection.close();
            }
        }

        protected void createTable() throws SQLException, ClassNotFoundException {
            setConnection();
            Statement st = connection.createStatement();

            String ddl = "DROP PUBLICATION IF EXISTS pub_city;"
                    + "DROP TABLE IF EXISTS tb_city;"
                    + "CREATE TABLE tb_city (id SERIAL PRIMARY KEY, name VARCHAR NOT NULL, country VARCHAR NOT NULL, founded DATE);"
                    + "ALTER TABLE tb_city REPLICA IDENTITY FULL;"
                    + "CREATE PUBLICATION pub_city FOR TABLE tb_city;";

            st.execute(ddl);
            st.close();
        }

        protected void dropTable() throws SQLException, ClassNotFoundException {
            setConnection();
            Statement st = connection.createStatement();
            String ddl = "DROP PUBLICATION IF EXISTS pub_city;" + "DROP TABLE IF EXISTS tb_city;";
            st.execute(ddl);
            st.close();
        }

        protected void insertRow() throws SQLException, ClassNotFoundException {
            setConnection();
            Statement st = connection.createStatement();
            st.execute(
                    "INSERT INTO tb_city (name, country, founded) VALUES ('RIO DE JANEIRO', 'BRAZIL', '1565-03-01')");
            st.close();
        }

        protected void updateRows() throws SQLException, ClassNotFoundException {
            setConnection();
            Statement st = connection.createStatement();
            st.execute("UPDATE tb_city SET name = 'WONDERFUL CITY' WHERE name = 'RIO DE JANEIRO'");
            st.close();
        }

        protected void deleteRows() throws SQLException, ClassNotFoundException {
            setConnection();
            Statement st = connection.createStatement();
            st.execute("DELETE FROM tb_city WHERE name = 'WONDERFUL CITY'");
            st.close();
        }

        protected int countRows(String filter) throws SQLException, ClassNotFoundException {
            setConnection();
            PreparedStatement st = connection
                    .prepareStatement("SELECT COUNT(*) FROM tb_city WHERE name = ?");
            st.setString(1, filter);
            ResultSet rs = st.executeQuery();

            int count = 0;
            if (rs.next())
                count = rs.getInt(1);

            st.close();
            return count;
        }

        protected Long getCurrentLSN() throws SQLException, ClassNotFoundException {
            setConnection();
            PreparedStatement st = connection.prepareStatement("SELECT pg_current_wal_insert_lsn()");
            ResultSet rs = st.executeQuery();

            Long count = 0L;
            if (rs.next())
                count = LogSequenceNumber.valueOf(rs.getString(1)).asLong();

            st.close();
            return count;
        }
    }
}
