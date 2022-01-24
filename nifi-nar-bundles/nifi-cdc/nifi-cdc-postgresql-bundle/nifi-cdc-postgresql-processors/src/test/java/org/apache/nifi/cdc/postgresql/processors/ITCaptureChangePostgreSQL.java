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
import org.junit.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PSQLException;

/**
 * Integration Test for PostgreSQL CDC Processor.
 *
 * The PostgreSQL Cluster should be configured to enable logical replication:
 *
 * - Property listen_addresses should be set to the hostname to listen to;
 *
 * - Property max_wal_senders should be at least equal to the number of
 * replication consumers;
 *
 * - Property wal_keep_size (or wal_keep_segments for PostgreSQL versions
 * before 13) specifies the minimum size of past WAL segments kept in the WAL
 * dir;
 *
 * - Property wal_level for logical replication should be equal to
 * logical;
 *
 * - Property max_replication_slots should be greater than zero for logical
 * replication.
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
 * For the Integration Tests it will be necessary to create specific users and
 * database:
 *
 * <code>
 * $ psql -h localhost -p 5432 -U postgres
 *
 * CREATE USER nifi SUPERUSER PASSWORD 'Change1t!';
 * CREATE USER intern PASSWORD 'justForT3sts!';
 *
 * CREATE DATABASE db_test;
 * </code>
 *
 * At last, authorize query and replication connections for user(s) and host(s).
 * For example:
 *
 * <code>
 * [pg_hba.conf]
 *
 * host db_test nifi 192.168.56.0/24 md5
 * host db_test intern 192.168.56.0/24 md5
 *
 * host replication nifi 192.168.56.0/24 md5
 * host replication intern 192.168.56.0/24 md5
 * </code>
 *
 * These configurations requires the restart of PostgreSQL service.
 */
public class ITCaptureChangePostgreSQL {

        // Change it!
        private static final String DRIVER_LOCATION = "/Users/davy.machado/Documents/SOFTWARES/LIBS/postgresql-42.3.1.jar";
        private static final String DRIVER_NAME = "org.postgresql.Driver";
        private static final String HOST = "192.168.56.101";
        private static final String PORT = "5432";
        private static final String DATABASE = "db_test";
        private static final String USERNAME = "nifi";
        private static final String PASSWORD = "Change1t!";
        private static final String CONNECTION_TIMEOUT = "30 seconds";
        private static final String PUBLICATION = "pub_city";
        private static final String REPLICATION_SLOT = "slot_city";

        ITPostgreSQLClient client;
        TestRunner runner;

        @Before
        public void setUp() throws Exception {
                runner = TestRunners.newTestRunner(new CaptureChangePostgreSQL());
                client = new ITPostgreSQLClient(HOST, PORT, DATABASE, USERNAME, PASSWORD);
        }

        @After
        public void tearDown() throws Exception {
                runner = null;

                if (client != null) {
                        client.closeConnection();
                        client = null;
                }
        }

        @Test
        public void testRequiredProperties() {
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
        public void testRegisterDriverFailure() {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
                runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
                runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);
                runner.assertValid();

                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, "/tmp/postgrez.jar"); // Incorrect driver
                                                                                                  // location
                runner.assertNotValid();
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.assertValid();

                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, "org.postgrez.Driver"); // Incorrect driver name
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable rue = pe.getCause();
                        assertTrue(rue instanceof RuntimeException);
                        assertEquals(
                                        "Failed to register JDBC driver. Ensure PostgreSQL Driver Location(s) and PostgreSQL Driver Class Name "
                                                        + "are configured correctly. org.apache.nifi.reporting.InitializationException: Can't load Database Driver",
                                        rue.getMessage());
                        Throwable ine = rue.getCause();
                        assertTrue(ine instanceof InitializationException);
                        assertEquals("Can't load Database Driver", ine.getMessage());
                }
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
        }

        @Test
        public void testConnectionFailures() {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
                runner.setProperty(CaptureChangePostgreSQL.PUBLICATION, PUBLICATION);
                runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT, REPLICATION_SLOT);

                runner.setProperty(CaptureChangePostgreSQL.HOST, "192.168.56.199"); // Incorrect IP
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating SQL connection to specified host and port", ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals("The connection attempt failed.", psqle.getMessage());
                }
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);

                runner.setProperty(CaptureChangePostgreSQL.PORT, "5199"); // Incorrect Port
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating SQL connection to specified host and port", ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals(
                                        "refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
                                        psqle.getMessage().substring(34, 144));
                }
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);

                runner.setProperty(CaptureChangePostgreSQL.DATABASE, "postgres"); // Access not authorized
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating SQL connection to specified host and port", ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals("FATAL: no pg_hba.conf entry for host", psqle.getMessage().substring(0, 36));
                }
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);

                runner.setProperty(CaptureChangePostgreSQL.DATABASE, "db_fake"); // Database doesn't exists
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating SQL connection to specified host and port", ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals("FATAL: no pg_hba.conf entry for host", psqle.getMessage().substring(0, 36));
                }
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);

                runner.setProperty(CaptureChangePostgreSQL.USERNAME, "test"); // User doesn't exists
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating SQL connection to specified host and port", ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals("FATAL: no pg_hba.conf entry for host", psqle.getMessage().substring(0, 36));
                }
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);

                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, "123123"); // Incorrect password
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating SQL connection to specified host and port", ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals("FATAL: password authentication failed for user",
                                        psqle.getMessage().substring(0, 46));
                }
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);

                runner.setProperty(CaptureChangePostgreSQL.USERNAME, "intern"); // User has not replication role
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, "justForT3sts!");
                try {
                        runner.run();
                } catch (AssertionError ae) {
                        Throwable pe = ae.getCause();
                        assertTrue(pe instanceof ProcessException);
                        Throwable ioe = pe.getCause();
                        assertTrue(ioe instanceof IOException);
                        assertEquals("Error creating Replication connection to specified host and port",
                                        ioe.getMessage());
                        Throwable psqle = ioe.getCause();
                        assertTrue(psqle instanceof PSQLException);
                        assertEquals("FATAL: must be superuser or replication role to start walsender",
                                        psqle.getMessage());
                }
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
        }

        @Test
        public void testCDCBeginCommitNotIncludedAllMetadataNotIncluded() throws SQLException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
        public void testCDCBeginCommitIncludedAllMetadataNotIncluded() throws SQLException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
                beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
                assertTrue(beginContent.equals(
                                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"xLSNFinal\":1111112}"));

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
                commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
                commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
                assertTrue(commitContent.equals(
                                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

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
                beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
                assertTrue(beginContent.equals(
                                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"xLSNFinal\":1111112}"));

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
                commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
                commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
                assertTrue(commitContent.equals(
                                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

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
                beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
                assertTrue(beginContent.equals(
                                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"xLSNFinal\":1111112}"));

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
                commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
                commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
                assertTrue(commitContent.equals(
                                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));
        }

        @Test
        public void testCDCBeginCommitIncludedAllMetadataIncluded() throws SQLException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
                beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
                assertTrue(beginContent.equals(
                                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"xLSNFinal\":1111112}"));

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
                commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
                commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
                assertTrue(commitContent.equals(
                                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

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
                beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
                assertTrue(beginContent.equals(
                                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"xLSNFinal\":1111112}"));

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
                commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
                commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
                assertTrue(commitContent.equals(
                                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));

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
                beginContent = beginContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                beginContent = beginContent.replaceAll("\"xLSNFinal\":[0-9]+}", "\"xLSNFinal\":1111112}");
                assertTrue(beginContent.equals(
                                "{\"xid\":123,\"lsn\":1111111,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"xLSNFinal\":1111112}"));

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
                commitContent = commitContent.replaceAll("\"xCommitTime\":\".+\",",
                                "\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",");
                commitContent = commitContent.replaceAll("\"commitLSN\":[0-9]+,", "\"commitLSN\":1111112,");
                commitContent = commitContent.replaceAll("\"xLSNEnd\":[0-9]+}", "\"xLSNEnd\":1111113}");
                assertTrue(commitContent.equals(
                                "{\"lsn\":1111113,\"flags\":0,\"xCommitTime\":\"2022-01-01 12:01:01 BRT -0300\",\"commitLSN\":1111112,\"xLSNEnd\":1111113}"));
        }

        @Test
        public void testCDCDropSlotIfExistsFalse() throws SQLException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
        public void testCDCStartLSN() throws SQLException, IOException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
        public void testState() throws SQLException, IOException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
        public void testDataProvenance() throws SQLException {
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_LOCATION, DRIVER_LOCATION);
                runner.setProperty(CaptureChangePostgreSQL.DRIVER_NAME, DRIVER_NAME);
                runner.setProperty(CaptureChangePostgreSQL.HOST, HOST);
                runner.setProperty(CaptureChangePostgreSQL.PORT, PORT);
                runner.setProperty(CaptureChangePostgreSQL.DATABASE, DATABASE);
                runner.setProperty(CaptureChangePostgreSQL.USERNAME, USERNAME);
                runner.setProperty(CaptureChangePostgreSQL.PASSWORD, PASSWORD);
                runner.setProperty(CaptureChangePostgreSQL.CONNECTION_TIMEOUT, CONNECTION_TIMEOUT);
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
                private final int connectionTimeoutSeconds = 10;
                private final Properties connectionProps = new Properties();
                private Connection connection;

                protected ITPostgreSQLClient(String host, String port, String database, String username,
                                String password)
                                throws ClassNotFoundException {
                        this.connectionUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database;
                        this.connectionProps.put("user", username);
                        this.connectionProps.put("password", password);

                        Class.forName("org.postgresql.Driver");
                }

                private void setConnection() throws SQLException {
                        if (connection != null && connection.isValid(connectionTimeoutSeconds)) {
                                return;
                        }

                        closeConnection();

                        connection = DriverManager.getConnection(connectionUrl, connectionProps);
                        connection.setAutoCommit(true);
                }

                protected void closeConnection() throws SQLException {
                        if (connection != null) {
                                dropTable();
                                connection.close();
                        }
                }

                protected void createTable() throws SQLException {
                        setConnection();
                        Statement st = connection.createStatement();

                        String ddl = "DROP PUBLICATION IF EXISTS pub_city;" + "DROP TABLE IF EXISTS tb_city;"
                                        + "CREATE TABLE tb_city (id SERIAL PRIMARY KEY, name VARCHAR NOT NULL, country VARCHAR NOT NULL, founded DATE);"
                                        + "ALTER TABLE tb_city REPLICA IDENTITY FULL;"
                                        + "CREATE PUBLICATION pub_city FOR TABLE tb_city;";

                        st.execute(ddl);
                        st.close();
                }

                protected void dropTable() throws SQLException {
                        setConnection();
                        Statement st = connection.createStatement();

                        String ddl = "DROP PUBLICATION IF EXISTS pub_city;" + "DROP TABLE IF EXISTS tb_city;";
                        st.execute(ddl);
                        st.close();
                }

                protected void insertRow() throws SQLException {
                        setConnection();
                        Statement st = connection.createStatement();
                        st.execute("INSERT INTO tb_city (name, country, founded) VALUES ('RIO DE JANEIRO', 'BRAZIL', '1565-03-01')");
                        st.close();
                }

                protected void updateRows() throws SQLException {
                        setConnection();
                        Statement st = connection.createStatement();
                        st.execute("UPDATE tb_city SET name = 'WONDERFUL CITY' WHERE name = 'RIO DE JANEIRO'");
                        st.close();
                }

                protected void deleteRows() throws SQLException {
                        setConnection();
                        Statement st = connection.createStatement();
                        st.execute("DELETE FROM tb_city WHERE name = 'WONDERFUL CITY'");
                        st.close();
                }

                protected int countRows(String city) throws SQLException {
                        setConnection();
                        PreparedStatement st = connection
                                        .prepareStatement("SELECT COUNT(*) FROM tb_city WHERE name = ?");
                        st.setString(1, city);
                        ResultSet rs = st.executeQuery();

                        int count = 0;
                        if (rs.next())
                                count = rs.getInt(1);

                        st.close();
                        return count;
                }

                protected Long getCurrentLSN() throws SQLException {
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
