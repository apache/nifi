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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PutDatabaseRecordTest {

    private static final String CONNECTION_FAILED = "Connection Failed";

    private static final String PARSER_ID = MockRecordParser.class.getSimpleName();

    private static final String TABLE_NAME = "PERSONS";

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private final static String DB_LOCATION = "target/db_pdr";

    TestRunner runner;
    PutDatabaseRecord processor;
    DBCPServiceSimpleImpl dbcp;

    @BeforeAll
    public static void setDatabaseLocation() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @AfterAll
    public static void shutdownDatabase() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (SQLNonTransientConnectionException ignore) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
        System.clearProperty("derby.stream.error.file");
    }

    @BeforeEach
    public void setRunner() throws Exception {
        processor = new PutDatabaseRecord();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testGetConnectionFailure() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService(PARSER_ID, parser);
        runner.enableControllerService(parser);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, PARSER_ID);
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, TABLE_NAME);

        when(dbcp.getConnection(anyMap())).thenThrow(new ProcessException(CONNECTION_FAILED));

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_FAILURE);
    }

    @Test
    public void testInsertNonRequiredColumnsUnmatchedField() throws InitializationException, ProcessException {
        // Need to override the @Before method with a new processor that behaves badly
        processor = new PutDatabaseRecordUnmatchedField();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp");

        recreateTable();
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService(PARSER_ID, parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("extra", RecordFieldType.STRING);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date nifiDate1 = new Date(testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date nifiDate2 = new Date(testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

        parser.addRecord(1, "rec1", "test", nifiDate1);
        parser.addRecord(2, "rec2", "test", nifiDate2);
        parser.addRecord(3, "rec3", "test", null);
        parser.addRecord(4, "rec4", "test", null);
        parser.addRecord(5, null, null, null);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, PARSER_ID);
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, TABLE_NAME);
        runner.setProperty(PutDatabaseRecord.UNMATCHED_FIELD_BEHAVIOR, PutDatabaseRecord.FAIL_UNMATCHED_FIELD);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1);
    }

    private void recreateTable() throws ProcessException {
        try (final Connection conn = dbcp.getConnection();
            final Statement stmt = conn.createStatement()) {
            stmt.execute("drop table PERSONS");
            stmt.execute(createPersons);
        } catch (SQLException ignore) {
            // Do nothing, may not have existed
        }
    }

    static class PutDatabaseRecordUnmatchedField extends PutDatabaseRecord {
        @Override
        SqlAndIncludedColumns generateInsert(RecordSchema recordSchema, String tableName, TableSchema tableSchema, DMLSettings settings) throws IllegalArgumentException {
            return new SqlAndIncludedColumns("INSERT INTO PERSONS VALUES (?,?,?,?)", Arrays.asList(0,1,2,3));
        }
    }
}