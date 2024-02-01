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

import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("resource")
public class PutDatabaseRecordIT {

    private static PostgreSQLContainer<?> postgres;
    private TestRunner runner;


    @BeforeAll
    public static void startPostgres() {
        postgres = new PostgreSQLContainer<>("postgres:9.6.12")
            .withInitScript("PutDatabaseRecordIT/create-person-table.sql");
        postgres.start();
    }

    @AfterAll
    public static void cleanup() {
        if (postgres != null) {
            postgres.close();
            postgres = null;
        }
    }

    @BeforeEach
    public void setup() throws InitializationException, SQLException {
        truncateTable();

        runner = TestRunners.newTestRunner(PutDatabaseRecord.class);
        final DBCPConnectionPool connectionPool = new DBCPConnectionPool();
        runner.addControllerService("connectionPool", connectionPool);
        runner.setProperty(connectionPool, DBCPProperties.DATABASE_URL, postgres.getJdbcUrl());
        runner.setProperty(connectionPool, DBCPProperties.DB_USER, postgres.getUsername());
        runner.setProperty(connectionPool, DBCPProperties.DB_PASSWORD, postgres.getPassword());
        runner.setProperty(connectionPool, DBCPProperties.DB_DRIVERNAME, postgres.getDriverClassName());
        runner.enableControllerService(connectionPool);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.setProperty(jsonReader, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd");
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSSSSS");
        runner.enableControllerService(jsonReader);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "json-reader");
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "connectionPool");

        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "person");
        runner.setProperty(PutDatabaseRecord.DB_TYPE, "PostgreSQL");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, "INSERT");
    }


    @Test
    public void testSimplePut() throws SQLException {
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "favorite_color": "blue"
            }
            """);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals("John Doe", results.get("name"));
        assertEquals(50, results.get("age"));
        assertEquals("blue", results.get("favorite_color"));
    }

    @Test
    public void testWithDate() throws SQLException {
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "dob": "1975-01-01"
            }
            """);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);


        final Map<String, Object> results = getResults();
        assertEquals("John Doe", results.get("name"));
        assertEquals(50, results.get("age"));
        final Date dob = (Date) results.get("dob");
        assertEquals(1975, dob.toLocalDate().getYear());
        assertEquals(Month.JANUARY, dob.toLocalDate().getMonth());
        assertEquals(1, dob.toLocalDate().getDayOfMonth());
    }

    @Test
    public void testWithTimestampUsingMillis() throws SQLException {
        final long now = System.currentTimeMillis();
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": %s
            }
            """.formatted(now));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(new Timestamp(now), results.get("lasttransactiontime"));
    }

    @Test
    public void testWithTimestampUsingMillisAsString() throws SQLException {
        final long now = System.currentTimeMillis();
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": "%s"
            }
            """.formatted(now));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(new Timestamp(now), results.get("lasttransactiontime"));
    }

    @Test
    public void testWithStringTimestampUsingMicros() throws SQLException {
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": "2024-01-02 08:41:12.123456"
            }
            """);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final LocalDateTime localDateTime = LocalDateTime.now();

        final Map<String, Object> results = getResults();
        assertEquals("John Doe", results.get("name"));
        assertEquals(50, results.get("age"));
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        final LocalDateTime transactionLocalTime = lastTransactionTime.toLocalDateTime();
        assertEquals(2024, transactionLocalTime.getYear());
        assertEquals(Month.JANUARY, transactionLocalTime.getMonth());
        assertEquals(2, transactionLocalTime.getDayOfMonth());
        assertEquals(8, transactionLocalTime.getHour());
        assertEquals(41, transactionLocalTime.getMinute());
        assertEquals(12, transactionLocalTime.getSecond());
        assertEquals(123456000, transactionLocalTime.getNano());
    }

    @Test
    public void testWithNumericTimestampUsingMicros() throws SQLException {
        final Instant now = nowWithMicrosPrecision();
        final long epochSecond = now.getEpochSecond();
        final long epochMicros = (epochSecond * 1_000_000L) + (now.getNano() / 1000);

        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": %s
            }
            """.formatted(epochMicros));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals("John Doe", results.get("name"));
        assertEquals(50, results.get("age"));
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(now, lastTransactionTime.toInstant());
    }

    // Depending on the operating system, the precision of Instant.now() may be microsecond precision, or it may be nanosecond precision.
    // If nanosecond precision, the result that we retrieve from database will be inaccurate because it will be rounded down to microsecond precision.
    // To adjust for this, we will use Instant.now().truncatedTo(ChronoUnit.MICROS) to ensure that the Instant is microsecond precision.
    private Instant nowWithMicrosPrecision() {
        return Instant.now().truncatedTo(ChronoUnit.MICROS);
    }

    @Test
    public void testWithDecimalTimestampUsingMicros() throws SQLException {
        final Instant now = nowWithMicrosPrecision();
        final long epochSecond = now.getEpochSecond();
        final long microsPastSecond = now.getNano() / 1000;

        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": %s.%s
            }
            """.formatted(epochSecond, microsPastSecond));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals("John Doe", results.get("name"));
        assertEquals(50, results.get("age"));
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(now, lastTransactionTime.toInstant());
    }

    @Test
    public void testWithDecimalTimestampUsingMicrosAsString() throws SQLException {
        final Instant now = nowWithMicrosPrecision();
        final long epochSecond = now.getEpochSecond();
        final long microsPastSecond = now.getNano() / 1000;

        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": "%s.%s"
            }
            """.formatted(epochSecond, microsPastSecond));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals("John Doe", results.get("name"));
        assertEquals(50, results.get("age"));
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(now, lastTransactionTime.toInstant());
    }


    private static void truncateTable() throws SQLException {
        try (final Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            final String sqlQuery = "TRUNCATE TABLE person";
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                preparedStatement.execute();
            }
        }
    }

    private Map<String, Object> getResults() throws SQLException {
        try (final Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            final String sqlQuery = "SELECT * FROM person";
            final Map<String, Object> resultsMap = new HashMap<>();

            try (final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
                 final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        final String columnName = metaData.getColumnName(i);
                        final Object columnValue = resultSet.getObject(i);
                        resultsMap.put(columnName, columnValue);
                    }
                }
            }

            return resultsMap;
        }
    }

}
