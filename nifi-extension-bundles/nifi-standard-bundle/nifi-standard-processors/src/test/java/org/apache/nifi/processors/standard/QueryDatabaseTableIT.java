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
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.PostgreSQLDatabaseAdapter;
import org.apache.nifi.reporting.InitializationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryDatabaseTableIT extends QueryDatabaseTableTest {
    private static PostgreSQLContainer<?> postgres;

    @BeforeAll
    public static void setupBeforeClass() {
        postgres = new PostgreSQLContainer<>("postgres:9.6.12")
                .withInitScript("PutDatabaseRecordIT/create-person-table.sql");
        postgres.start();
    }

    @AfterAll
    public static void cleanUpAfterClass() {
        if (postgres != null) {
            postgres.close();
            postgres = null;
        }
    }

    @Override
    public DatabaseAdapter createDatabaseAdapter() {
        return new PostgreSQLDatabaseAdapter();
    }

    @Override
    public void createDbcpControllerService() throws InitializationException {
        final DBCPConnectionPool connectionPool = new DBCPConnectionPool();
        runner.addControllerService("dbcp", connectionPool);
        runner.setProperty(connectionPool, DBCPProperties.DATABASE_URL, postgres.getJdbcUrl());
        runner.setProperty(connectionPool, DBCPProperties.DB_USER, postgres.getUsername());
        runner.setProperty(connectionPool, DBCPProperties.DB_PASSWORD, postgres.getPassword());
        runner.setProperty(connectionPool, DBCPProperties.DB_DRIVERNAME, postgres.getDriverClassName());
        runner.enableControllerService(connectionPool);
    }

    @Test
    public void testAddedRowsAutoCommitTrue() {
        // this test in the base class is not valid for PostgreSQL so check the validation error message.
        final AssertionError assertionError = assertThrows(AssertionError.class, super::testAddedRowsAutoCommitTrue);
        assertEquals(assertionError.getMessage(), "Processor has 1 validation failures:\n" +
                "'Set Auto Commit' validated against 'true' is invalid because 'Set Auto Commit' " +
                "must be set to 'false' because 'PostgreSQL' Database Type requires it to be 'false'\n");
    }
}
