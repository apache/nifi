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
package org.apache.nifi.db.schemaregistry;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.ConnectionUrlValidator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.dbcp.DriverClassValidator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

public class DatabaseTableSchemaRegistryTest {

    private static final List<String> CREATE_TABLE_STATEMENTS = Arrays.asList(
            "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
                    " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)",
            "CREATE TABLE SCHEMA1.PERSONS (id integer primary key, name varchar(100)," +
                    " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)",
            "CREATE TABLE SCHEMA2.PERSONS (id2 integer primary key, name varchar(100)," +
                    " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)",
            "CREATE TABLE UUID_TEST (id integer primary key, name VARCHAR(100))",
            "CREATE TABLE LONGVARBINARY_TEST (id integer primary key, name LONG VARCHAR FOR BIT DATA)",
            "CREATE TABLE CONSTANTS (id integer primary key, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    );

    private static final String SERVICE_ID = SimpleDBCPService.class.getName();

    private final static String DB_LOCATION = "target/db_schema_reg";

    // This is to mimic those in DBCPProperties to avoid adding the dependency to nifi-dbcp-base
    private static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("Database Connection URL")
            .addValidator(new ConnectionUrlValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    private static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .addValidator(new DriverClassValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private TestRunner runner;

    @BeforeAll
    public static void setupDatabase() throws Exception {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignored) {
            // Do nothing, may not have existed
        }

        // Mock the DBCP Controller Service so we can control the Results
        DBCPService setupDbService = spy(new SimpleDBCPService(DB_LOCATION));

        final Connection conn = setupDbService.getConnection();
        final Statement stmt = conn.createStatement();
        for (String createTableStatement : CREATE_TABLE_STATEMENTS) {
            stmt.execute(createTableStatement);
        }

        stmt.close();
        conn.close();
    }

    @AfterAll
    public static void shutdownDatabase() {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (Exception ignored) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignored) {
            // Do nothing, may not have existed
        }
        System.clearProperty("derby.stream.error.file");
    }

    @BeforeEach
    public void setService() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        DBCPService dbcp = new SimpleDBCPService(DB_LOCATION);
        runner.addControllerService(SERVICE_ID, dbcp);

        final String url = String.format("jdbc:derby:%s;create=false", DB_LOCATION);
        runner.setProperty(dbcp, DATABASE_URL, url);
        runner.setProperty(dbcp, DB_USER, String.class.getSimpleName());
        runner.setProperty(dbcp, DB_PASSWORD, String.class.getName());
        runner.setProperty(dbcp, DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.enableControllerService(dbcp);
    }

    @Test
    public void testGetSchemaExists() throws Exception {
        DatabaseTableSchemaRegistry dbSchemaRegistry = new DatabaseTableSchemaRegistry();
        runner.addControllerService("schemaRegistry", dbSchemaRegistry);
        runner.setProperty(dbSchemaRegistry, DatabaseTableSchemaRegistry.DBCP_SERVICE, SERVICE_ID);
        runner.setProperty(dbSchemaRegistry, DatabaseTableSchemaRegistry.SCHEMA_NAME, "SCHEMA1");
        runner.enableControllerService(dbSchemaRegistry);
        SchemaIdentifier schemaIdentifier = new StandardSchemaIdentifier.Builder()
                .name("PERSONS")
                .build();
        RecordSchema recordSchema = dbSchemaRegistry.retrieveSchema(schemaIdentifier);
        assertNotNull(recordSchema);
        Optional<RecordField> recordField = recordSchema.getField("ID");
        assertTrue(recordField.isPresent());
        assertEquals(RecordFieldType.INT.getDataType(), recordField.get().getDataType());
        recordField = recordSchema.getField("NAME");
        assertTrue(recordField.isPresent());
        assertEquals(RecordFieldType.STRING.getDataType(), recordField.get().getDataType());
        recordField = recordSchema.getField("CODE");
        assertTrue(recordField.isPresent());
        assertEquals(RecordFieldType.INT.getDataType(), recordField.get().getDataType());
        recordField = recordSchema.getField("DT");
        assertTrue(recordField.isPresent());
        assertEquals(RecordFieldType.DATE.getDataType(), recordField.get().getDataType());
        // Get nonexistent field
        recordField = recordSchema.getField("NOT_A_FIELD");
        assertFalse(recordField.isPresent());
    }

    @Test
    public void testGetSchemaNotExists() throws Exception {
        DatabaseTableSchemaRegistry dbSchemaRegistry = new DatabaseTableSchemaRegistry();
        runner.addControllerService("schemaRegistry", dbSchemaRegistry);
        runner.setProperty(dbSchemaRegistry, DatabaseTableSchemaRegistry.DBCP_SERVICE, SERVICE_ID);
        runner.setProperty(dbSchemaRegistry, DatabaseTableSchemaRegistry.SCHEMA_NAME, "SCHEMA1");
        runner.enableControllerService(dbSchemaRegistry);
        SchemaIdentifier schemaIdentifier = new StandardSchemaIdentifier.Builder()
                .name("NOT_A_TABLE")
                .build();
        assertThrows(SchemaNotFoundException.class, () -> dbSchemaRegistry.retrieveSchema(schemaIdentifier));
    }

    @Test
    public void testGetSchemaCurrentTimestampIgnored() throws Exception {
        final DatabaseTableSchemaRegistry dbSchemaRegistry = new DatabaseTableSchemaRegistry();
        runner.addControllerService("schemaRegistry", dbSchemaRegistry);

        runner.setProperty(dbSchemaRegistry, DatabaseTableSchemaRegistry.DBCP_SERVICE, SERVICE_ID);

        runner.enableControllerService(dbSchemaRegistry);

        final SchemaIdentifier schemaIdentifier = new StandardSchemaIdentifier.Builder()
                .name("CONSTANTS")
                .build();
        final RecordSchema recordSchema = dbSchemaRegistry.retrieveSchema(schemaIdentifier);
        assertNotNull(recordSchema);

        final Optional<RecordField> idField = recordSchema.getField("ID");
        assertTrue(idField.isPresent());
        assertEquals(RecordFieldType.INT.getDataType(), idField.get().getDataType());

        final Optional<RecordField> createdFieldFound = recordSchema.getField("CREATED");
        assertTrue(createdFieldFound.isPresent());
        final RecordField createdField = createdFieldFound.get();
        assertEquals(RecordFieldType.TIMESTAMP.getDataType(), createdField.getDataType());

        // Default Value of CURRENT_TIMESTAMP ignored
        assertNull(createdField.getDefaultValue());
    }

    private static class SimpleDBCPService extends AbstractControllerService implements DBCPService {

        private final String databaseLocation;
        static {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            } catch (final Exception e) {
                throw new RuntimeException("Derby driver not found, something is very wrong", e);
            }
        }

        public SimpleDBCPService(final String databaseLocation) {
            this.databaseLocation = databaseLocation;
        }

        @Override
        public String getIdentifier() {
            return this.getClass().getName();
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                return DriverManager.getConnection("jdbc:derby:" + databaseLocation + ";create=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}