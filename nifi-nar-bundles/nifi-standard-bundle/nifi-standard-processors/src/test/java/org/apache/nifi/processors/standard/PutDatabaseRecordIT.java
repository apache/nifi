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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class PutDatabaseRecordIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TestRunner runner;
    private DBCPService service;
    private MockRecordParser mockRecordParser;
    private Connection connection;
    private Statement statement;

    public void setup(Class poolType) throws Exception {
        //Mock the DBCP Controller Service so we can control the Results
        service = (DBCPService) poolType.getDeclaredConstructor().newInstance();
        mockRecordParser = new MockRecordParser();

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(PutDatabaseRecord.class);
        runner.addControllerService("dbcp", service, dbcpProperties);
        runner.addControllerService("parser", mockRecordParser);
        runner.enableControllerService(service);
        runner.enableControllerService(mockRecordParser);
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp");
        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");

        connection = service.getConnection();
        statement = connection.createStatement();
    }

    @AfterEach
    public void afterEach() throws Exception {
        statement.close();
        connection.close();
    }

    private void runDdl(String sql) throws SQLException {
        statement.executeUpdate(sql);
    }

    private ResultSet runQuery(String sql) throws SQLException {
        return statement.executeQuery(sql);
    }

    @Test
    public void testPostgresJsonSupport() throws Exception {
        setup(PostgreSQLSimplePool.class);
        runDdl("CREATE TABLE json_test(id serial, msg json, msgb jsonb)");

        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "json_test");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.MAP_RECORD_TO_JSON, "true");
        runner.assertValid();

        RecordField sender = new RecordField("sender", RecordFieldType.STRING.getDataType());
        RecordField message = new RecordField("message", RecordFieldType.STRING.getDataType());
        RecordSchema jsonMessage = new SimpleRecordSchema(Arrays.asList(sender, message));

        Map<String, Object> map = new HashMap<>();
        map.put("sender", "john.smith");
        map.put("message", "Hello, world");

        Record record = new MapRecord(jsonMessage, map);

        mockRecordParser.addSchemaField(new RecordField("msg", new RecordDataType(jsonMessage)));
        mockRecordParser.addSchemaField(new RecordField("msgb", new RecordDataType(jsonMessage)));
        mockRecordParser.addRecord(record, record);

        runner.enqueue("");
        runner.run();

        ResultSet resultSet = runQuery("SELECT * FROM json_test;");
        assertTrue(resultSet.next());

        String msg = resultSet.getString("msg");
        String msgb = resultSet.getString("msgb");

        Map<String, Object> msgParsed = MAPPER.readValue(msg, Map.class);
        Map<String, Object> msgbParsed = MAPPER.readValue(msgb, Map.class);

        assertInstanceOf(Map.class, msgParsed);
        assertInstanceOf(Map.class, msgbParsed);

        assertEquals(map, msgParsed);
        assertEquals(map, msgbParsed);
    }

    private void testMySQLVariant(Class pool) throws Exception {
        setup(pool);
        runDdl("CREATE TABLE json_test(id int primary key auto_increment, msg json)");

        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "json_test");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.MAP_RECORD_TO_JSON, "true");
        runner.assertValid();

        RecordField sender = new RecordField("sender", RecordFieldType.STRING.getDataType());
        RecordField message = new RecordField("message", RecordFieldType.STRING.getDataType());
        RecordSchema jsonMessage = new SimpleRecordSchema(Arrays.asList(sender, message));

        Map<String, Object> map = new HashMap<>();
        map.put("sender", "john.smith");
        map.put("message", "Hello, world");

        Record record = new MapRecord(jsonMessage, map);

        mockRecordParser.addSchemaField(new RecordField("msg", new RecordDataType(jsonMessage)));
        mockRecordParser.addRecord(record, record);

        runner.enqueue("");
        runner.run();

        ResultSet resultSet = runQuery("SELECT * FROM json_test;");
        assertTrue(resultSet.next());

        String msg = resultSet.getString("msg");
        Map<String, Object> msgParsed = MAPPER.readValue(msg, Map.class);

        assertInstanceOf(Map.class, msgParsed);
        assertEquals(map, msgParsed);
    }

    @Test
    public void testMariaDBJsonSupport() throws Exception {
        testMySQLVariant(MariaDBSimplePool.class);
    }

    @Test
    public void testMySQLJsonSupport() throws Exception {
        testMySQLVariant(MySQLSimplePool.class);
    }

    static abstract class AbstractSimplePool extends AbstractControllerService implements DBCPService {
        private String url;
        private String username;
        private String password;

        public AbstractSimplePool(String url, String username, String password) {
            this.url = url;
            this.username = username;
            this.password = password;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.testcontainers.jdbc.ContainerDatabaseDriver");
                return DriverManager.getConnection(url, username, password);
            } catch (Exception ex) {
                throw new ProcessException(ex);
            }
        }
    }

    static class MariaDBSimplePool extends AbstractSimplePool {
        public MariaDBSimplePool() {
            super("jdbc:tc:mariadb:10.9:///test", "root", "root");
        }
    }

    static class MySQLSimplePool extends AbstractSimplePool {
        public MySQLSimplePool() {
            super("jdbc:tc:mysql:8:///test","root", "root");
        }
    }

    static class PostgreSQLSimplePool extends AbstractSimplePool {
        public PostgreSQLSimplePool() {
            super("jdbc:tc:postgresql:14:///postgres", "postgres", "postgres");
        }
    }
}