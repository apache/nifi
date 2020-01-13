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
package org.apache.nifi.dbcp;

import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestDBCPClientMapCacheService {
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();
    private final static DBCPClientMapCacheService dbcpClientMapCacheService = new DBCPClientMapCacheService();

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty("derby.stream.error.file", "target/derby.log");
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        DBCPConnectionPool dbcpConnectionPool = new DBCPConnectionPool();
        runner.addControllerService("dbcp-connection-pool", dbcpConnectionPool);
        runner.addControllerService("dbcp-client-map-cache-service", dbcpClientMapCacheService);

        // Currently have both of these here because I'm having trouble with Derby and Travis
        // Setup Embedded Derby Database
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:memory:test;create=true");
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        // Setup Embedded H2 Database
//        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DATABASE_URL, "jdbc:h2:mem:test");
//        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_DRIVERNAME, "org.h2.Driver");
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_USER, "tester");
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_PASSWORD, "testerp");

        runner.enableControllerService(dbcpConnectionPool);
        runner.assertValid(dbcpConnectionPool);

        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("create schema test_schema");
                stmt.execute("create table test_schema.test_table(k varchar(255) for bit data primary key, v varchar(255) for bit data, r bigint)");
            }
        }

        // setup dbcp client map cache service
        runner.setProperty(dbcpClientMapCacheService, DBCPClientMapCacheService.DBCP_CONNECTION_POOL, "dbcp-connection-pool");
        runner.setProperty(dbcpClientMapCacheService, DBCPClientMapCacheService.SCHEMA_NAME, "TEST_SCHEMA");
        runner.setProperty(dbcpClientMapCacheService, DBCPClientMapCacheService.TABLE_NAME, "TEST_TABLE");
        runner.setProperty(dbcpClientMapCacheService, DBCPClientMapCacheService.KEY_COLUMN_NAME, "K");
        runner.setProperty(dbcpClientMapCacheService, DBCPClientMapCacheService.VAL_COLUMN_NAME, "V");
        runner.setProperty(dbcpClientMapCacheService, DBCPClientMapCacheService.REV_COLUMN_NAME, "R");
        runner.enableControllerService(dbcpClientMapCacheService);
        runner.assertValid(dbcpClientMapCacheService);
    }

    @Test
    public void testReplaceAndFetch() throws IOException {
        String key = UUID.randomUUID().toString();
        String value = "Hello World";
        Long rev = 1L;
        boolean success = dbcpClientMapCacheService.replace(new AtomicCacheEntry<String, String, Long>(key, value, rev), stringSerializer, stringSerializer);
        assertTrue(success);

        AtomicCacheEntry<String, String, Long> result = dbcpClientMapCacheService.fetch(key, stringSerializer, stringDeserializer);
        assertEquals(result.getKey(), key);
        assertEquals(result.getValue(), value);
        assertEquals(result.getRevision().get(), rev);
    }

    @Test
    public void testPutAndGet() throws IOException {
        String key = UUID.randomUUID().toString();
        String value = "Hello World";
        dbcpClientMapCacheService.put(key, value, stringSerializer, stringSerializer);

        String result = dbcpClientMapCacheService.get(key, stringSerializer, stringDeserializer);
        assertEquals(value, result);
    }

    @Test
    public void testPutIfAbsent() throws IOException {
        String key = UUID.randomUUID().toString();
        String value = "Hello World";
        boolean result;
        result = dbcpClientMapCacheService.putIfAbsent(key, value, stringSerializer, stringSerializer);
        assertEquals(result, true);
        result = dbcpClientMapCacheService.putIfAbsent(key, value, stringSerializer, stringSerializer);
        assertEquals(result, false);
    }

    @Test
    public void testRemove() throws IOException {
        String key = UUID.randomUUID().toString();
        String value = "Hello World";
        String result;
        dbcpClientMapCacheService.put(key, value, stringSerializer, stringSerializer);
        result = dbcpClientMapCacheService.get(key, stringSerializer, stringDeserializer);
        assertEquals(value, result);
        boolean success = dbcpClientMapCacheService.remove(key, stringSerializer);
        assertEquals(success, true);
        result = dbcpClientMapCacheService.get(key, stringSerializer, stringDeserializer);
        assertNull(result);
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(byte[] input) throws DeserializationException, IOException {
            return new String(input);
        }
    }
}
