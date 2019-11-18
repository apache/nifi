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

import java.io.File;
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
    final static String DB_LOCATION = "target/db";

    private static DBCPClientMapCacheService dbcpClientMapCacheService;
    private Serializer<String> stringSerializer = new StringSerializer();
    private Deserializer<String> stringDeserializer = new StringDeserializer();

    @BeforeClass
    public static void setup() throws Exception {
        System.setProperty("derby.stream.error.file", "target/derby.log");
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool dbcpConnectionPool = new DBCPConnectionPool();
        dbcpClientMapCacheService = new DBCPClientMapCacheService();
        runner.addControllerService("dbcp-connection-pool", dbcpConnectionPool);
        runner.addControllerService("dbcp-client-map-cache-service", dbcpClientMapCacheService);

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // set embedded Derby database connection url
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_USER, "tester");
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_PASSWORD, "testerp");
        runner.setProperty(dbcpConnectionPool, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");

        runner.enableControllerService(dbcpConnectionPool);
        runner.assertValid(dbcpConnectionPool);

        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("create schema test_schema");
                stmt.execute("create table test_schema.test_table(k varchar(255) for bit data primary key, v varchar(255) for bit data, r integer)");
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
        Integer rev = 1;
        boolean success = dbcpClientMapCacheService.replace(new AtomicCacheEntry<String, String, Integer>(key, value, rev), stringSerializer, stringSerializer);
        assertTrue(success);

        AtomicCacheEntry<String, String, Integer> result = dbcpClientMapCacheService.fetch(key, stringSerializer, stringDeserializer);
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
