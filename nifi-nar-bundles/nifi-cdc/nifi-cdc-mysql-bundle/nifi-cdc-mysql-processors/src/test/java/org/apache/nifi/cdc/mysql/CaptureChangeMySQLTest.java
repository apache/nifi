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
package org.apache.nifi.cdc.mysql;


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.event.TableInfo;
import org.apache.nifi.cdc.event.TableInfoCacheKey;
import org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQL;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CaptureChangeMySQLTest {

    private static final String DRIVER_LOCATION = "http://mysql-driver.com/driver.jar";
    CaptureChangeMySQL processor;
    TestRunner testRunner;
    MockBinlogClientJava client = new MockBinlogClientJava("localhost", 3306, "root", "password");

    @BeforeEach
    void setUp() throws Exception {
        processor = new MockCaptureChangeMySQL();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    void testConnectionFailures() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, "localhost:3306");
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, "root");
        testRunner.setProperty(CaptureChangeMySQL.SERVER_ID, "1");
        final DistributedMapCacheClientImpl cacheClient = createCacheClient();
        Map<String, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), "localhost");
        testRunner.addControllerService("client", cacheClient, clientProperties);
        testRunner.setProperty(CaptureChangeMySQL.DIST_CACHE_CLIENT, "client");
        testRunner.enableControllerService(cacheClient);
        client.connectionError = true;
        try {
            testRunner.run();
        } catch (AssertionError ae) {
            Throwable pe = ae.getCause();
            assertTrue(pe instanceof ProcessException);
            Throwable ioe = pe.getCause();
            assertTrue(ioe instanceof IOException);
            assertEquals("Could not connect binlog client to any of the specified hosts due to: Error during connect", ioe.getMessage());
            assertTrue(ioe.getCause() instanceof IOException);
        }
        client.connectionError = false;

        client.connectionTimeout = true;
        try {
            testRunner.run();
        } catch (AssertionError ae) {
            Throwable pe = ae.getCause();
            assertTrue(pe instanceof ProcessException);
            Throwable ioe = pe.getCause();
            assertTrue(ioe instanceof IOException);
            assertEquals("Could not connect binlog client to any of the specified hosts due to: Connection timed out", ioe.getMessage());
            assertTrue(ioe.getCause() instanceof TimeoutException);
        }
        client.connectionTimeout = false;
    }

    @Test
    void testSslModeDisabledSslContextServiceNotRequired() {
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, "localhost:3306");
        testRunner.setProperty(CaptureChangeMySQL.SSL_MODE, SSLMode.DISABLED.toString());
        testRunner.assertValid();
    }

    @Test
    void testGetXIDEvents() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, "localhost:3306");
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, "root");
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, "2 seconds");
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, "true");
        final DistributedMapCacheClientImpl cacheClient = createCacheClient();
        Map<String, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), "localhost");
        testRunner.addControllerService("client", cacheClient, clientProperties);
        testRunner.setProperty(CaptureChangeMySQL.DIST_CACHE_CLIENT, "client");
        testRunner.enableControllerService(cacheClient);

        testRunner.run(1, false, true);
        // COMMIT
        EventHeaderV4 header2 = new EventHeaderV4();
        header2.setEventType(EventType.XID);
        header2.setNextPosition(12);
        header2.setTimestamp(new Date().getTime());
        EventData eventData = new EventData() {
        };
        client.sendEvent(new Event(header2, eventData));

        // when we ge a xid event without having got a 'begin' event ,throw an exception
        assertThrows(AssertionError.class, () -> testRunner.run(1, false, false));
    }

    @Test
    void testBeginCommitTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQL.DRIVER_LOCATION, DRIVER_LOCATION);
        testRunner.setProperty(CaptureChangeMySQL.HOSTS, "localhost:3306");
        testRunner.setProperty(CaptureChangeMySQL.USERNAME, "root");
        testRunner.setProperty(CaptureChangeMySQL.CONNECT_TIMEOUT, "2 seconds");
        testRunner.setProperty(CaptureChangeMySQL.INCLUDE_BEGIN_COMMIT, "true");
        final DistributedMapCacheClientImpl cacheClient = createCacheClient();
        Map<String, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), "localhost");
        testRunner.addControllerService("client", cacheClient, clientProperties);
        testRunner.setProperty(CaptureChangeMySQL.DIST_CACHE_CLIENT, "client");
        testRunner.enableControllerService(cacheClient);


        testRunner.run(1, false, true);

        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.ROTATE);
        header.setNextPosition(2);
        header.setTimestamp(new Date().getTime());
        RotateEventData rotateEventData = new RotateEventData();
        rotateEventData.setBinlogFilename("mysql-bin.000001");
        rotateEventData.setBinlogPosition(4L);
        client.sendEvent(new Event(header, rotateEventData));

        // BEGIN
        EventHeaderV4 header1 = new EventHeaderV4();
        header1.setEventType(EventType.QUERY);
        header1.setNextPosition(6);
        header1.setTimestamp(new Date().getTime());
        QueryEventData rotateEventData1 = new QueryEventData();
        rotateEventData1.setDatabase("mysql-bin.000001");
        rotateEventData1.setDatabase("myDB");
        rotateEventData1.setSql("BEGIN");
        client.sendEvent(new Event(header1, rotateEventData1));

        // COMMIT
        EventHeaderV4 header2 = new EventHeaderV4();
        header2.setEventType(EventType.XID);
        header2.setNextPosition(12);
        header2.setTimestamp(new Date().getTime());
        EventData eventData2 = new EventData() {
        };
        client.sendEvent(new Event(header2, eventData2));

        //when get a xid event,stop and restart the processor
        //here we used to get an exception
        testRunner.run(1, true, false);
        testRunner.run(1, false, false);

        // next transaction
        // BEGIN
        EventHeaderV4 header3 = new EventHeaderV4();
        header3.setEventType(EventType.QUERY);
        header3.setNextPosition(16);
        header3.setTimestamp(new Date().getTime());
        QueryEventData rotateEventData3 = new QueryEventData();
        rotateEventData3.setDatabase("mysql-bin.000001");
        rotateEventData3.setDatabase("myDB");
        rotateEventData3.setSql("BEGIN");
        client.sendEvent(new Event(header3, rotateEventData3));


        testRunner.run(1, true, false);
    }

    /********************************
     * Mock and helper classes below
     ********************************/

    static DistributedMapCacheClientImpl createCacheClient() throws InitializationException {

        final DistributedMapCacheClientImpl client = new DistributedMapCacheClientImpl();
        final ComponentLog logger = new MockComponentLog("client", client);
        final MockControllerServiceInitializationContext clientInitContext = new MockControllerServiceInitializationContext(client, "client", logger, new MockStateManager(client));

        client.initialize(clientInitContext);

        return client;
    }

    static final class DistributedMapCacheClientImpl extends AbstractControllerService implements DistributedMapCacheClient {

        private final Map<String, String> cacheMap = new HashMap<>();


        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            List<PropertyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(DistributedMapCacheClientService.HOSTNAME);
            descriptors.add(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT);
            descriptors.add(DistributedMapCacheClientService.PORT);
            descriptors.add(DistributedMapCacheClientService.SSL_CONTEXT_SERVICE);
            return descriptors;
        }

        @Override
        public <K, V> boolean putIfAbsent(
                final K key,
                final V value,
                final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {

            StringWriter keyWriter = new StringWriter();
            keySerializer.serialize(key, new WriterOutputStream(keyWriter));
            String keyString = keyWriter.toString();

            if (cacheMap.containsKey(keyString)) return false;

            StringWriter valueWriter = new StringWriter();
            valueSerializer.serialize(value, new WriterOutputStream(valueWriter));
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(
                final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                final Deserializer<V> valueDeserializer) throws IOException {
            StringWriter keyWriter = new StringWriter();
            keySerializer.serialize(key, new WriterOutputStream(keyWriter));
            String keyString = keyWriter.toString();

            if (cacheMap.containsKey(keyString))
                return valueDeserializer.deserialize(cacheMap.get(keyString).getBytes(StandardCharsets.UTF_8));

            StringWriter valueWriter = new StringWriter();
            valueSerializer.serialize(value, new WriterOutputStream(valueWriter));
            return null;
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            StringWriter keyWriter = new StringWriter();
            keySerializer.serialize(key, new WriterOutputStream(keyWriter));
            String keyString = keyWriter.toString();

            return cacheMap.containsKey(keyString);
        }

        @Override
        public <K, V> V get(
                final K key,
                final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            StringWriter keyWriter = new StringWriter();
            keySerializer.serialize(key, new WriterOutputStream(keyWriter));
            String keyString = keyWriter.toString();

            return (cacheMap.containsKey(keyString)) ? valueDeserializer.deserialize(cacheMap.get(keyString).getBytes(StandardCharsets.UTF_8)) : null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            StringWriter keyWriter = new StringWriter();
            serializer.serialize(key, new WriterOutputStream(keyWriter));
            String keyString = keyWriter.toString();

            boolean removed = (cacheMap.containsKey(keyString));
            cacheMap.remove(keyString);
            return removed;
        }

        @Override
        public long removeByPattern(String regex) throws IOException {
            final List<String> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (String key : cacheMap.keySet()) {
                // Key must be backed by something that can be converted into a String
                Matcher m = p.matcher(key);
                if (m.matches()) {
                    removedRecords.add(cacheMap.get(key));
                }
            }
            final long numRemoved = removedRecords.size();
            for (String it : removedRecords) {
                cacheMap.remove(it);
            }
            return numRemoved;
        }

        @Override
        public <K, V> void put(
                final K key,
                final V value,
                final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            StringWriter keyWriter = new StringWriter();
            keySerializer.serialize(key, new WriterOutputStream(keyWriter));
            StringWriter valueWriter = new StringWriter();
            valueSerializer.serialize(value, new WriterOutputStream(valueWriter));
        }
    }

    public class MockCaptureChangeMySQL extends CaptureChangeMySQL {

        Map<TableInfoCacheKey, TableInfo> cache = new HashMap<>();

        public BinaryLogClient createBinlogClient(String hostname, int port, String username, String password) {
            return client;
        }

        @Override
        public TableInfo loadTableInfo(TableInfoCacheKey key) {
            TableInfo tableInfo = cache.get(key);
            if (tableInfo == null) {
                List<ColumnDefinition> column = new ArrayList<>();
                column.add(new ColumnDefinition((byte) 4, "id"));
                column.add(new ColumnDefinition((byte) -4, "string1"));

                tableInfo = new TableInfo(key.getDatabaseName(), key.getTableName(), key.getTableId(), column);
                cache.put(key, tableInfo);
            }
            return tableInfo;
        }

        @Override
        protected void registerDriver(String locationString, String drvName) throws InitializationException {
        }

        @Override
        protected Connection getJdbcConnection() throws SQLException {
            Connection mockConnection = mock(Connection.class);
            Statement mockStatement = mock(Statement.class);
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            ResultSet mockResultSet = mock(ResultSet.class);
            when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
            return mockConnection;
        }
    }
}
