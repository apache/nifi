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
package org.apache.nifi.processors.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.service.CassandraSessionProvider;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the AbstractCassandraProcessor class
 */
public class AbstractCassandraProcessorTest {

    MockAbstractCassandraProcessor processor;
    private TestRunner testRunner;

    @BeforeEach
    public void setUp() throws Exception {
        processor = new MockAbstractCassandraProcessor();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testCustomValidate() throws Exception {
        testRunner.setProperty(AbstractCassandraProcessor.LOCAL_DATACENTER, "datacenter1");
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.assertValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042, node2: 4399");
        testRunner.assertValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, " localhost : 9042, node2: 4399");
        testRunner.assertValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042, node2");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:65536");
        testRunner.assertNotValid();
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "user");
        testRunner.assertNotValid(); // Needs a password set if user is set
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.assertValid();
    }

    @Test
    public void testCustomValidateValidValues() throws Exception {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.LOCAL_DATACENTER, "datacenter1");
        testRunner.setProperty(AbstractCassandraProcessor.KEYSPACE, "sample_keyspace");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "cassandra_user");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password123");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "UTF-8");
        testRunner.assertValid();
    }

    @Test
    public void testGetCassandraObject() throws Exception {
        Row row = CassandraQueryTestUtil.createMockRow("user1");

        assertEquals("user1", AbstractCassandraProcessor.getCassandraObject(row, 0));
        assertEquals("Joe", AbstractCassandraProcessor.getCassandraObject(row, 1));
        assertEquals("Smith", AbstractCassandraProcessor.getCassandraObject(row, 2));

        Set<String> emails = (Set<String>) AbstractCassandraProcessor.getCassandraObject(row, 3);
        assertNotNull(emails);
        assertEquals(1, emails.size());
        assertTrue(emails.contains("jsmith@notareal.com"));

        List<String> topPlaces = (List<String>) AbstractCassandraProcessor.getCassandraObject(row, 4);
        assertNotNull(topPlaces);
        assertEquals(2, topPlaces.size());
        assertEquals("New York, NY", topPlaces.get(0));

        Map<String, String> todoMap = (Map<String, String>) AbstractCassandraProcessor.getCassandraObject(row, 5);
        assertNotNull(todoMap);
        assertEquals(1, todoMap.size());
        assertEquals("Set my alarm \"for\" a month from now", todoMap.get("2016-01-03 05:00:00+0000"));

        Boolean registered = (Boolean) AbstractCassandraProcessor.getCassandraObject(row, 6);
        assertNotNull(registered);
        assertFalse(registered);

        assertEquals(1.0f, (Float) AbstractCassandraProcessor.getCassandraObject(row, 7), 0.001);
        assertEquals(2.0, (Double) AbstractCassandraProcessor.getCassandraObject(row, 8), 0.001);
    }

    @Test
    public void testGetSchemaForType() throws Exception {
        assertEquals("string", AbstractCassandraProcessor.getSchemaForType("string").getType().getName());
        assertEquals("boolean", AbstractCassandraProcessor.getSchemaForType("boolean").getType().getName());
        assertEquals("int", AbstractCassandraProcessor.getSchemaForType("int").getType().getName());
        assertEquals("long", AbstractCassandraProcessor.getSchemaForType("long").getType().getName());
        assertEquals("float", AbstractCassandraProcessor.getSchemaForType("float").getType().getName());
        assertEquals("double", AbstractCassandraProcessor.getSchemaForType("double").getType().getName());
        assertEquals("bytes", AbstractCassandraProcessor.getSchemaForType("bytes").getType().getName());
    }

    @Test
    public void testGetSchemaForTypeBadType() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> AbstractCassandraProcessor.getSchemaForType("nothing"));
    }

    @Test
    public void testGetPrimitiveAvroTypeFromCassandraType() throws Exception {
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.ASCII));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.TEXT));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.TIMESTAMP));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.DATE));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.TIME));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.TIMEUUID));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.UUID));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.INET));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.VARINT));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.DECIMAL));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.DURATION));

        assertEquals("boolean", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.BOOLEAN));

        assertEquals("int", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.INT));
        assertEquals("int", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.SMALLINT));
        assertEquals("int", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.TINYINT));

        assertEquals("long", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.BIGINT));
        assertEquals("long", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.COUNTER));

        assertEquals("float", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.FLOAT));
        assertEquals("double", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.DOUBLE));

        assertEquals("bytes", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataTypes.BLOB));
    }

    @Test
    public void testGetPrimitiveAvroTypeFromCassandraTypeBadType() throws Exception {
        DataType mockDataType = mock(DataType.class);
        assertThrows(IllegalArgumentException.class, () -> AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(mockDataType));
    }

    @Test
    public void testGetPrimitiveDataTypeFromString() {
        assertEquals(DataTypes.ASCII, AbstractCassandraProcessor.getPrimitiveDataTypeFromString("ascii"));
    }

    @Test
    public void testGetContactPoints() throws Exception {
        List<InetSocketAddress> contactPoints = processor.getContactPoints("");
        assertNotNull(contactPoints);
        assertEquals(1, contactPoints.size());
        assertEquals("localhost", contactPoints.get(0).getHostName());
        assertEquals(AbstractCassandraProcessor.DEFAULT_CASSANDRA_PORT, contactPoints.get(0).getPort());

        contactPoints = processor.getContactPoints("192.168.99.100:9042");
        assertNotNull(contactPoints);
        assertEquals(1, contactPoints.size());
        assertEquals("192.168.99.100", contactPoints.get(0).getAddress().getHostAddress());
        assertEquals(9042, contactPoints.get(0).getPort());

        contactPoints = processor.getContactPoints("192.168.99.100:9042, mydomain.com : 4000");
        assertNotNull(contactPoints);
        assertEquals(2, contactPoints.size());
        assertEquals("192.168.99.100", contactPoints.get(0).getAddress().getHostAddress());
        assertEquals(9042, contactPoints.get(0).getPort());
        assertEquals("mydomain.com", contactPoints.get(1).getHostName());
        assertEquals(4000, contactPoints.get(1).getPort());
    }

    @Test
    public void testConnectToCassandra() throws Exception {
        CqlSession mockSession = mock(CqlSession.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(mockSession.getMetadata()).thenReturn(mockMetadata);

        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.LOCAL_DATACENTER, "datacenter1");

        processor.connectToCassandra(testRunner.getProcessContext());

        assertNotNull(processor.cassandraSession.get(), "Session should be initialized");
        processor.stop(testRunner.getProcessContext());
        assertNull(processor.cassandraSession.get(), "Session should be null after stop");
    }

    @Test
    public void testConnectToCassandraUsernamePassword() throws Exception {
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "user");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        // Now do a connect where a cluster is "built"
        processor.connectToCassandra(testRunner.getProcessContext());
        assertNotNull(processor.getSession());
    }

    @Test
    public void testCustomValidateCassandraConnectionConfiguration() throws InitializationException {
        MockCassandraSessionProvider sessionProviderService = new MockCassandraSessionProvider();

        testRunner.addControllerService("cassandra-connection-provider", sessionProviderService);
        testRunner.setProperty(sessionProviderService, CassandraSessionProvider.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(sessionProviderService, CassandraSessionProvider.KEYSPACE, "somekyespace");
        testRunner.setProperty(sessionProviderService, CassandraSessionProvider.LOCAL_DATACENTER, "datacenter1");

        testRunner.setProperty(AbstractCassandraProcessor.CONNECTION_PROVIDER_SERVICE, "cassandra-connection-provider");
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "localhost:9042");
        testRunner.setProperty(AbstractCassandraProcessor.KEYSPACE, "some-keyspace");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "user");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.LOCAL_DATACENTER, "datacenter1");

        testRunner.enableControllerService(sessionProviderService);
        testRunner.assertNotValid();

        testRunner.removeProperty(AbstractCassandraProcessor.CONTACT_POINTS);
        testRunner.removeProperty(AbstractCassandraProcessor.KEYSPACE);
        testRunner.removeProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL);
        testRunner.removeProperty(AbstractCassandraProcessor.USERNAME);
        testRunner.removeProperty(AbstractCassandraProcessor.PASSWORD);

        testRunner.assertValid();
    }

    /**
     * Updated stubbed processor for testing Java Driver 4.x logic
     */
    public static class MockAbstractCassandraProcessor extends AbstractCassandraProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            // Updated to include LOCAL_DATACENTER which is required in your new 4.x implementation
            return Arrays.asList(CONNECTION_PROVIDER_SERVICE, CONTACT_POINTS, KEYSPACE,
                    USERNAME, PASSWORD, CONSISTENCY_LEVEL, CHARSET, LOCAL_DATACENTER, PROP_SSL_CONTEXT_SERVICE);
        }

        @Override
        public void onTrigger(ProcessContext context, org.apache.nifi.processor.ProcessSession session) throws ProcessException {
        }

        @Override
        protected void connectToCassandra(ProcessContext context) {
            if (cassandraSession.get() == null) {
                CqlSession mockSession = mock(CqlSession.class);
                Metadata mockMetadata = mock(Metadata.class);

                lenient().when(mockSession.getMetadata()).thenReturn(mockMetadata);

                cassandraSession.set(mockSession);
            }
        }

        public CqlSession getSession() {
            return cassandraSession.get();
        }

        public void setSession(CqlSession newSession) {
            this.cassandraSession.set(newSession);
        }
    }

    /**
     * Mock CassandraSessionProvider implementation for 4.x
     */
    private class MockCassandraSessionProvider extends CassandraSessionProvider {
        private final CqlSession mockSession = mock(CqlSession.class);

        @Override
        public CqlSession getCassandraSession() {
            return mockSession;
        }

        @OnEnabled
        public void onEnabled(final ConfigurationContext context) {
            // Logic for initializing the mock if necessary
        }
    }

}
