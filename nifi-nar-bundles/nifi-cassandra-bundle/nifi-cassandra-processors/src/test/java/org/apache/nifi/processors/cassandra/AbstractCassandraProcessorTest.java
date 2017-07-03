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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.google.common.collect.Sets;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the AbstractCassandraProcessor class
 */
public class AbstractCassandraProcessorTest {

    MockAbstractCassandraProcessor processor;
    private TestRunner testRunner;

    @Before
    public void setUp() throws Exception {
        processor = new MockAbstractCassandraProcessor();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testCustomValidate() throws Exception {
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
    public void testCustomValidateEL() throws Exception {
        testRunner.setProperty(AbstractCassandraProcessor.CONTACT_POINTS, "${host}");
        testRunner.setProperty(AbstractCassandraProcessor.KEYSPACE, "${keyspace}");
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "${user}");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "${password}");
        testRunner.setProperty(AbstractCassandraProcessor.CHARSET, "${charset}");
        testRunner.assertValid();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetCassandraObject() throws Exception {
        Row row = CassandraQueryTestUtil.createRow("user1", "Joe", "Smith",
                Sets.newHashSet("jsmith@notareal.com", "joes@fakedomain.com"), Arrays.asList("New York, NY", "Santa Clara, CA"),
                new HashMap<Date, String>() {{
                    put(Calendar.getInstance().getTime(), "Set my alarm for a month from now");
                }}, true, 1.0f, 2.0);

        assertEquals("user1", AbstractCassandraProcessor.getCassandraObject(row, 0, DataType.text()));
        assertEquals("Joe", AbstractCassandraProcessor.getCassandraObject(row, 1, DataType.text()));
        assertEquals("Smith", AbstractCassandraProcessor.getCassandraObject(row, 2, DataType.text()));
        Set<String> emails = (Set<String>) AbstractCassandraProcessor.getCassandraObject(row, 3, DataType.set(DataType.text()));
        assertNotNull(emails);
        assertEquals(2, emails.size());
        List<String> topPlaces = (List<String>) AbstractCassandraProcessor.getCassandraObject(row, 4, DataType.list(DataType.text()));
        assertNotNull(topPlaces);
        Map<Date, String> todoMap = (Map<Date, String>) AbstractCassandraProcessor.getCassandraObject(
                row, 5, DataType.map(DataType.timestamp(), DataType.text()));
        assertNotNull(todoMap);
        assertEquals(1, todoMap.values().size());
        Boolean registered = (Boolean) AbstractCassandraProcessor.getCassandraObject(row, 6, DataType.cboolean());
        assertNotNull(registered);
        assertTrue(registered);
    }

    @Test
    public void testGetSchemaForType() throws Exception {
        assertEquals(AbstractCassandraProcessor.getSchemaForType("string").getType().getName(), "string");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("boolean").getType().getName(), "boolean");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("int").getType().getName(), "int");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("long").getType().getName(), "long");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("float").getType().getName(), "float");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("double").getType().getName(), "double");
        assertEquals(AbstractCassandraProcessor.getSchemaForType("bytes").getType().getName(), "bytes");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSchemaForTypeBadType() throws Exception {
        AbstractCassandraProcessor.getSchemaForType("nothing");
    }

    @Test
    public void testGetPrimitiveAvroTypeFromCassandraType() throws Exception {
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.ascii()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.text()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.varchar()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.timestamp()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.timeuuid()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.uuid()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.inet()));
        assertEquals("string", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.varint()));

        assertEquals("boolean", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cboolean()));
        assertEquals("int", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cint()));

        assertEquals("long", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.bigint()));
        assertEquals("long", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.counter()));

        assertEquals("float", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cfloat()));
        assertEquals("double", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.cdouble()));

        assertEquals("bytes", AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(DataType.blob()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPrimitiveAvroTypeFromCassandraTypeBadType() throws Exception {
        DataType mockDataType = mock(DataType.class);
        AbstractCassandraProcessor.getPrimitiveAvroTypeFromCassandraType(mockDataType);
    }

    @Test
    public void testGetPrimitiveDataTypeFromString() {
        assertEquals(DataType.ascii(), AbstractCassandraProcessor.getPrimitiveDataTypeFromString("ascii"));
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
        // Follow the non-null path
        Cluster cluster = mock(Cluster.class);
        processor.setCluster(cluster);
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        processor.connectToCassandra(testRunner.getProcessContext());
        processor.stop();
        assertNull(processor.getCluster());

        // Now do a connect where a cluster is "built"
        processor.connectToCassandra(testRunner.getProcessContext());
        assertEquals("cluster1", processor.getCluster().getMetadata().getClusterName());
    }

    @Test
    public void testConnectToCassandraWithSSL() throws Exception {
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        testRunner.addControllerService("ssl-context", sslService);
        testRunner.enableControllerService(sslService);
        testRunner.setProperty(AbstractCassandraProcessor.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.assertValid(sslService);
        processor.connectToCassandra(testRunner.getProcessContext());
        assertNotNull(processor.getCluster());
        processor.setCluster(null);
        // Try with a ClientAuth value
        testRunner.setProperty(AbstractCassandraProcessor.CLIENT_AUTH, "WANT");
        processor.connectToCassandra(testRunner.getProcessContext());
        assertNotNull(processor.getCluster());
    }

    @Test(expected = ProviderCreationException.class)
    public void testConnectToCassandraWithSSLBadClientAuth() throws Exception {
        SSLContextService sslService = mock(SSLContextService.class);
        when(sslService.getIdentifier()).thenReturn("ssl-context");
        testRunner.addControllerService("ssl-context", sslService);
        testRunner.enableControllerService(sslService);
        testRunner.setProperty(AbstractCassandraProcessor.PROP_SSL_CONTEXT_SERVICE, "ssl-context");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        testRunner.assertValid(sslService);
        processor.connectToCassandra(testRunner.getProcessContext());
        assertNotNull(processor.getCluster());
        processor.setCluster(null);
        // Try with a ClientAuth value
        testRunner.setProperty(AbstractCassandraProcessor.CLIENT_AUTH, "BAD");
        processor.connectToCassandra(testRunner.getProcessContext());
    }

    @Test
    public void testConnectToCassandraUsernamePassword() throws Exception {
        testRunner.setProperty(AbstractCassandraProcessor.USERNAME, "user");
        testRunner.setProperty(AbstractCassandraProcessor.PASSWORD, "password");
        testRunner.setProperty(AbstractCassandraProcessor.CONSISTENCY_LEVEL, "ONE");
        // Now do a connect where a cluster is "built"
        processor.connectToCassandra(testRunner.getProcessContext());
        assertNotNull(processor.getCluster());
    }

    /**
     * Provides a stubbed processor instance for testing
     */
    public static class MockAbstractCassandraProcessor extends AbstractCassandraProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Arrays.asList(CONTACT_POINTS, KEYSPACE, USERNAME, PASSWORD, CONSISTENCY_LEVEL, CHARSET);
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }

        @Override
        protected Cluster createCluster(List<InetSocketAddress> contactPoints, SSLContext sslContext,
                                        String username, String password) {
            Cluster mockCluster = mock(Cluster.class);
            Metadata mockMetadata = mock(Metadata.class);
            when(mockMetadata.getClusterName()).thenReturn("cluster1");
            when(mockCluster.getMetadata()).thenReturn(mockMetadata);
            Configuration config = Configuration.builder().build();
            when(mockCluster.getConfiguration()).thenReturn(config);
            return mockCluster;
        }

        public Cluster getCluster() {
            return cluster.get();
        }

        public void setCluster(Cluster newCluster) {
            this.cluster.set(newCluster);
        }
    }
}