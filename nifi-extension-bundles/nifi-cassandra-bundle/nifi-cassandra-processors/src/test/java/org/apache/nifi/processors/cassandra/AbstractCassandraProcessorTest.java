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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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
        testRunner.assertNotValid();

        final CqlSession mockSession = mock(CqlSession.class);
        final MockCassandraSessionProviderService sessionProvider = new MockCassandraSessionProviderService(mockSession);
        testRunner.addControllerService("cassandra-session-provider", sessionProvider);
        testRunner.enableControllerService(sessionProvider);
        testRunner.setProperty(AbstractCassandraProcessor.CONNECTION_PROVIDER_SERVICE, "cassandra-session-provider");
        testRunner.assertValid();
    }

    @Test
    public void testCustomValidateValidValues() throws Exception {
        final CqlSession mockSession = mock(CqlSession.class);
        final MockCassandraSessionProviderService sessionProvider = new MockCassandraSessionProviderService(mockSession);
        testRunner.addControllerService("cassandra-session-provider", sessionProvider);
        testRunner.enableControllerService(sessionProvider);
        testRunner.setProperty(AbstractCassandraProcessor.CONNECTION_PROVIDER_SERVICE, "cassandra-session-provider");
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

    /**
     * Updated stubbed processor for testing Java Driver 4.x logic
     */
    public static class MockAbstractCassandraProcessor extends AbstractCassandraProcessor {

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return COMMON_PROPERTY_DESCRIPTORS;
        }

        @Override
        public void onTrigger(ProcessContext context, org.apache.nifi.processor.ProcessSession session) throws ProcessException {
        }
    }

}
