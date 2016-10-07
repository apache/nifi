/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.commons.schema.v1;

import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.ConfigSchemaTest.assertMessageDoesNotExist;
import static org.apache.nifi.minifi.commons.schema.ConfigSchemaTest.getListWithKeyValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectionSchemaV1Test {
    private String testName;
    private String testSourceRelationship;
    private String testSourceName;
    private String testDestinationName;
    private int testMaxWorkQueueSize;
    private String testMaxWorkQueueDataSize;
    private String testFlowfileExpiration;
    private String testQueuePrioritizerClass;

    @Before
    public void setup() {
        testName = "testName";
        testSourceRelationship = "testSourceRelationship";
        testSourceName = "testSourceName";
        testDestinationName = "testDestinationName";
        testMaxWorkQueueSize = 55;
        testMaxWorkQueueDataSize = "testMaxWorkQueueDataSize";
        testFlowfileExpiration = "testFlowfileExpiration";
        testQueuePrioritizerClass = "testQueuePrioritizerClass";
    }

    private ConnectionSchemaV1 createSchema(int expectedValidationIssues) {
        return createSchema(createMap(), expectedValidationIssues);
    }

    private ConnectionSchemaV1 createSchema(Map<String, Object> map, int expectedValidationIssues) {
        ConnectionSchemaV1 connectionSchema = new ConnectionSchemaV1(map);
        assertEquals(expectedValidationIssues, connectionSchema.getValidationIssues().size());
        return connectionSchema;
    }

    private Map<String, Object> createMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(ConnectionSchemaV1.SOURCE_RELATIONSHIP_NAME_KEY, testSourceRelationship);
        map.put(ConnectionSchemaV1.SOURCE_NAME_KEY, testSourceName);
        map.put(ConnectionSchemaV1.DESTINATION_NAME_KEY, testDestinationName);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY, testMaxWorkQueueSize);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY, testMaxWorkQueueDataSize);
        map.put(ConnectionSchema.FLOWFILE_EXPIRATION__KEY, testFlowfileExpiration);
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, testQueuePrioritizerClass);
        return map;
    }

    @Test
    public void testName() {
        ConnectionSchemaV1 schema = createSchema(0);
        assertEquals(testName, schema.getName());
        assertEquals(schema.getName(), schema.convert().getName());
    }

    @Test
    public void testNoName() {
        Map<String, Object> map = createMap();
        map.remove(CommonPropertyKeys.NAME_KEY);
        ConnectionSchemaV1 schema = createSchema(map, 1);
        assertNull(schema.getName());
        assertEquals("", schema.convert().getName());
    }

    @Test
    public void testSourceRelationShipName() {
        ConnectionSchemaV1 schema = createSchema(0);
        List<String> sourceRelationshipNames = schema.convert().getSourceRelationshipNames();
        assertEquals(1, sourceRelationshipNames.size());
        assertEquals(testSourceRelationship, sourceRelationshipNames.get(0));
    }

    @Test
    public void testNoSourceRelationshipName() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchemaV1.SOURCE_RELATIONSHIP_NAME_KEY);
        ConnectionSchemaV1 schema = createSchema(map, 1);
        List<String> sourceRelationshipNames = schema.convert().getSourceRelationshipNames();
        assertEquals(0, sourceRelationshipNames.size());
    }

    @Test
    public void testDestinationName() {
        assertEquals(testDestinationName, createSchema(0).getDestinationName());
    }

    @Test
    public void testNoDestinationName() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchemaV1.DESTINATION_NAME_KEY);
        assertNull(createSchema(map, 1).getDestinationName());
    }

    @Test
    public void testMaxWorkQueueSize() {
        assertEquals(testMaxWorkQueueSize, createSchema(0).convert().getMaxWorkQueueSize());
    }

    @Test
    public void testNoMaxWorkQueueSize() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY);
        assertEquals(ConnectionSchema.DEFAULT_MAX_WORK_QUEUE_SIZE, createSchema(map, 0).convert().getMaxWorkQueueSize());
    }

    @Test
    public void testMaxWorkQueueDataSize() {
        assertEquals(testMaxWorkQueueDataSize, createSchema(0).convert().getMaxWorkQueueDataSize());
    }

    @Test
    public void testNoMaxWorkQueueDataSize() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY);
        assertEquals(ConnectionSchema.DEFAULT_MAX_QUEUE_DATA_SIZE, createSchema(map, 0).convert().getMaxWorkQueueDataSize());
    }

    @Test
    public void testFlowFileExpiration() {
        assertEquals(testFlowfileExpiration, createSchema(0).convert().getFlowfileExpiration());
    }

    @Test
    public void testNoFlowFileExpiration() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.FLOWFILE_EXPIRATION__KEY);
        assertEquals(ConnectionSchema.DEFAULT_FLOWFILE_EXPIRATION, createSchema(map, 0).convert().getFlowfileExpiration());
    }

    @Test
    public void testQueuePrioritizer() {
        assertEquals(testQueuePrioritizerClass, createSchema(0).convert().getQueuePrioritizerClass());
    }

    @Test
    public void testNoQueuePrioritizer() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY);
        assertEquals("", createSchema(map, 0).convert().getQueuePrioritizerClass());
    }

    @Test
    public void testConnectionGeneratedIds() {
        List<Map<String, Object>> listWithKeyValues = getListWithKeyValues(CommonPropertyKeys.NAME_KEY, "test", "test", "test_2", "test", "test_2");

        ConfigSchema configSchema = new ConfigSchemaV1(Collections.singletonMap(CommonPropertyKeys.CONNECTIONS_KEY, listWithKeyValues)).convert();
        assertMessageDoesNotExist(configSchema, ConfigSchema.FOUND_THE_FOLLOWING_DUPLICATE_CONNECTION_IDS);
        List<ConnectionSchema> connections = configSchema.getConnections();
        assertEquals(5, connections.size());

        // Generated unique ids
        assertEquals("test", connections.get(0).getId());
        assertEquals("test_2", connections.get(1).getId());
        assertEquals("test_2_2", connections.get(2).getId());
        assertEquals("test_3", connections.get(3).getId());
        assertEquals("test_2_3", connections.get(4).getId());
    }
}
