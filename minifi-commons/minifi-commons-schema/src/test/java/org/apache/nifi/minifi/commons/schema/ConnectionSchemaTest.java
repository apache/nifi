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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConnectionSchemaTest {
    private String testId;
    private String testName;
    private String testSourceId;
    private String testSourceRelationShip1;
    private String testSourceRelationShip2;
    private List<String> testSourceRelationships;
    private String testDestinationId;
    private int testMaxWorkQueueSize;
    private String testMaxWorkQueueDataSize;
    private String testFlowfileExpiration;
    private String testQueuePrioritizerClass;

    @Before
    public void setup() {
        testId = "testId";
        testName = "testName";
        testSourceId = "testSourceId";
        testSourceRelationShip1 = "testSourceRelationShip1";
        testSourceRelationShip2 = "testSourceRelationShip2";
        testSourceRelationships = Arrays.asList(testSourceRelationShip1, testSourceRelationShip2);
        testDestinationId = "testDestinationId";
        testMaxWorkQueueSize = 55;
        testMaxWorkQueueDataSize = "testMaxWorkQueueDataSize";
        testFlowfileExpiration = "testFlowfileExpiration";
        testQueuePrioritizerClass = "testQueuePrioritizerClass";
    }

    private ConnectionSchema createSchema(int expectedValidationIssues) {
        return createSchema(createMap(), expectedValidationIssues);
    }

    private ConnectionSchema createSchema(Map<String, Object> map, int expectedValidationIssues) {
        ConnectionSchema connectionSchema = new ConnectionSchema(map);
        assertEquals(connectionSchema.getValidationIssues().toString(), expectedValidationIssues, connectionSchema.getValidationIssues().size());
        return connectionSchema;
    }

    private Map<String, Object> createMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, testId);
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(ConnectionSchema.SOURCE_ID_KEY, testSourceId);
        map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY, testSourceRelationships);
        map.put(ConnectionSchema.DESTINATION_ID_KEY, testDestinationId);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY, testMaxWorkQueueSize);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY, testMaxWorkQueueDataSize);
        map.put(ConnectionSchema.FLOWFILE_EXPIRATION__KEY, testFlowfileExpiration);
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, testQueuePrioritizerClass);
        return map;
    }

    @Test
    public void testIdKey() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testId, schema.getId());
        assertEquals(schema.getId(), schema.toMap().get(CommonPropertyKeys.ID_KEY));
    }

    @Test
    public void testNoId() {
        Map<String, Object> map = createMap();
        map.remove(CommonPropertyKeys.ID_KEY);
        ConnectionSchema schema = createSchema(map, 1);
        assertEquals("", schema.getId());
        assertEquals(schema.getId(), schema.toMap().get(CommonPropertyKeys.ID_KEY));
    }

    @Test
    public void testName() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testName, schema.getName());
        assertEquals(schema.getName(), schema.toMap().get(CommonPropertyKeys.NAME_KEY));
    }

    @Test
    public void testNoName() {
        Map<String, Object> map = createMap();
        map.remove(CommonPropertyKeys.NAME_KEY);
        ConnectionSchema schema = createSchema(map, 0);
        assertEquals("", schema.getName());
        assertEquals(schema.getName(), schema.toMap().get(CommonPropertyKeys.NAME_KEY));
    }

    @Test
    public void testSourceId() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testSourceId, schema.getSourceId());
        assertEquals(schema.getSourceId(), schema.toMap().get(ConnectionSchema.SOURCE_ID_KEY));
    }

    @Test
    public void testNoSourceId() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.SOURCE_ID_KEY);
        ConnectionSchema schema = createSchema(map, 1);
        assertEquals("", schema.getSourceId());
        assertEquals(schema.getSourceId(), schema.toMap().get(ConnectionSchema.SOURCE_ID_KEY));
    }

    @Test
    public void testSourceRelationshipNames() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testSourceRelationships, schema.getSourceRelationshipNames());
        assertEquals(schema.getSourceRelationshipNames(), schema.toMap().get(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY));
    }

    @Test
    public void testNoSourceRelationshipNames() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY);
        ConnectionSchema schema = createSchema(map, 1);
        assertEquals(new ArrayList<>(), schema.getSourceRelationshipNames());
        assertEquals(schema.getSourceRelationshipNames(), schema.toMap().get(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY));
    }

    @Test
    public void testDestinationId() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testDestinationId, schema.getDestinationId());
        assertEquals(schema.getDestinationId(), schema.toMap().get(ConnectionSchema.DESTINATION_ID_KEY));
    }

    @Test
    public void testNoDestinationId() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.DESTINATION_ID_KEY);
        ConnectionSchema schema = createSchema(map, 1);
        assertEquals("", schema.getDestinationId());
        assertEquals(schema.getDestinationId(), schema.toMap().get(ConnectionSchema.DESTINATION_ID_KEY));
    }

    @Test
    public void testMaxWorkQueueSize() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testMaxWorkQueueSize, schema.getMaxWorkQueueSize());
        assertEquals(schema.getMaxWorkQueueSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY));
    }

    @Test
    public void testNoMaxWorkQueueSize() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY);
        ConnectionSchema schema = createSchema(map, 0);
        assertEquals(ConnectionSchema.DEFAULT_MAX_WORK_QUEUE_SIZE, schema.getMaxWorkQueueSize());
        assertEquals(schema.getMaxWorkQueueSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY));
    }

    @Test
    public void testMaxWorkQueueDataSize() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testMaxWorkQueueDataSize, schema.getMaxWorkQueueDataSize());
        assertEquals(schema.getMaxWorkQueueDataSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY));
    }

    @Test
    public void testNoMaxWorkQueueDataSize() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY);
        ConnectionSchema schema = createSchema(map, 0);
        assertEquals(ConnectionSchema.DEFAULT_MAX_QUEUE_DATA_SIZE, schema.getMaxWorkQueueDataSize());
        assertEquals(schema.getMaxWorkQueueDataSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY));
    }

    @Test
    public void testFlowFileExpiration() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testFlowfileExpiration, schema.getFlowfileExpiration());
        assertEquals(schema.getFlowfileExpiration(), schema.toMap().get(ConnectionSchema.FLOWFILE_EXPIRATION__KEY));
    }

    @Test
    public void testNoFlowFileExpiration() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.FLOWFILE_EXPIRATION__KEY);
        ConnectionSchema schema = createSchema(map, 0);
        assertEquals(ConnectionSchema.DEFAULT_FLOWFILE_EXPIRATION, schema.getFlowfileExpiration());
        assertEquals(schema.getFlowfileExpiration(), schema.toMap().get(ConnectionSchema.FLOWFILE_EXPIRATION__KEY));
    }

    @Test
    public void testQueuePrioritizer() {
        ConnectionSchema schema = createSchema(0);
        assertEquals(testQueuePrioritizerClass, schema.getQueuePrioritizerClass());
        assertEquals(schema.getQueuePrioritizerClass(), schema.toMap().get(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY));
    }

    @Test
    public void testNoQueuePrioritizer() {
        Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY);
        ConnectionSchema schema = createSchema(map, 0);
        assertEquals("", schema.getQueuePrioritizerClass());
        assertEquals(schema.getQueuePrioritizerClass(), schema.toMap().get(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY));
    }
}
