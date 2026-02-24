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

package org.apache.nifi.minifi.toolkit.schema;

import org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    @BeforeEach
    public void setup() {
        testId = UUID.nameUUIDFromBytes("testId".getBytes(StandardCharsets.UTF_8)).toString();
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

    private ConnectionSchema createSchema(final int expectedValidationIssues) {
        return createSchema(createMap(), expectedValidationIssues);
    }

    private ConnectionSchema createSchema(final Map<String, Object> map, final int expectedValidationIssues) {
        final ConnectionSchema connectionSchema = new ConnectionSchema(map);
        assertEquals(expectedValidationIssues, connectionSchema.getValidationIssues().size(), connectionSchema.getValidationIssues().toString());
        return connectionSchema;
    }

    private Map<String, Object> createMap() {
        final Map<String, Object> map = new HashMap<>();
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
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testId, schema.getId());
        assertEquals(schema.getId(), schema.toMap().get(CommonPropertyKeys.ID_KEY));
    }

    @Test
    public void testNoId() {
        final Map<String, Object> map = createMap();
        map.remove(CommonPropertyKeys.ID_KEY);
        final ConnectionSchema schema = createSchema(map, 1);
        assertEquals("", schema.getId());
        assertEquals(schema.getId(), schema.toMap().get(CommonPropertyKeys.ID_KEY));
    }

    @Test
    public void testName() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testName, schema.getName());
        assertEquals(schema.getName(), schema.toMap().get(CommonPropertyKeys.NAME_KEY));
    }

    @Test
    public void testNoName() {
        final Map<String, Object> map = createMap();
        map.remove(CommonPropertyKeys.NAME_KEY);
        final ConnectionSchema schema = createSchema(map, 0);
        assertEquals("", schema.getName());
        assertEquals(schema.getName(), schema.toMap().get(CommonPropertyKeys.NAME_KEY));
    }

    @Test
    public void testSourceId() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testSourceId, schema.getSourceId());
        assertEquals(schema.getSourceId(), schema.toMap().get(ConnectionSchema.SOURCE_ID_KEY));
    }

    @Test
    public void testNoSourceId() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.SOURCE_ID_KEY);
        final ConnectionSchema schema = createSchema(map, 1);
        assertEquals("", schema.getSourceId());
        assertEquals(schema.getSourceId(), schema.toMap().get(ConnectionSchema.SOURCE_ID_KEY));
    }

    @Test
    public void testSourceRelationshipNames() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testSourceRelationships, schema.getSourceRelationshipNames());
        assertEquals(schema.getSourceRelationshipNames(), schema.toMap().get(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY));
    }

    @Test
    public void testNoSourceRelationshipNames() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY);
        final ConnectionSchema schema = createSchema(map, 1);
        assertEquals(new ArrayList<>(), schema.getSourceRelationshipNames());
        assertEquals(schema.getSourceRelationshipNames(), schema.toMap().get(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY));
    }

    @Test
    public void testDestinationId() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testDestinationId, schema.getDestinationId());
        assertEquals(schema.getDestinationId(), schema.toMap().get(ConnectionSchema.DESTINATION_ID_KEY));
    }

    @Test
    public void testNoDestinationId() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.DESTINATION_ID_KEY);
        final ConnectionSchema schema = createSchema(map, 1);
        assertEquals("", schema.getDestinationId());
        assertEquals(schema.getDestinationId(), schema.toMap().get(ConnectionSchema.DESTINATION_ID_KEY));
    }

    @Test
    public void testMaxWorkQueueSize() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testMaxWorkQueueSize, schema.getMaxWorkQueueSize());
        assertEquals(schema.getMaxWorkQueueSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY));
    }

    @Test
    public void testNoMaxWorkQueueSize() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY);
        final ConnectionSchema schema = createSchema(map, 0);
        assertEquals(ConnectionSchema.DEFAULT_MAX_WORK_QUEUE_SIZE, schema.getMaxWorkQueueSize());
        assertEquals(schema.getMaxWorkQueueSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY));
    }

    @Test
    public void testMaxWorkQueueDataSize() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testMaxWorkQueueDataSize, schema.getMaxWorkQueueDataSize());
        assertEquals(schema.getMaxWorkQueueDataSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY));
    }

    @Test
    public void testNoMaxWorkQueueDataSize() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY);
        final ConnectionSchema schema = createSchema(map, 0);
        assertEquals(ConnectionSchema.DEFAULT_MAX_QUEUE_DATA_SIZE, schema.getMaxWorkQueueDataSize());
        assertEquals(schema.getMaxWorkQueueDataSize(), schema.toMap().get(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY));
    }

    @Test
    public void testFlowFileExpiration() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testFlowfileExpiration, schema.getFlowfileExpiration());
        assertEquals(schema.getFlowfileExpiration(), schema.toMap().get(ConnectionSchema.FLOWFILE_EXPIRATION__KEY));
    }

    @Test
    public void testNoFlowFileExpiration() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.FLOWFILE_EXPIRATION__KEY);
        final ConnectionSchema schema = createSchema(map, 0);
        assertEquals(ConnectionSchema.DEFAULT_FLOWFILE_EXPIRATION, schema.getFlowfileExpiration());
        assertEquals(schema.getFlowfileExpiration(), schema.toMap().get(ConnectionSchema.FLOWFILE_EXPIRATION__KEY));
    }

    @Test
    public void testQueuePrioritizer() {
        final ConnectionSchema schema = createSchema(0);
        assertEquals(testQueuePrioritizerClass, schema.getQueuePrioritizerClass());
        assertEquals(schema.getQueuePrioritizerClass(), schema.toMap().get(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY));
    }

    @Test
    public void testNoQueuePrioritizer() {
        final Map<String, Object> map = createMap();
        map.remove(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY);
        final ConnectionSchema schema = createSchema(map, 0);
        assertEquals("", schema.getQueuePrioritizerClass());
        assertEquals(schema.getQueuePrioritizerClass(), schema.toMap().get(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY));
    }
}
