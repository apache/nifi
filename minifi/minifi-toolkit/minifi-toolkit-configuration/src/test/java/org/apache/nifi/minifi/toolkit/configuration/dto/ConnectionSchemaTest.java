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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.minifi.commons.schema.ConnectionSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectionSchemaTest extends BaseSchemaTester<ConnectionSchema, ConnectionDTO> {
    private static final String testId = UUID.nameUUIDFromBytes("testId".getBytes(StandardCharsets.UTF_8)).toString();
    private static final String testName = "testName";
    private static final String testSourceId = "testSourceId";
    private static final String testSelectedRelationship = "testSelectedRelationship";
    private static final String testDestinationId = "testDestinationId";
    private static final long testMaxWorkQueueSize = 101L;
    private static final String testMaxWorkQueueDataSize = "120 GB";
    private static final String testFlowfileExpiration = "1 day";
    private static final String testQueuePrioritizerClass = "testQueuePrioritizerClass";

    public ConnectionSchemaTest() {
        super(new ConnectionSchemaFunction(), ConnectionSchema::new);
    }

    @BeforeEach
    public void setup() {
        ConnectableDTO source = new ConnectableDTO();
        source.setId(testSourceId);

        ConnectableDTO destination = new ConnectableDTO();
        destination.setId(testDestinationId);

        dto = new ConnectionDTO();
        dto.setId(testId);
        dto.setName(testName);
        dto.setSource(source);
        dto.setSelectedRelationships(Collections.singleton(testSelectedRelationship));
        dto.setDestination(destination);
        dto.setBackPressureObjectThreshold(testMaxWorkQueueSize);
        dto.setBackPressureDataSizeThreshold(testMaxWorkQueueDataSize);
        dto.setFlowFileExpiration(testFlowfileExpiration);
        dto.setPrioritizers(Collections.singletonList(testQueuePrioritizerClass));

        map = new HashMap<>();
        map.put(CommonPropertyKeys.ID_KEY, testId);
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(ConnectionSchema.SOURCE_ID_KEY, testSourceId);
        map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY, new ArrayList<>(Collections.singletonList(testSelectedRelationship)));
        map.put(ConnectionSchema.DESTINATION_ID_KEY, testDestinationId);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY, testMaxWorkQueueSize);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY, testMaxWorkQueueDataSize);
        map.put(ConnectionSchema.FLOWFILE_EXPIRATION__KEY, testFlowfileExpiration);
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, testQueuePrioritizerClass);
    }

    @Test
    public void testNoName() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoId() {
        dto.setId(null);
        map.remove(CommonPropertyKeys.ID_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoSourceId() {
        dto.setSource(new ConnectableDTO());
        map.remove(ConnectionSchema.SOURCE_ID_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testDtoMultipleSourceRelationships() {
        List<String> relationships = Arrays.asList("one", "two");
        dto.setSelectedRelationships(new LinkedHashSet<>(relationships));
        map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY, new ArrayList<>(relationships));
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoSelectedRelationshipName() {
        dto.setSelectedRelationships(null);
        map.remove(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY);
        assertDtoAndMapConstructorAreSame(1);
        dto.setSelectedRelationships(Collections.emptySet());
        map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAMES_KEY, new ArrayList<>());
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoDestinationName() {
        dto.setDestination(new ConnectableDTO());
        map.remove(ConnectionSchema.DESTINATION_ID_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoMaxWorkQueueSize() {
        dto.setBackPressureObjectThreshold(null);
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoMaxWorkQueueDataSize() {
        dto.setBackPressureDataSizeThreshold(null);
        map.remove(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoFlowFileExpiration() {
        dto.setFlowFileExpiration(null);
        map.remove(ConnectionSchema.FLOWFILE_EXPIRATION__KEY);
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testNoQueuePrioritizerClass() {
        dto.setPrioritizers(null);
        map.remove(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY);
        assertDtoAndMapConstructorAreSame(0);
        dto.setPrioritizers(Collections.emptyList());
        assertDtoAndMapConstructorAreSame(0);
    }

    @Test
    public void testFunnelValidationMessage() {
        dto.getSource().setType(ConnectableType.FUNNEL.name());
        assertEquals(1, dtoSchemaFunction.apply(dto).getValidationIssues().size());
    }

    @Override
    public void assertSchemaEquals(ConnectionSchema one, ConnectionSchema two) {
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getId(), two.getId());
        assertEquals(one.getSourceId(), two.getSourceId());
        assertEquals(one.getSourceRelationshipNames(), two.getSourceRelationshipNames());
        assertEquals(one.getDestinationId(), two.getDestinationId());
        assertEquals(one.getMaxWorkQueueSize(), two.getMaxWorkQueueSize());
        assertEquals(one.getMaxWorkQueueDataSize(), two.getMaxWorkQueueDataSize());
        assertEquals(one.getFlowfileExpiration(), two.getFlowfileExpiration());
        assertEquals(one.getQueuePrioritizerClass(), two.getQueuePrioritizerClass());
    }
}
