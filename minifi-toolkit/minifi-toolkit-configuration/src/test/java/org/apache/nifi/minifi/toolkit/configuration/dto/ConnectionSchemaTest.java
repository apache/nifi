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
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ConnectionSchemaTest extends BaseSchemaTester<ConnectionSchema, ConnectionDTO> {
    private String testName = "testName";
    private String testSourceName = "testSourceName";
    private String testSelectedRelationship = "testSelectedRelationship";
    private String testDestinationName = "testDestinationName";
    private long testMaxWorkQueueSize = 101L;
    private String testMaxWorkQueueDataSize = "120 GB";
    private String testFlowfileExpiration = "1 day";
    private String testQueuePrioritizerClass = "testQueuePrioritizerClass";

    public ConnectionSchemaTest() {
        super(new ConnectionSchemaFunction(), ConnectionSchema::new);
    }

    @Before
    public void setup() {
        ConnectableDTO source = new ConnectableDTO();
        source.setName(testSourceName);

        ConnectableDTO destination = new ConnectableDTO();
        destination.setName(testDestinationName);

        dto = new ConnectionDTO();
        dto.setName(testName);
        dto.setSource(source);
        dto.setSelectedRelationships(Arrays.asList(testSelectedRelationship).stream().collect(Collectors.toSet()));
        dto.setDestination(destination);
        dto.setBackPressureObjectThreshold(testMaxWorkQueueSize);
        dto.setBackPressureDataSizeThreshold(testMaxWorkQueueDataSize);
        dto.setFlowFileExpiration(testFlowfileExpiration);
        dto.setPrioritizers(Arrays.asList(testQueuePrioritizerClass));

        map = new HashMap<>();
        map.put(CommonPropertyKeys.NAME_KEY, testName);
        map.put(ConnectionSchema.SOURCE_NAME_KEY, testSourceName);
        map.put(ConnectionSchema.SOURCE_RELATIONSHIP_NAME_KEY, testSelectedRelationship);
        map.put(ConnectionSchema.DESTINATION_NAME_KEY, testDestinationName);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_SIZE_KEY, testMaxWorkQueueSize);
        map.put(ConnectionSchema.MAX_WORK_QUEUE_DATA_SIZE_KEY, testMaxWorkQueueDataSize);
        map.put(ConnectionSchema.FLOWFILE_EXPIRATION__KEY, testFlowfileExpiration);
        map.put(ConnectionSchema.QUEUE_PRIORITIZER_CLASS_KEY, testQueuePrioritizerClass);
    }

    @Test
    public void testNoName() {
        dto.setName(null);
        map.remove(CommonPropertyKeys.NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoSourceName() {
        dto.setSource(new ConnectableDTO());
        map.remove(ConnectionSchema.SOURCE_NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testDtoMultipleSourceRelationships() {
        dto.setSelectedRelationships(Arrays.asList("one", "two").stream().collect(Collectors.toSet()));
        assertEquals(1, dtoSchemaFunction.apply(dto).validationIssues.size());
    }

    @Test
    public void testNoSelectedRelationshipName() {
        dto.setSelectedRelationships(null);
        map.remove(ConnectionSchema.SOURCE_RELATIONSHIP_NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
        dto.setSelectedRelationships(Collections.emptySet());
        map.remove(ConnectionSchema.SOURCE_RELATIONSHIP_NAME_KEY);
        assertDtoAndMapConstructorAreSame(1);
    }

    @Test
    public void testNoDestinationName() {
        dto.setDestination(new ConnectableDTO());
        map.remove(ConnectionSchema.DESTINATION_NAME_KEY);
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
        assertEquals(1, dtoSchemaFunction.apply(dto).validationIssues.size());
    }

    @Override
    public void assertSchemaEquals(ConnectionSchema one, ConnectionSchema two) {
        assertEquals(one.getName(), two.getName());
        assertEquals(one.getSourceName(), two.getSourceName());
        assertEquals(one.getSourceRelationshipName(), two.getSourceRelationshipName());
        assertEquals(one.getDestinationName(), two.getDestinationName());
        assertEquals(one.getMaxWorkQueueSize(), two.getMaxWorkQueueSize());
        assertEquals(one.getMaxWorkQueueDataSize(), two.getMaxWorkQueueDataSize());
        assertEquals(one.getFlowfileExpiration(), two.getFlowfileExpiration());
        assertEquals(one.getQueuePrioritizerClass(), two.getQueuePrioritizerClass());
    }
}
