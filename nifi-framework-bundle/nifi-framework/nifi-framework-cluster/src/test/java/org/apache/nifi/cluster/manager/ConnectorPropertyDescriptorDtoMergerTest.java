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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.web.api.dto.AllowableValueDTO;
import org.apache.nifi.web.api.dto.ConnectorPropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConnectorPropertyDescriptorDtoMergerTest {

    @Test
    void testMergeWithNoAllowableValues() {
        final ConnectorPropertyDescriptorDTO clientPropertyDescriptor = new ConnectorPropertyDescriptorDTO();

        final Map<NodeIdentifier, ConnectorPropertyDescriptorDTO> dtoMap =
                Map.of(createNodeIdentifier("node1"), new ConnectorPropertyDescriptorDTO(),
                       createNodeIdentifier("node2"), new ConnectorPropertyDescriptorDTO());

        ConnectorPropertyDescriptorDtoMerger.merge(clientPropertyDescriptor, dtoMap);

        assertNull(clientPropertyDescriptor.getAllowableValues());
    }

    @Test
    void testMergeWithEmptyAllowableValuesList() {
        testMerge(
            createPropertyDescriptorDTO(),
            Map.of(createNodeIdentifier("node1"), createPropertyDescriptorDTO(),
                   createNodeIdentifier("node2"), createPropertyDescriptorDTO()),
            createPropertyDescriptorDTO()
        );
    }

    @Test
    void testMergeWithSingleNode() {
        testMerge(
            createPropertyDescriptorDTO(v("value1"), v("value2")),
            Collections.emptyMap(),
            createPropertyDescriptorDTO(v("value1"), v("value2"))
        );
    }

    @Test
    void testMergeWithNonOverlappingAllowableValues() {
        testMerge(
            createPropertyDescriptorDTO(v("value1"), v("value2")),
            Map.of(createNodeIdentifier("node1"), createPropertyDescriptorDTO(v("value3")),
                   createNodeIdentifier("node2"), createPropertyDescriptorDTO(v("value4"), v("value5"), v("value6"))),
            createPropertyDescriptorDTO()
        );
    }

    @Test
    void testMergeWithOverlappingAllowableValues() {
        testMerge(
            createPropertyDescriptorDTO(v("value1"), v("value2"), v("value3")),
            Map.of(createNodeIdentifier("node1"), createPropertyDescriptorDTO(v("value1"), v("value2"), v("value3")),
                   createNodeIdentifier("node2"), createPropertyDescriptorDTO(v("value2"), v("value3", false))),
            createPropertyDescriptorDTO(v("value2"), v("value3", false))
        );
    }

    @Test
    void testMergeWithIdenticalAllowableValues() {
        testMerge(
            createPropertyDescriptorDTO(v("value1"), v("value2")),
            Map.of(createNodeIdentifier("node1"), createPropertyDescriptorDTO(v("value1"), v("value2")),
                   createNodeIdentifier("node2"), createPropertyDescriptorDTO(v("value1"), v("value2"))),
            createPropertyDescriptorDTO(v("value1"), v("value2"))
        );
    }

    @Test
    void testMergeWithDuplicateAllowableValues() {
        testMerge(
            createPropertyDescriptorDTO(v("value1"), v("value1")),
            Map.of(createNodeIdentifier("node1"), createPropertyDescriptorDTO(v("value1"), v("value1")),
                   createNodeIdentifier("node2"), createPropertyDescriptorDTO(v("value1"), v("value1"))),
            createPropertyDescriptorDTO(v("value1"), v("value1"))
        );
    }

    private ConnectorPropertyDescriptorDTO createPropertyDescriptorDTO(final AllowableValueData... allowableValueData) {
        final ConnectorPropertyDescriptorDTO clientPropertyDescriptor = new ConnectorPropertyDescriptorDTO();

        final List<AllowableValueEntity> allowableValueEntities = Arrays.stream(allowableValueData)
            .map(AllowableValueData::toEntity)
            .collect(Collectors.toList());

        clientPropertyDescriptor.setAllowableValues(allowableValueEntities);

        return clientPropertyDescriptor;
    }

    private NodeIdentifier createNodeIdentifier(final String id) {
        return new NodeIdentifier(id, id, 1, id, 1, id, 1, null, false);
    }

    private void testMerge(final ConnectorPropertyDescriptorDTO clientPropertyDescriptor,
                          final Map<NodeIdentifier, ConnectorPropertyDescriptorDTO> dtoMap,
                          final ConnectorPropertyDescriptorDTO expected) {
        ConnectorPropertyDescriptorDtoMerger.merge(clientPropertyDescriptor, dtoMap);

        final List<Function<AllowableValueEntity, Object>> equalsProperties = Arrays.asList(
            AllowableValueEntity::getAllowableValue,
            AllowableValueEntity::getCanRead
        );

        final List<EqualsWrapper<AllowableValueEntity>> expectedWrappers = EqualsWrapper.wrapList(expected.getAllowableValues(), equalsProperties);
        final List<EqualsWrapper<AllowableValueEntity>> actualWrappers = EqualsWrapper.wrapList(clientPropertyDescriptor.getAllowableValues(), equalsProperties);

        assertEquals(expectedWrappers, actualWrappers);
    }

    private static class AllowableValueData {
        private final String value;
        private final Boolean canRead;

        private AllowableValueData(final String value, final Boolean canRead) {
            this.value = value;
            this.canRead = canRead;
        }

        private AllowableValueEntity toEntity() {
            final AllowableValueEntity entity = new AllowableValueEntity();

            final AllowableValueDTO allowableValueDTO = new AllowableValueDTO();
            allowableValueDTO.setValue(value);

            entity.setAllowableValue(allowableValueDTO);
            entity.setCanRead(canRead);

            return entity;
        }
    }

    private static AllowableValueData v(final String value) {
        return v(value, true);
    }

    private static AllowableValueData v(final String value, final Boolean canRead) {
        return new AllowableValueData(value, canRead);
    }
}
