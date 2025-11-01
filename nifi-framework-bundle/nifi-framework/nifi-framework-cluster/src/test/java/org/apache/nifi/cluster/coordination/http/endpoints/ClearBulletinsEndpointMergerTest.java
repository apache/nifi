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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ClearBulletinsResultEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClearBulletinsEndpointMergerTest {

    @Test
    public void testCanHandle() {
        final ClearBulletinsEndpointMerger merger = new ClearBulletinsEndpointMerger();

        // Test valid URIs
        assertTrue(merger.canHandle(URI.create("/nifi-api/processors/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/controller-services/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/reporting-tasks/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/parameter-providers/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/registry-clients/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/flow-analysis-rules/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/remote-process-groups/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));

        // Test invalid URIs
        assertFalse(merger.canHandle(URI.create("/nifi-api/processors/12345678-1234-1234-1234-123456789012/bulletins"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/process-groups/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/processors/invalid-id/bulletins"), "POST"));
    }

    @Test
    public void testMergeResponses() {
        final ClearBulletinsEndpointMerger merger = new ClearBulletinsEndpointMerger();

        // Create test entities
        final ClearBulletinsResultEntity clientEntity = new ClearBulletinsResultEntity();
        clientEntity.setComponentId("test-component");
        clientEntity.setBulletinsCleared(5);
        clientEntity.setBulletins(createTestBulletins(2));

        final Map<NodeIdentifier, ClearBulletinsResultEntity> entityMap = new HashMap<>();

        // Node 1
        final NodeIdentifier node1 = new NodeIdentifier("node1", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final ClearBulletinsResultEntity entity1 = new ClearBulletinsResultEntity();
        entity1.setComponentId("test-component");
        entity1.setBulletinsCleared(5);
        entity1.setBulletins(createTestBulletins(2));
        entityMap.put(node1, entity1);

        // Node 2
        final NodeIdentifier node2 = new NodeIdentifier("node2", "localhost", 8090, "localhost", 8091, "localhost", 8092, 8093, false);
        final ClearBulletinsResultEntity entity2 = new ClearBulletinsResultEntity();
        entity2.setComponentId("test-component");
        entity2.setBulletinsCleared(3);
        entity2.setBulletins(createTestBulletins(1));
        entityMap.put(node2, entity2);

        // Merge responses
        merger.mergeResponses(clientEntity, entityMap, null, null);

        // Verify merged results
        assertEquals(8, clientEntity.getBulletinsCleared()); // 5 + 3
        // Note: BulletinMerger may consolidate bulletins with same message, so we just verify we have bulletins
        assertFalse(clientEntity.getBulletins().isEmpty());
    }

    private List<BulletinEntity> createTestBulletins(int count) {
        final List<BulletinEntity> bulletins = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final BulletinEntity bulletin = new BulletinEntity();
            bulletin.setId((long) i);
            bulletin.setCanRead(true);
            bulletin.setTimestamp(new java.util.Date());
            bulletin.setSourceId("test-source");
            bulletin.setGroupId("test-group");

            // Create the nested BulletinDTO
            final org.apache.nifi.web.api.dto.BulletinDTO bulletinDto = new org.apache.nifi.web.api.dto.BulletinDTO();
            bulletinDto.setMessage("Test bulletin " + i);
            bulletinDto.setLevel("INFO");
            bulletinDto.setSourceId("test-source");
            bulletinDto.setTimestamp(new java.util.Date());
            bulletin.setBulletin(bulletinDto);

            bulletins.add(bulletin);
        }
        return bulletins;
    }
}
