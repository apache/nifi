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
import org.apache.nifi.web.api.entity.ClearBulletinsForGroupResultsEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClearBulletinsForGroupEndpointMergerTest {

    @Test
    public void testCanHandle() {
        final ClearBulletinsForGroupEndpointMerger merger = new ClearBulletinsForGroupEndpointMerger();

        // Test valid URIs
        assertTrue(merger.canHandle(URI.create("/nifi-api/process-groups/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));

        // Test invalid URIs
        assertFalse(merger.canHandle(URI.create("/nifi-api/process-groups/12345678-1234-1234-1234-123456789012/bulletins"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/processors/12345678-1234-1234-1234-123456789012/bulletins"), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/process-groups/invalid-id/bulletins"), "POST"));
    }

    @Test
    public void testMergeResponses() {
        final ClearBulletinsForGroupEndpointMerger merger = new ClearBulletinsForGroupEndpointMerger();

        // Create test entities
        final ClearBulletinsForGroupResultsEntity clientEntity = new ClearBulletinsForGroupResultsEntity();
        clientEntity.setBulletinsCleared(10);

        final Map<NodeIdentifier, ClearBulletinsForGroupResultsEntity> entityMap = new HashMap<>();

        // Node 1
        final NodeIdentifier node1 = new NodeIdentifier("node1", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final ClearBulletinsForGroupResultsEntity entity1 = new ClearBulletinsForGroupResultsEntity();
        entity1.setBulletinsCleared(10);
        entityMap.put(node1, entity1);

        // Node 2
        final NodeIdentifier node2 = new NodeIdentifier("node2", "localhost", 8090, "localhost", 8091, "localhost", 8092, 8093, false);
        final ClearBulletinsForGroupResultsEntity entity2 = new ClearBulletinsForGroupResultsEntity();
        entity2.setBulletinsCleared(7);
        entityMap.put(node2, entity2);

        // Merge responses
        merger.mergeResponses(clientEntity, entityMap, null, null);

        // Verify merged results
        assertEquals(17, clientEntity.getBulletinsCleared()); // 10 + 7
    }
}
