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
import org.apache.nifi.web.api.dto.MigrationRequestDTO;
import org.apache.nifi.web.api.dto.MigrationUpdateStepDTO;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MigrationRequestEndpointMergerTest {

    private MigrationRequestEndpointMerger merger;

    @BeforeEach
    public void setUp() {
        merger = new MigrationRequestEndpointMerger();
    }

    @Test
    public void testMergeReflectsWorstCompletionState() {
        final MigrationRequestEntity client = createMigrationRequest(true, null, 100, new Date(2_000L));
        final Map<NodeIdentifier, MigrationRequestEntity> map = new LinkedHashMap<>();
        map.put(nodeId("node-1", 8080), createMigrationRequest(true, null, 100, new Date(2_000L)));
        map.put(nodeId("node-2", 8081), createMigrationRequest(false, null, 40, new Date(1_500L)));

        merger.mergeResponses(client, map, null, null);

        // Any node still in progress drives the merged response to incomplete.
        assertFalse(client.getRequest().isComplete());
    }

    @Test
    public void testMergePropagatesFailureFromAnyNode() {
        final MigrationRequestEntity client = createMigrationRequest(true, null, 100, new Date(2_000L));
        final Map<NodeIdentifier, MigrationRequestEntity> map = new LinkedHashMap<>();
        map.put(nodeId("node-1", 8080), createMigrationRequest(true, null, 100, new Date(2_000L)));
        map.put(nodeId("node-2", 8081), createMigrationRequest(true, "rollback failed", 100, new Date(2_500L)));

        merger.mergeResponses(client, map, null, null);

        assertNotNull(client.getRequest().getFailureReason());
        assertTrue(client.getRequest().getFailureReason().contains("rollback failed"), client.getRequest().getFailureReason());
        assertTrue(client.getRequest().getFailureReason().contains("node-2"), client.getRequest().getFailureReason());
        assertTrue(client.getRequest().getState().startsWith("Failed: "), client.getRequest().getState());
    }

    @Test
    public void testMergePicksMinimumPercentComplete() {
        final MigrationRequestEntity client = createMigrationRequest(false, null, 90, new Date(2_000L));
        final Map<NodeIdentifier, MigrationRequestEntity> map = new LinkedHashMap<>();
        map.put(nodeId("node-1", 8080), createMigrationRequest(false, null, 60, new Date(2_500L)));
        map.put(nodeId("node-2", 8081), createMigrationRequest(false, null, 25, new Date(2_700L)));

        merger.mergeResponses(client, map, null, null);

        assertEquals(25, client.getRequest().getPercentCompleted());
    }

    @Test
    public void testMergePropagatesPerStepFailures() {
        final MigrationRequestEntity client = createMigrationRequest(true, null, 100, new Date(2_000L));
        final MigrationRequestEntity nodeFailure = createMigrationRequest(true, null, 100, new Date(2_500L));
        nodeFailure.getRequest().getUpdateSteps().get(0).setFailureReason("step failed on node");
        nodeFailure.getRequest().getUpdateSteps().get(0).setComplete(false);

        final Map<NodeIdentifier, MigrationRequestEntity> map = new LinkedHashMap<>();
        map.put(nodeId("node-1", 8080), nodeFailure);

        merger.mergeResponses(client, map, null, null);

        final MigrationUpdateStepDTO mergedStep = client.getRequest().getUpdateSteps().get(0);
        assertFalse(mergedStep.isComplete());
        assertEquals("step failed on node", mergedStep.getFailureReason());
    }

    private MigrationRequestEntity createMigrationRequest(final boolean complete, final String failureReason, final int percentComplete, final Date lastUpdated) {
        final MigrationRequestDTO request = new MigrationRequestDTO();
        request.setComplete(complete);
        request.setFailureReason(failureReason);
        request.setPercentCompleted(percentComplete);
        request.setLastUpdated(lastUpdated);

        final MigrationUpdateStepDTO step = new MigrationUpdateStepDTO();
        step.setDescription("Migrate Versioned Flow");
        step.setComplete(complete);
        request.setUpdateSteps(List.of(step));

        final MigrationRequestEntity entity = new MigrationRequestEntity();
        entity.setRequest(request);
        return entity;
    }

    private NodeIdentifier nodeId(final String hostname, final int port) {
        return new NodeIdentifier(hostname + "-" + port, hostname, port, hostname, port + 1, hostname, port + 2, port + 3, false);
    }
}
