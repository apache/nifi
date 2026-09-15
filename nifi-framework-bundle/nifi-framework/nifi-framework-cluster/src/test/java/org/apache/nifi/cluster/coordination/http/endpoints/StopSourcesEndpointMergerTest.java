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
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StopSourcesEndpointMergerTest {

    private static final String GROUP_ID = "12345678-1234-1234-1234-123456789012";

    @Test
    public void testCanHandle() {
        final StopSourcesEndpointMerger merger = new StopSourcesEndpointMerger();

        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + GROUP_ID + "/sources"), "PUT"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/process-groups/root/sources"), "PUT"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + GROUP_ID + "/sources"), "put"));

        assertFalse(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + GROUP_ID + "/sources"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + GROUP_ID), "PUT"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + GROUP_ID), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/process-groups/" + GROUP_ID + "/sources"), "PUT"));
    }

    @Test
    public void testMergeResponsesUnionsComponentsAndKeepsClientRevision() {
        final StopSourcesEndpointMerger merger = new StopSourcesEndpointMerger();

        final RevisionDTO clientRevision = revision(1L, "client");
        final RevisionDTO nodeRevision = revision(2L, "node-2");
        final RevisionDTO extraRevision = revision(3L, "node-2-extra");

        final ScheduleComponentsEntity clientEntity = new ScheduleComponentsEntity();
        clientEntity.setId(GROUP_ID);
        clientEntity.setState("STOPPED");
        clientEntity.setComponents(new HashMap<>(Map.of("source-1", clientRevision)));

        final NodeIdentifier node1 = new NodeIdentifier("node1", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final NodeIdentifier node2 = new NodeIdentifier("node2", "localhost", 8090, "localhost", 8091, "localhost", 8092, 8093, false);

        final ScheduleComponentsEntity node1Entity = new ScheduleComponentsEntity();
        node1Entity.setId(GROUP_ID);
        node1Entity.setState("STOPPED");
        node1Entity.setComponents(Map.of("source-1", clientRevision));

        final ScheduleComponentsEntity node2Entity = new ScheduleComponentsEntity();
        node2Entity.setId(GROUP_ID);
        node2Entity.setState("STOPPED");
        node2Entity.setComponents(Map.of("source-1", nodeRevision, "source-2", extraRevision));

        merger.mergeResponses(clientEntity, Map.of(node1, node1Entity, node2, node2Entity), null, null);

        assertEquals(GROUP_ID, clientEntity.getId());
        assertEquals("STOPPED", clientEntity.getState());
        assertEquals(2, clientEntity.getComponents().size());
        assertSame(clientRevision, clientEntity.getComponents().get("source-1"));
        assertSame(extraRevision, clientEntity.getComponents().get("source-2"));
    }

    private static RevisionDTO revision(final long version, final String clientId) {
        final RevisionDTO revision = new RevisionDTO();
        revision.setVersion(version);
        revision.setClientId(clientId);
        return revision;
    }
}
