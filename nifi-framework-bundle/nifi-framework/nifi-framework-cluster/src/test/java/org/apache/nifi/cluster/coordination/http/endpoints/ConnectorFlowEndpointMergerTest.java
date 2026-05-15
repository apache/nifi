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

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorFlowEndpointMergerTest {

    @Test
    public void testCanHandle() {
        final ConnectorFlowEndpointMerger merger = new ConnectorFlowEndpointMerger();
        final String connectorId = "12345678-1234-1234-1234-123456789012";
        final String processGroupId = "abcdef01-2345-6789-abcd-ef0123456789";
        final String connectorFlowUri = "/nifi-api/connectors/" + connectorId + "/flow/process-groups/" + processGroupId;

        // Test valid URIs
        assertTrue(merger.canHandle(URI.create(connectorFlowUri), "GET"));
        assertTrue(merger.canHandle(URI.create(connectorFlowUri + "?uiOnly=true"), "GET"));

        // Test invalid URIs
        assertFalse(merger.canHandle(URI.create(connectorFlowUri), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + connectorId + "/flow"), "GET"));
        assertFalse(merger.canHandle(URI.create(connectorFlowUri + "/controller-services"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + processGroupId), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/not-a-uuid/flow/process-groups/" + processGroupId), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + connectorId + "/flow/process-groups/not-a-uuid"), "GET"));
    }
}
