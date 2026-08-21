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

public class ControllerServicesEndpointMergerTest {

    private static final String CONNECTOR_ID = "12345678-1234-1234-1234-123456789012";
    private static final String PROCESS_GROUP_ID = "abcdef01-2345-6789-abcd-ef0123456789";

    @Test
    public void testCanHandleControllerControllerServicesUri() {
        final ControllerServicesEndpointMerger merger = new ControllerServicesEndpointMerger();

        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/controller/controller-services"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/flow/controller/controller-services"), "POST"));
    }

    @Test
    public void testCanHandleProcessGroupControllerServicesUri() {
        final ControllerServicesEndpointMerger merger = new ControllerServicesEndpointMerger();

        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/process-groups/root/controller-services"), "GET"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/flow/process-groups/" + PROCESS_GROUP_ID + "/controller-services"), "GET"));
    }

    @Test
    public void testCanHandleConnectorProcessGroupControllerServicesUri() {
        final ControllerServicesEndpointMerger merger = new ControllerServicesEndpointMerger();
        final String connectorProcessGroupControllerServicesUri =
                "/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/" + PROCESS_GROUP_ID + "/controller-services";

        // Test valid URIs
        assertTrue(merger.canHandle(URI.create(connectorProcessGroupControllerServicesUri), "GET"));
        assertTrue(merger.canHandle(URI.create(connectorProcessGroupControllerServicesUri + "?includeDescendantGroups=true"), "GET"));

        // Test invalid URIs
        assertFalse(merger.canHandle(URI.create(connectorProcessGroupControllerServicesUri), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/" + PROCESS_GROUP_ID), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/not-a-uuid/flow/process-groups/" + PROCESS_GROUP_ID + "/controller-services"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/not-a-uuid/controller-services"), "GET"));
    }
}
