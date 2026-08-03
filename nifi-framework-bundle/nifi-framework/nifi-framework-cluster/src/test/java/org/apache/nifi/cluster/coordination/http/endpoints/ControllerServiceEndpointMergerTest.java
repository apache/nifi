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

public class ControllerServiceEndpointMergerTest {

    private static final String CONNECTOR_ID = "12345678-1234-1234-1234-123456789012";
    private static final String CONTROLLER_SERVICE_ID = "abcdef01-2345-6789-abcd-ef0123456789";

    @Test
    public void testCanHandleControllerServiceUri() {
        final ControllerServiceEndpointMerger merger = new ControllerServiceEndpointMerger();
        final String controllerServiceUri = "/nifi-api/controller-services/" + CONTROLLER_SERVICE_ID;

        assertTrue(merger.canHandle(URI.create(controllerServiceUri), "GET"));
        assertTrue(merger.canHandle(URI.create(controllerServiceUri), "PUT"));
        assertFalse(merger.canHandle(URI.create(controllerServiceUri), "DELETE"));
    }

    @Test
    public void testCanHandleConnectorControllerServiceUri() {
        final ControllerServiceEndpointMerger merger = new ControllerServiceEndpointMerger();
        final String connectorControllerServiceUri = "/nifi-api/connectors/" + CONNECTOR_ID + "/controller-services/" + CONTROLLER_SERVICE_ID;

        // Test valid URIs
        assertTrue(merger.canHandle(URI.create(connectorControllerServiceUri), "GET"));
        assertTrue(merger.canHandle(URI.create(connectorControllerServiceUri + "?uiOnly=true"), "GET"));

        // A connector-managed Controller Service is only ever retrieved via GET; PUT is not a supported operation on this path.
        assertFalse(merger.canHandle(URI.create(connectorControllerServiceUri), "PUT"));

        // Test invalid URIs
        assertFalse(merger.canHandle(URI.create(connectorControllerServiceUri + "/state"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/controller-services"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/not-a-uuid/controller-services/" + CONTROLLER_SERVICE_ID), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/controller-services/not-a-uuid"), "GET"));
    }
}
