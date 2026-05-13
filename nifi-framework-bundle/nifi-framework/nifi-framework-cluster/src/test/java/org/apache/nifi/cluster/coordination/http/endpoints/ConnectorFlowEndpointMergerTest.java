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

    private static final String CONNECTOR_ID = "12345678-1234-1234-1234-123456789012";
    private static final String PROCESS_GROUP_ID = "abcdef01-2345-6789-abcd-ef0123456789";

    private final ConnectorFlowEndpointMerger merger = new ConnectorFlowEndpointMerger();

    @Test
    public void testCanHandleConnectorProcessGroupFlowUri() {
        final URI uri = URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/" + PROCESS_GROUP_ID);
        assertTrue(merger.canHandle(uri, "GET"));
    }

    @Test
    public void testCanHandleIgnoresQueryParameters() {
        final URI uri = URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/" + PROCESS_GROUP_ID + "?uiOnly=true");
        assertTrue(merger.canHandle(uri, "GET"));
    }

    @Test
    public void testCannotHandleNonGetMethods() {
        final URI uri = URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/" + PROCESS_GROUP_ID);
        assertFalse(merger.canHandle(uri, "POST"));
        assertFalse(merger.canHandle(uri, "PUT"));
        assertFalse(merger.canHandle(uri, "DELETE"));
    }

    @Test
    public void testCannotHandleConnectorFlowWithoutProcessGroup() {
        final URI uri = URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow");
        assertFalse(merger.canHandle(uri, "GET"));
    }

    @Test
    public void testCannotHandleControllerServicesSubPath() {
        final URI uri = URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/" + PROCESS_GROUP_ID + "/controller-services");
        assertFalse(merger.canHandle(uri, "GET"));
    }

    @Test
    public void testCannotHandleStandardFlowUri() {
        final URI uri = URI.create("/nifi-api/flow/process-groups/" + PROCESS_GROUP_ID);
        assertFalse(merger.canHandle(uri, "GET"));
    }

    @Test
    public void testCannotHandleInvalidConnectorId() {
        final URI uri = URI.create("/nifi-api/connectors/not-a-uuid/flow/process-groups/" + PROCESS_GROUP_ID);
        assertFalse(merger.canHandle(uri, "GET"));
    }

    @Test
    public void testCannotHandleInvalidProcessGroupId() {
        final URI uri = URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/flow/process-groups/not-a-uuid");
        assertFalse(merger.canHandle(uri, "GET"));
    }
}
