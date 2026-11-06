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
package org.apache.nifi.cluster.coordination.http;

import jakarta.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.entity.ConnectorTypesEntity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardHttpResponseMapperTest {
    private static final String CONNECTOR_ID = "abcdef00-abcd-abcd-abcd-abcdef000000";
    private static final String REQUEST_ID = "00000000-0000-0000-0000-000000000001";
    private static final URI CONNECTOR_TYPES_URI = URI.create("/nifi-api/flow/connector-types");
    private static final String CONNECTOR_TYPE = "org.apache.nifi.connectors.ExampleConnector";

    private final StandardHttpResponseMapper mapper = new StandardHttpResponseMapper(NiFiProperties.createBasicNiFiProperties((String) null));

    @Test
    void testConnectorPurgeRequestsInterpreted() {
        assertTrue(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests"), "POST"));
        assertTrue(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests/" + REQUEST_ID), "GET"));
        assertTrue(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests/" + REQUEST_ID), "DELETE"));
    }

    @Test
    void testUnsupportedConnectorPurgeRequestsNotInterpreted() {
        assertFalse(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests"), "GET"));
    }

    @Test
    void testConnectorTypesMergedAcrossNodes() {
        // Each node has a different set of Connector versions installed. Only a version installed on every node can be offered to the client.
        final Set<NodeResponse> nodeResponses = Set.of(
            createConnectorTypesResponse(1, "1.0.0", "2.0.0", "3.0.0"),
            createConnectorTypesResponse(2, "1.0.0", "2.0.0"),
            createConnectorTypesResponse(3, "2.0.0", "3.0.0"));

        final NodeResponse mergedResponse = mapper.mapResponses(CONNECTOR_TYPES_URI, "GET", nodeResponses, true);

        final ConnectorTypesEntity mergedEntity = (ConnectorTypesEntity) mergedResponse.getUpdatedEntity();
        assertEquals(Set.of(createConnectorType("2.0.0")), mergedEntity.getConnectorTypes());
    }

    private NodeResponse createConnectorTypesResponse(final int nodeNumber, final String... versions) {
        final Set<DocumentedTypeDTO> connectorTypes = new HashSet<>();
        for (final String version : versions) {
            connectorTypes.add(createConnectorType(version));
        }

        final ConnectorTypesEntity entity = new ConnectorTypesEntity();
        entity.setConnectorTypes(connectorTypes);

        final Response response = mock(Response.class);
        when(response.getStatus()).thenReturn(200);
        when(response.readEntity(ConnectorTypesEntity.class)).thenReturn(entity);

        final NodeIdentifier nodeId = new NodeIdentifier("node-" + nodeNumber, "localhost", 8080 + nodeNumber, "localhost", 9080 + nodeNumber, null, null, null, false);
        return new NodeResponse(nodeId, "GET", CONNECTOR_TYPES_URI, response, 0L, REQUEST_ID);
    }

    private DocumentedTypeDTO createConnectorType(final String version) {
        final DocumentedTypeDTO connectorType = new DocumentedTypeDTO();
        connectorType.setType(CONNECTOR_TYPE);
        connectorType.setBundle(new BundleDTO("org.apache.nifi", "nifi-example-connector-nar", version));
        return connectorType;
    }
}
