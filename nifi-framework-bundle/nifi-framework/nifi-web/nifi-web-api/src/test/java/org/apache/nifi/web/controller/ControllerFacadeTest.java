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
package org.apache.nifi.web.controller;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ConnectorDefinition;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.search.query.SearchQuery;
import org.apache.nifi.web.search.query.SearchQueryParser;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ControllerFacadeTest {

    private static final String SEARCH_TERM = "test-search";

    @Mock
    private SearchQueryParser searchQueryParser;

    @Mock
    private ControllerSearchService controllerSearchService;

    @Mock
    private ProcessGroup connectorProcessGroup;

    @Mock
    private SearchQuery searchQuery;

    @Mock
    private RuntimeManifestService runtimeManifestService;

    @Mock
    private RuntimeManifest runtimeManifest;

    @Mock
    private Bundle manifestBundle;

    @Mock
    private ComponentManifest componentManifest;

    private ControllerFacade controllerFacade;

    private static final String TEST_GROUP = "org.apache.nifi";
    private static final String TEST_ARTIFACT = "nifi-test-nar";
    private static final String TEST_VERSION = "2.0.0";
    private static final String TEST_CONNECTOR_TYPE = "org.apache.nifi.connectors.TestConnector";

    @BeforeEach
    public void setUp() {
        controllerFacade = new ControllerFacade();
        controllerFacade.setSearchQueryParser(searchQueryParser);
        controllerFacade.setControllerSearchService(controllerSearchService);
        controllerFacade.setRuntimeManifestService(runtimeManifestService);

        final NiFiUser user = new StandardNiFiUser.Builder().identity("test-user").build();
        SecurityContextHolder.getContext().setAuthentication(new NiFiAuthenticationToken(new NiFiUserDetails(user)));
    }

    @Test
    public void testSearchConnectorDelegatesSearchQueryParserWithConnectorProcessGroupAsRootAndActive() {
        when(searchQueryParser.parse(eq(SEARCH_TERM), any(NiFiUser.class), eq(connectorProcessGroup), eq(connectorProcessGroup))).thenReturn(searchQuery);
        when(searchQuery.getTerm()).thenReturn(SEARCH_TERM);

        final SearchResultsDTO results = controllerFacade.searchConnector(SEARCH_TERM, connectorProcessGroup);

        assertNotNull(results);
        verify(searchQueryParser).parse(eq(SEARCH_TERM), any(NiFiUser.class), eq(connectorProcessGroup), eq(connectorProcessGroup));
        verify(controllerSearchService).search(eq(searchQuery), any(SearchResultsDTO.class));
        verify(controllerSearchService).searchParameters(eq(searchQuery), any(SearchResultsDTO.class));
    }

    @Test
    public void testSearchConnectorWithEmptyTermDoesNotInvokeSearchService() {
        when(searchQueryParser.parse(eq(""), any(NiFiUser.class), eq(connectorProcessGroup), eq(connectorProcessGroup))).thenReturn(searchQuery);
        when(searchQuery.getTerm()).thenReturn("");

        final SearchResultsDTO results = controllerFacade.searchConnector("", connectorProcessGroup);

        assertNotNull(results);
        verify(searchQueryParser).parse(eq(""), any(NiFiUser.class), eq(connectorProcessGroup), eq(connectorProcessGroup));
    }

    @Test
    public void testSearchConnectorWithNullTermDoesNotInvokeSearchService() {
        when(searchQueryParser.parse(eq(null), any(NiFiUser.class), eq(connectorProcessGroup), eq(connectorProcessGroup))).thenReturn(searchQuery);
        when(searchQuery.getTerm()).thenReturn(null);

        final SearchResultsDTO results = controllerFacade.searchConnector(null, connectorProcessGroup);

        assertNotNull(results);
        verify(searchQueryParser).parse(eq(null), any(NiFiUser.class), eq(connectorProcessGroup), eq(connectorProcessGroup));
    }

    @Test
    public void testGetConnectorDefinitionReturnsDefinitionWhenFound() {
        final ConnectorDefinition expectedDefinition = new ConnectorDefinition();
        expectedDefinition.setGroup(TEST_GROUP);
        expectedDefinition.setArtifact(TEST_ARTIFACT);
        expectedDefinition.setVersion(TEST_VERSION);
        expectedDefinition.setType(TEST_CONNECTOR_TYPE);

        when(runtimeManifestService.getManifestForBundle(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION)).thenReturn(runtimeManifest);
        when(runtimeManifest.getBundles()).thenReturn(List.of(manifestBundle));
        when(manifestBundle.getComponentManifest()).thenReturn(componentManifest);
        when(componentManifest.getConnectors()).thenReturn(List.of(expectedDefinition));

        final ConnectorDefinition result = controllerFacade.getConnectorDefinition(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION, TEST_CONNECTOR_TYPE);

        assertNotNull(result);
        assertEquals(TEST_CONNECTOR_TYPE, result.getType());
    }

    @Test
    public void testGetConnectorDefinitionReturnsNullWhenConnectorsListIsNull() {
        when(runtimeManifestService.getManifestForBundle(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION)).thenReturn(runtimeManifest);
        when(runtimeManifest.getBundles()).thenReturn(List.of(manifestBundle));
        when(manifestBundle.getComponentManifest()).thenReturn(componentManifest);
        when(componentManifest.getConnectors()).thenReturn(null);

        final ConnectorDefinition result = controllerFacade.getConnectorDefinition(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION, TEST_CONNECTOR_TYPE);

        assertNull(result);
    }

    @Test
    public void testGetConnectorDefinitionReturnsNullWhenConnectorTypeNotFound() {
        final ConnectorDefinition otherDefinition = new ConnectorDefinition();
        otherDefinition.setType("org.apache.nifi.connectors.OtherConnector");

        when(runtimeManifestService.getManifestForBundle(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION)).thenReturn(runtimeManifest);
        when(runtimeManifest.getBundles()).thenReturn(List.of(manifestBundle));
        when(manifestBundle.getComponentManifest()).thenReturn(componentManifest);
        when(componentManifest.getConnectors()).thenReturn(List.of(otherDefinition));

        final ConnectorDefinition result = controllerFacade.getConnectorDefinition(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION, TEST_CONNECTOR_TYPE);

        assertNull(result);
    }

    @Test
    public void testGetConnectorDefinitionReturnsNullWhenConnectorsListIsEmpty() {
        when(runtimeManifestService.getManifestForBundle(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION)).thenReturn(runtimeManifest);
        when(runtimeManifest.getBundles()).thenReturn(List.of(manifestBundle));
        when(manifestBundle.getComponentManifest()).thenReturn(componentManifest);
        when(componentManifest.getConnectors()).thenReturn(Collections.emptyList());

        final ConnectorDefinition result = controllerFacade.getConnectorDefinition(TEST_GROUP, TEST_ARTIFACT, TEST_VERSION, TEST_CONNECTOR_TYPE);

        assertNull(result);
    }
}

