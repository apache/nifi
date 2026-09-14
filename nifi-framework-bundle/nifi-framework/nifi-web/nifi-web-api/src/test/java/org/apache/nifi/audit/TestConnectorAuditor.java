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
package org.apache.nifi.audit;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestConnectorAuditor {

    private static final String CONNECTOR_ID = "connector-1";
    private static final String COMPONENT_TYPE = "com.example.TestConnector";
    private static final String OLD_NAME = "Old Name";
    private static final String NEW_NAME = "New Name";
    private static final BundleCoordinate OLD_COORDINATE = new BundleCoordinate("com.example", "test-connector", "1.0.0");
    private static final BundleCoordinate NEW_COORDINATE = new BundleCoordinate("com.example", "test-connector", "2.0.0");

    private ConnectorAuditor auditor;
    private AuditService auditService;
    private ConnectorDAO connectorDAO;
    private ProceedingJoinPoint proceedingJoinPoint;

    @BeforeEach
    void setUp() {
        auditService = mock(AuditService.class);
        connectorDAO = mock(ConnectorDAO.class);
        proceedingJoinPoint = mock(ProceedingJoinPoint.class);

        auditor = new ConnectorAuditor();
        auditor.setAuditService(auditService);

        final Authentication authentication = new UsernamePasswordAuthenticationToken("user", "credentials");
        final SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(authentication);
        SecurityContextHolder.setContext(securityContext);
    }

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testUpdateConnectorAuditsNameAndExtensionVersionChanges() throws Throwable {
        final ConnectorNode oldConnector = createConnector(OLD_NAME, OLD_COORDINATE);
        final ConnectorNode newConnector = createConnector(NEW_NAME, NEW_COORDINATE);
        when(connectorDAO.getConnector(CONNECTOR_ID, ConnectorSyncMode.LOCAL_ONLY)).thenReturn(oldConnector, newConnector);

        final ConnectorDTO connectorDTO = new ConnectorDTO();
        connectorDTO.setId(CONNECTOR_ID);
        connectorDTO.setName(NEW_NAME);
        connectorDTO.setBundle(new BundleDTO(NEW_COORDINATE.getGroup(), NEW_COORDINATE.getId(), NEW_COORDINATE.getVersion()));

        auditor.updateConnectorAdvice(proceedingJoinPoint, connectorDTO, connectorDAO);

        verify(proceedingJoinPoint).proceed();

        final Map<String, FlowChangeConfigureDetails> detailsByProperty = captureConfigureDetails();
        assertEquals(2, detailsByProperty.size());

        final FlowChangeConfigureDetails nameDetails = detailsByProperty.get("Name");
        assertNotNull(nameDetails);
        assertEquals(OLD_NAME, nameDetails.getPreviousValue());
        assertEquals(NEW_NAME, nameDetails.getValue());

        final FlowChangeConfigureDetails versionDetails = detailsByProperty.get("Extension Version");
        assertNotNull(versionDetails);
        assertEquals("com.example.TestConnector 1.0.0 from com.example - test-connector", versionDetails.getPreviousValue());
        assertEquals("com.example.TestConnector 2.0.0 from com.example - test-connector", versionDetails.getValue());
    }

    private ConnectorNode createConnector(final String name, final BundleCoordinate coordinate) {
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn(CONNECTOR_ID);
        when(connector.getName()).thenReturn(name);
        when(connector.getComponentType()).thenReturn(COMPONENT_TYPE);
        when(connector.getBundleCoordinate()).thenReturn(coordinate);
        return connector;
    }

    @SuppressWarnings("unchecked")
    private Map<String, FlowChangeConfigureDetails> captureConfigureDetails() {
        final ArgumentCaptor<Collection<Action>> actionsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(auditService).addActions(actionsCaptor.capture());

        final Map<String, FlowChangeConfigureDetails> detailsByProperty = new HashMap<>();
        final Collection<Action> actions = actionsCaptor.getValue();
        for (final Action action : actions) {
            assertEquals(Operation.Configure, action.getOperation());
            assertEquals(CONNECTOR_ID, action.getSourceId());
            assertEquals(NEW_NAME, action.getSourceName());
            assertEquals(Component.Connector, action.getSourceType());

            final ActionDetails actionDetails = action.getActionDetails();
            assertNotNull(actionDetails);
            final FlowChangeConfigureDetails configureDetails = (FlowChangeConfigureDetails) actionDetails;
            detailsByProperty.put(configureDetails.getName(), configureDetails);
        }

        return detailsByProperty;
    }
}
