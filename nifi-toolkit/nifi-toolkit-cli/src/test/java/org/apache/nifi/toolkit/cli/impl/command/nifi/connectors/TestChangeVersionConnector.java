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
package org.apache.nifi.toolkit.cli.impl.command.nifi.connectors;

import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ConnectorsResult;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorsEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestChangeVersionConnector {

    private static final String CONNECTOR_ID = "connector-1";
    private static final String UNREADABLE_CONNECTOR_ID = "unreadable-connector";
    private static final String BUNDLE_GROUP = "org.apache.nifi";
    private static final String BUNDLE_ARTIFACT = "nifi-test-connector-nar";
    private static final String SOURCE_VERSION = "1.0.0";
    private static final String TARGET_VERSION = "2.0.0";
    private static final String CONNECTOR_TYPE = "org.apache.nifi.connectors.TestConnector";
    private static final String CLIENT_ID = "cli-client";

    @Mock
    private NiFiClient niFiClient;
    @Mock
    private FlowClient flowClient;
    @Mock
    private ConnectorClient connectorClient;

    @Test
    public void testChangeVersionWaitsForStoppedBeforeReload() throws Exception {
        when(niFiClient.getFlowClient()).thenReturn(flowClient);
        when(niFiClient.getConnectorClient()).thenReturn(connectorClient);

        final ConnectorEntity running = connectorEntity("RUNNING", 1L);
        final ConnectorEntity stopping = connectorEntity("STOPPING", 2L);
        final ConnectorEntity stopped = connectorEntity("STOPPED", 3L);
        final ConnectorEntity updated = connectorEntity("STOPPED", 4L);

        final ConnectorsEntity connectorsEntity = new ConnectorsEntity();
        connectorsEntity.setConnectors(Set.of(running));
        when(flowClient.getConnectors()).thenReturn(connectorsEntity);
        when(connectorClient.getConnector(CONNECTOR_ID)).thenReturn(stopping, stopped, updated, updated);
        when(connectorClient.updateConnector(any())).thenReturn(updated);
        when(connectorClient.startConnector(any(ConnectorEntity.class))).thenReturn(updated);

        new ChangeVersionConnector().doExecute(niFiClient, changeVersionProperties());

        final ArgumentCaptor<ConnectorEntity> updateCaptor = ArgumentCaptor.forClass(ConnectorEntity.class);
        verify(connectorClient).stopConnector(running);
        verify(connectorClient).startConnector(any(ConnectorEntity.class));
        verify(connectorClient).updateConnector(updateCaptor.capture());
        assertEquals(3L, updateCaptor.getValue().getRevision().getVersion());
        assertEquals(TARGET_VERSION, updateCaptor.getValue().getComponent().getBundle().getVersion());
    }

    @Test
    public void testChangeVersionAbortsWhenAnyConnectorIsUnreadable() throws Exception {
        stubClients();

        final ConnectorEntity readable = connectorEntity(CONNECTOR_ID, "STOPPED", 1L, SOURCE_VERSION);
        stubListedConnectors(readable, unreadableConnector());

        final CommandException exception = assertThrows(CommandException.class, () ->
                new ChangeVersionConnector().doExecute(niFiClient, changeVersionProperties()));

        assertEquals("Cannot change version because Connector " + UNREADABLE_CONNECTOR_ID + " is unreadable", exception.getMessage());
        verify(connectorClient, never()).stopConnector(any());
        verify(connectorClient, never()).updateConnector(any());
        verify(connectorClient, never()).startConnector(any(ConnectorEntity.class));
    }

    @Test
    public void testChangeVersionSkipsConnectorAlreadyOnTargetVersion() throws Exception {
        stubClients();
        stubListedConnectors(connectorEntity(CONNECTOR_ID, "STOPPED", 1L, TARGET_VERSION));

        final Properties properties = changeVersionProperties();
        properties.remove(CommandOption.EXT_BUNDLE_CURRENT_VERSION.getLongName());

        final ConnectorsResult result = new ChangeVersionConnector().doExecute(niFiClient, properties);

        assertEquals(0, result.getResult().getConnectors().size());
        verify(connectorClient, never()).stopConnector(any());
        verify(connectorClient, never()).updateConnector(any());
        verify(connectorClient, never()).startConnector(any(ConnectorEntity.class));
    }

    @Test
    public void testChangeVersionRestartsConnectorWhenUpdateFails() throws Exception {
        stubClients();

        final ConnectorEntity running = connectorEntity("RUNNING", 1L);
        final ConnectorEntity stopping = connectorEntity("STOPPING", 2L);
        final ConnectorEntity stopped = connectorEntity("STOPPED", 3L);
        stubListedConnectors(running);
        when(connectorClient.getConnector(CONNECTOR_ID)).thenReturn(stopping, stopped, stopped);
        when(connectorClient.updateConnector(any())).thenThrow(new NiFiClientException("update failed"));
        when(connectorClient.startConnector(any(ConnectorEntity.class))).thenThrow(new NiFiClientException("restart failed"));

        final NiFiClientException exception = assertThrows(NiFiClientException.class, () ->
                new ChangeVersionConnector().doExecute(niFiClient, changeVersionProperties()));

        assertEquals("update failed", exception.getMessage());
        assertEquals(1, exception.getSuppressed().length);
        assertInstanceOf(NiFiClientException.class, exception.getSuppressed()[0]);
        assertEquals("restart failed", exception.getSuppressed()[0].getMessage());
        verify(connectorClient).stopConnector(running);
        verify(connectorClient).startConnector(any(ConnectorEntity.class));
    }

    private Properties changeVersionProperties() {
        final Properties properties = new Properties();
        properties.setProperty(CommandOption.EXT_BUNDLE_GROUP.getLongName(), BUNDLE_GROUP);
        properties.setProperty(CommandOption.EXT_BUNDLE_ARTIFACT.getLongName(), BUNDLE_ARTIFACT);
        properties.setProperty(CommandOption.EXT_BUNDLE_VERSION.getLongName(), TARGET_VERSION);
        properties.setProperty(CommandOption.EXT_QUALIFIED_NAME.getLongName(), CONNECTOR_TYPE);
        properties.setProperty(CommandOption.EXT_BUNDLE_CURRENT_VERSION.getLongName(), SOURCE_VERSION);
        return properties;
    }

    private void stubClients() {
        when(niFiClient.getFlowClient()).thenReturn(flowClient);
        when(niFiClient.getConnectorClient()).thenReturn(connectorClient);
    }

    private void stubListedConnectors(final ConnectorEntity... connectors) throws NiFiClientException, IOException {
        final Set<ConnectorEntity> connectorSet = new LinkedHashSet<>();
        for (final ConnectorEntity connector : connectors) {
            connectorSet.add(connector);
        }

        final ConnectorsEntity connectorsEntity = new ConnectorsEntity();
        connectorsEntity.setConnectors(connectorSet);
        when(flowClient.getConnectors()).thenReturn(connectorsEntity);
    }

    private ConnectorEntity connectorEntity(final String state, final long revisionVersion) {
        return connectorEntity(CONNECTOR_ID, state, revisionVersion, SOURCE_VERSION);
    }

    private ConnectorEntity connectorEntity(final String connectorId, final String state, final long revisionVersion, final String bundleVersion) {
        final BundleDTO bundle = new BundleDTO(BUNDLE_GROUP, BUNDLE_ARTIFACT, bundleVersion);
        final ConnectorDTO connectorDto = new ConnectorDTO();
        connectorDto.setId(connectorId);
        connectorDto.setType(CONNECTOR_TYPE);
        connectorDto.setState(state);
        connectorDto.setBundle(bundle);

        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(revisionVersion);

        final ConnectorEntity entity = new ConnectorEntity();
        entity.setId(connectorId);
        entity.setComponent(connectorDto);
        entity.setRevision(revision);
        return entity;
    }

    private ConnectorEntity unreadableConnector() {
        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(false);

        final ConnectorEntity entity = new ConnectorEntity();
        entity.setId(UNREADABLE_CONNECTOR_ID);
        entity.setPermissions(permissions);
        return entity;
    }
}
