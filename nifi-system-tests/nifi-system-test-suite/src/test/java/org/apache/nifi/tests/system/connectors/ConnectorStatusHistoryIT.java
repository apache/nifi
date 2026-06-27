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

package org.apache.nifi.tests.system.connectors;

import jakarta.ws.rs.WebApplicationException;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * System tests that verify the Status History REST endpoints for components inside a Connector's managed flow.
 */
public class ConnectorStatusHistoryIT extends NiFiSystemIT {

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        // Capture component status snapshots once per second so the test does not have to wait the default one minute
        // for the first snapshot to be recorded.
        return Map.of("nifi.components.status.snapshot.frequency", "1 sec");
    }

    /**
     * Status history for components inside a Connector's managed flow must be retrievable whether the Connector is
     * RUNNING or in Troubleshooting mode. The standard component endpoints reject requests with a 409 when the
     * Connector is not in Troubleshooting; the Connector status history endpoints must not be subject to that gate.
     */
    @Test
    public void testStatusHistoryAvailableRegardlessOfTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        final String managedGroupId = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getManagedProcessGroupId();
        assertNotNull(managedGroupId);

        final String processorId = findFirstProcessorId(connectorId);
        final String connectionId = findFirstConnectionId(connectorId);
        assertNotNull(processorId);
        assertNotNull(connectionId);

        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);

        // While the Connector is RUNNING (not in Troubleshooting), the standard component endpoint is gated with a 409,
        // but the Connector status history endpoints must still return data.
        assertConflict("GET processor outside Troubleshooting", () -> getNifiClient().getProcessorClient().getProcessor(processorId));

        assertStatusHistoryAvailable(connectorId, processorId, connectionId, managedGroupId);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        // The same component status history must remain available once the Connector enters Troubleshooting.
        assertStatusHistoryAvailable(connectorId, processorId, connectionId, managedGroupId);
    }

    /**
     * Unknown component ids, and component ids that exist elsewhere in NiFi but not inside the target Connector's
     * managed flow, must produce a 404 Not Found for each component type. The latter verifies that the endpoints are
     * scoped to the Connector rather than resolving any component id from the repository.
     */
    @Test
    public void testStatusHistoryNotFoundForUnknownComponent() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();
        final String unknownId = UUID.randomUUID().toString();

        assertNotFound("processor status history", () -> connectorClient.getProcessorStatusHistory(connectorId, unknownId));
        assertNotFound("connection status history", () -> connectorClient.getConnectionStatusHistory(connectorId, unknownId));
        assertNotFound("process group status history", () -> connectorClient.getProcessGroupStatusHistory(connectorId, unknownId));
        assertNotFound("remote process group status history", () -> connectorClient.getRemoteProcessGroupStatusHistory(connectorId, unknownId));

        // A Processor that exists in the root flow, outside any Connector, must not be resolvable through the Connector
        // status history endpoint.
        final String rootProcessorId = getClientUtil().createProcessor("CountEvents").getId();
        assertNotFound("processor status history for component outside the connector",
                () -> connectorClient.getProcessorStatusHistory(connectorId, rootProcessorId));
    }

    private void assertStatusHistoryAvailable(final String connectorId, final String processorId, final String connectionId, final String processGroupId)
            throws InterruptedException {

        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();

        final StatusHistoryEntity processorHistory = waitForStatusHistory(() -> connectorClient.getProcessorStatusHistory(connectorId, processorId));
        assertComponentHistory(processorHistory, processorId);

        final StatusHistoryEntity connectionHistory = waitForStatusHistory(() -> connectorClient.getConnectionStatusHistory(connectorId, connectionId));
        assertComponentHistory(connectionHistory, connectionId);

        final StatusHistoryEntity processGroupHistory = waitForStatusHistory(() -> connectorClient.getProcessGroupStatusHistory(connectorId, processGroupId));
        assertComponentHistory(processGroupHistory, processGroupId);
    }

    private void assertComponentHistory(final StatusHistoryEntity entity, final String componentId) {
        assertNotNull(entity);
        assertTrue(entity.getCanRead());
        assertNotNull(entity.getStatusHistory());
        assertFalse(entity.getStatusHistory().getAggregateSnapshots().isEmpty());
        assertEquals(componentId, entity.getStatusHistory().getComponentDetails().get(StatusHistoryRepository.COMPONENT_DETAIL_ID));
    }

    private StatusHistoryEntity waitForStatusHistory(final Callable<StatusHistoryEntity> supplier) throws InterruptedException {
        final AtomicReference<StatusHistoryEntity> holder = new AtomicReference<>();
        waitFor(() -> {
            final StatusHistoryEntity entity = supplier.call();
            if (entity != null && entity.getStatusHistory() != null && !entity.getStatusHistory().getAggregateSnapshots().isEmpty()) {
                holder.set(entity);
                return true;
            }

            return false;
        });
        return holder.get();
    }

    private String findFirstProcessorId(final String connectorId) throws NiFiClientException, IOException {
        final List<ProcessorEntity> processors = new ArrayList<>();
        collectProcessors(connectorId, null, processors);
        return processors.isEmpty() ? null : processors.getFirst().getId();
    }

    private void collectProcessors(final String connectorId, final String groupId, final List<ProcessorEntity> collected) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = (groupId == null)
                ? getNifiClient().getConnectorClient().getFlow(connectorId)
                : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        collected.addAll(flow.getProcessors());

        for (final ProcessGroupEntity child : flow.getProcessGroups()) {
            collectProcessors(connectorId, child.getId(), collected);
        }
    }

    private String findFirstConnectionId(final String connectorId) throws NiFiClientException, IOException {
        return collectFirstConnectionId(connectorId, null);
    }

    private String collectFirstConnectionId(final String connectorId, final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = (groupId == null)
                ? getNifiClient().getConnectorClient().getFlow(connectorId)
                : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        for (final ConnectionEntity connection : flow.getConnections()) {
            return connection.getId();
        }

        for (final ProcessGroupEntity child : flow.getProcessGroups()) {
            final String childConnectionId = collectFirstConnectionId(connectorId, child.getId());
            if (childConnectionId != null) {
                return childConnectionId;
            }
        }

        return null;
    }

    private void assertConnectorState(final String connectorId, final ConnectorState expected) throws NiFiClientException, IOException {
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(expected.name(), entity.getComponent().getState());
    }

    private void assertNotFound(final String description, final Callable<?> call) {
        assertExpectedStatus(description, call, 404);
    }

    private void assertConflict(final String description, final Callable<?> call) {
        assertExpectedStatus(description, call, 409);
    }

    private void assertExpectedStatus(final String description, final Callable<?> call, final int expectedStatus) {
        try {
            call.call();
            fail("Expected HTTP " + expectedStatus + " for " + description + " but request succeeded");
        } catch (final NiFiClientException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof final WebApplicationException wae) {
                assertEquals(expectedStatus, wae.getResponse().getStatus(), "Unexpected status for " + description);
            } else {
                fail("Expected WebApplicationException " + expectedStatus + " for " + description + ", got: " + cause);
            }
        } catch (final Exception e) {
            fail("Unexpected exception while invoking " + description + ": " + e.getMessage());
        }
    }
}
