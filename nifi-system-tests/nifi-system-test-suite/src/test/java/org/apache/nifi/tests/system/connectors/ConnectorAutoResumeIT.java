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

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * System tests verifying that Connectors and the components inside their managed flows are not
 * auto-resumed after a restart when the {@code nifi.flowcontroller.autoResumeState} property is
 * set to {@code false}.
 */
public class ConnectorAutoResumeIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorAutoResumeIT.class);

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testConnectorNotResumedWhenAutoResumeDisabled() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().startConnector(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);

        restartWithAutoResumeStateDisabled();

        final ConnectorEntity connectorAfterRestart = getNifiClient().getConnectorClient().getConnector(connectorId);
        final String stateAfterRestart = connectorAfterRestart.getComponent().getState();

        assertEquals(ConnectorState.STOPPED.name(), stateAfterRestart);
    }

    /**
     * Verifies that when a Connector is in Troubleshooting mode with processors running and controller services
     * enabled, restarting NiFi with {@code nifi.flowcontroller.autoResumeState=false} leaves the Connector in
     * Troubleshooting mode but does not resume any of the processors or controller services inside the managed flow.
     */
    @Test
    public void testTroubleshootingConnectorNotResumedWhenAutoResumeDisabled() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().startConnector(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);

        // Entering Troubleshooting from RUNNING leaves the managed flow's processors running and its controller services
        // enabled, so the post-restart assertions have running components to verify against.
        getClientUtil().enterTroubleshooting(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        restartWithAutoResumeStateDisabled();

        // The Troubleshooting state itself is preserved across restarts; autoResumeState does not transition the
        // Connector out of Troubleshooting mode.
        final ConnectorEntity connectorAfterRestart = getNifiClient().getConnectorClient().getConnector(connectorId);
        final String connectorStateAfterRestart = connectorAfterRestart.getComponent().getState();
        assertEquals(ConnectorState.TROUBLESHOOTING.name(), connectorStateAfterRestart);

        final List<ProcessorEntity> processorsAfterRestart = new ArrayList<>();
        collectProcessors(connectorId, null, processorsAfterRestart);
        assertFalse(processorsAfterRestart.isEmpty());

        for (final ProcessorEntity processor : processorsAfterRestart) {
            final String processorState = processor.getComponent().getState();
            assertNotEquals(ScheduledState.RUNNING.name(), processorState);
        }

        final String managedGroupId = connectorAfterRestart.getComponent().getManagedProcessGroupId();
        final List<String> controllerServiceIds = collectAllControllerServiceIds(managedGroupId);
        assertFalse(controllerServiceIds.isEmpty());

        for (final String serviceId : controllerServiceIds) {
            final ControllerServiceEntity serviceEntity = getNifiClient().getControllerServicesClient().getControllerService(serviceId);
            final String serviceState = serviceEntity.getComponent().getState();
            assertNotEquals("ENABLED", serviceState);
        }
    }

    private void restartWithAutoResumeStateDisabled() throws IOException {
        getNiFiInstance().stop();
        getNiFiInstance().setProperty(NiFiProperties.AUTO_RESUME_STATE, "false");
        getNiFiInstance().start();

        setupClient();

        if (getNiFiInstance().isClustered()) {
            waitForAllNodesConnected();
        }

        logger.info("NiFi restarted with autoResumeState=false");
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

    private List<String> collectAllControllerServiceIds(final String groupId) throws NiFiClientException, IOException {
        final List<String> serviceIds = new ArrayList<>();
        for (final ControllerServiceEntity entity : getNifiClient().getFlowClient().getControllerServices(groupId).getControllerServices()) {
            if (entity.getComponent() != null) {
                serviceIds.add(entity.getId());
            }
        }

        final ProcessGroupFlowEntity groupFlow = getNifiClient().getFlowClient().getProcessGroup(groupId);
        for (final ProcessGroupEntity child : groupFlow.getProcessGroupFlow().getFlow().getProcessGroups()) {
            serviceIds.addAll(collectAllControllerServiceIds(child.getId()));
        }

        return serviceIds;
    }
}
