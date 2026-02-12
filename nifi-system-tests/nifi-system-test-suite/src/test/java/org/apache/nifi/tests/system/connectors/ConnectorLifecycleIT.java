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

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.StatelessGroupScheduledState;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that verifies the connector start/stop lifecycle correctly starts and stops
 * all component types: processors, ports, and stateless groups.
 */
public class ConnectorLifecycleIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorLifecycleIT.class);


    @Test
    public void testStartStopStartsAndStopsAllComponents() throws NiFiClientException, IOException, InterruptedException {
        logger.info("Creating ComponentLifecycleConnector");
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        assertNotNull(connector);
        final String connectorId = connector.getId();

        logger.info("Applying connector configuration");
        getClientUtil().applyConnectorUpdate(connector);

        logger.info("Waiting for connector to be valid");
        getClientUtil().waitForValidConnector(connectorId);

        logger.info("Starting connector {}", connectorId);
        getClientUtil().startConnector(connectorId);

        // Ensure that there are no Parameter Contexts defined. When we start a flow that has a Stateless Group,
        // we synchronize the Process Group with the Versioned External Flow, and we want to ensure that this does
        // not register a Parameter Context
        final ParameterContextsEntity contextsEntity = getNifiClient().getParamContextClient().getParamContexts();
        final Set<ParameterContextEntity> parameterContexts = contextsEntity.getParameterContexts();
        assertNotNull(parameterContexts);
        assertEquals(Collections.emptySet(), parameterContexts);

        logger.info("Verifying flow has components after start");
        verifyFlowHasComponents(connectorId);

        logger.info("Verifying all processors are running");
        waitForAllProcessorsRunning(connectorId);

        logger.info("Verifying all ports are running");
        waitForAllPortsRunning(connectorId);

        logger.info("Verifying stateless group is running");
        waitForStatelessGroupRunning(connectorId);

        logger.info("Stopping connector {}", connectorId);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        logger.info("Verifying all processors are stopped");
        waitForAllProcessorsStopped(connectorId);

        logger.info("Verifying all ports are stopped");
        waitForAllPortsStopped(connectorId);

        logger.info("Verifying stateless group is stopped");
        waitForStatelessGroupStopped(connectorId);

        logger.info("testStartStopStartsAndStopsAllComponents completed successfully");
    }

    private void verifyFlowHasComponents(final String connectorId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getConnectorClient().getFlow(connectorId);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();

        boolean hasProcessors = !flowDto.getProcessors().isEmpty();
        boolean hasChildGroups = !flowDto.getProcessGroups().isEmpty();

        logger.info("Root group has {} processors and {} child groups",
            flowDto.getProcessors().size(), flowDto.getProcessGroups().size());

        assertTrue(hasProcessors || hasChildGroups, "Flow should have processors or child groups");
    }

    private void waitForAllProcessorsRunning(final String connectorId) throws InterruptedException {
        waitFor(() -> allProcessorsInState(connectorId, null, ScheduledState.RUNNING.name()));
    }

    private void waitForAllProcessorsStopped(final String connectorId) throws InterruptedException {
        waitFor(() -> allProcessorsInState(connectorId, null, ScheduledState.STOPPED.name()));
    }

    private boolean allProcessorsInState(final String connectorId, final String groupId, final String expectedState) throws NiFiClientException, IOException {
        final FlowDTO flowDto = getFlow(connectorId, groupId);

        for (final ProcessorEntity processorEntity : flowDto.getProcessors()) {
            final ProcessorDTO processorDto = processorEntity.getComponent();
            final String state = processorDto.getState();
            if (!expectedState.equals(state) && !ScheduledState.DISABLED.name().equals(state)) {
                logger.debug("Processor {} ({}) in group {} is in state {} (expected {})",
                    processorDto.getName(), processorEntity.getId(), groupId, state, expectedState);
                return false;
            }
        }

        for (final ProcessGroupEntity childGroupEntity : flowDto.getProcessGroups()) {
            final ProcessGroupDTO childGroupDto = childGroupEntity.getComponent();
            if (!"STATELESS".equals(childGroupDto.getExecutionEngine())) {
                if (!allProcessorsInState(connectorId, childGroupEntity.getId(), expectedState)) {
                    return false;
                }
            }
        }

        return true;
    }

    private void waitForAllPortsRunning(final String connectorId) throws InterruptedException {
        waitFor(() -> allPortsInState(connectorId, null, ScheduledState.RUNNING.name()));
    }

    private void waitForAllPortsStopped(final String connectorId) throws InterruptedException {
        waitFor(() -> allPortsInState(connectorId, null, ScheduledState.STOPPED.name()));
    }

    private boolean allPortsInState(final String connectorId, final String groupId, final String expectedState) throws NiFiClientException, IOException {
        final FlowDTO flowDto = getFlow(connectorId, groupId);

        for (final PortEntity portEntity : flowDto.getInputPorts()) {
            final PortDTO portDto = portEntity.getComponent();
            final String state = portDto.getState();
            if (!expectedState.equals(state) && !ScheduledState.DISABLED.name().equals(state)) {
                logger.debug("Input port {} is in state {} (expected {})", portDto.getName(), state, expectedState);
                return false;
            }
        }

        for (final PortEntity portEntity : flowDto.getOutputPorts()) {
            final PortDTO portDto = portEntity.getComponent();
            final String state = portDto.getState();
            if (!expectedState.equals(state) && !ScheduledState.DISABLED.name().equals(state)) {
                logger.debug("Output port {} is in state {} (expected {})", portDto.getName(), state, expectedState);
                return false;
            }
        }

        for (final ProcessGroupEntity childGroupEntity : flowDto.getProcessGroups()) {
            final ProcessGroupDTO childGroupDto = childGroupEntity.getComponent();
            if (!"STATELESS".equals(childGroupDto.getExecutionEngine())) {
                if (!allPortsInState(connectorId, childGroupEntity.getId(), expectedState)) {
                    return false;
                }
            }
        }

        return true;
    }

    private void waitForStatelessGroupRunning(final String connectorId) throws InterruptedException {
        waitFor(() -> isStatelessGroupInState(connectorId, StatelessGroupScheduledState.RUNNING.name()));
    }

    private void waitForStatelessGroupStopped(final String connectorId) throws InterruptedException {
        waitFor(() -> isStatelessGroupInState(connectorId, StatelessGroupScheduledState.STOPPED.name()));
    }

    private boolean isStatelessGroupInState(final String connectorId, final String expectedState) throws NiFiClientException, IOException {
        return findStatelessGroupInState(connectorId, null, expectedState);
    }

    private boolean findStatelessGroupInState(final String connectorId, final String groupId, final String expectedState) throws NiFiClientException, IOException {
        final FlowDTO flowDto = getFlow(connectorId, groupId);

        for (final ProcessGroupEntity childGroupEntity : flowDto.getProcessGroups()) {
            final ProcessGroupDTO childGroupDto = childGroupEntity.getComponent();
            if ("STATELESS".equals(childGroupDto.getExecutionEngine())) {
                final String actualState = childGroupDto.getStatelessGroupScheduledState();
                logger.debug("Stateless group {} is in state {} (expected {})", childGroupDto.getName(), actualState, expectedState);
                if (expectedState.equals(actualState)) {
                    return true;
                }
            } else {
                if (findStatelessGroupInState(connectorId, childGroupEntity.getId(), expectedState)) {
                    return true;
                }
            }
        }

        return false;
    }

    private FlowDTO getFlow(final String connectorId, final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = (groupId == null)
            ? getNifiClient().getConnectorClient().getFlow(connectorId)
            : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);

        return flowEntity.getProcessGroupFlow().getFlow();
    }
}
