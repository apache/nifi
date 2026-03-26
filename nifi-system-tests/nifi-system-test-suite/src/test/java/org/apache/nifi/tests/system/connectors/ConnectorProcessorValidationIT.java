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
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Verifies that processors within a Connector's managed ProcessGroup complete validation
 * after a NiFi restart. Reproduces the issue where connector-internal processors get stuck
 * in VALIDATING state because TriggerValidationTask only walks the root group hierarchy,
 * and connector-managed groups are not descendants of the root group.
 */
public class ConnectorProcessorValidationIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorProcessorValidationIT.class);

    @Test
    public void testStoppedConnectorProcessorsValidateAfterRestart() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);
        waitForAllConnectorProcessorsValidated(connectorId);
        logger.info("All connector processors validated before restart");

        getNiFiInstance().stop();
        getNiFiInstance().start();

        getClientUtil().waitForValidConnector(connectorId);
        waitForAllConnectorProcessorsValidated(connectorId);
        logger.info("All connector processors validated after restart");
    }

    private void waitForAllConnectorProcessorsValidated(final String connectorId) throws InterruptedException {
        waitFor(() -> {
            try {
                final List<ProcessorDTO> stuckProcessors = getValidatingProcessors(connectorId, null);
                if (!stuckProcessors.isEmpty()) {
                    logger.debug("{} connector processor(s) still in VALIDATING state: {}",
                        stuckProcessors.size(), stuckProcessors.stream().map(ProcessorDTO::getName).toList());
                    return false;
                }

                return true;
            } catch (final Exception e) {
                logger.debug("Failed to retrieve connector flow for validation check", e);
                return false;
            }
        });
    }

    private List<ProcessorDTO> getValidatingProcessors(final String connectorId, final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = (groupId == null)
            ? getNifiClient().getConnectorClient().getFlow(connectorId)
            : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);

        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final List<ProcessorDTO> matching = new ArrayList<>();

        for (final ProcessorEntity processorEntity : flowDto.getProcessors()) {
            final ProcessorDTO dto = processorEntity.getComponent();
            if (ScheduledState.DISABLED.name().equals(dto.getState())) {
                continue;
            }

            if (ProcessorDTO.VALIDATING.equalsIgnoreCase(dto.getValidationStatus())) {
                matching.add(dto);
            }
        }

        for (final ProcessGroupEntity childGroupEntity : flowDto.getProcessGroups()) {
            matching.addAll(getValidatingProcessors(connectorId, childGroupEntity.getId()));
        }

        return matching;
    }
}
