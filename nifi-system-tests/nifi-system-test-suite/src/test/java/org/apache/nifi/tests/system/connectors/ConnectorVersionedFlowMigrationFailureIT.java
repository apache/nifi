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

import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectorVersionedFlowMigrationFailureIT extends ConnectorVersionedFlowMigrationLocalIT {
    @Test
    public void testMigrationFailureRollsBackConnectorAndLeavesSourceUntouched() throws Exception {
        final File outputFile = new File("target/migration/failure-local-output.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("FailureSource", registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final String sourceGroupId = sourceFixture.processGroup().getId();
        final String originalName = getNifiClient().getProcessGroupClient().getProcessGroup(sourceGroupId).getComponent().getName();

        final ConnectorEntity connector = getClientUtil().createConnector("FailingConfigurationMigrationConnector");
        final String connectorId = connector.getId();
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceGroupId);
        final MigrationRequestEntity completedRequest = getClientUtil().waitForMigrationFailure(connectorId, requestEntity.getRequest().getRequestId());
        assertNotNull(completedRequest.getRequest().getFailureReason());

        assertConnectorFresh(connectorId);
        assertSourceUntouched(sourceFixture, originalName);
    }

    @Test
    public void testCorruptPayloadUploadRejectedAndConnectorRemainsFresh() throws Exception {
        final File corruptPayload = new File("target/migration/corrupt-payload.json");
        corruptPayload.getParentFile().mkdirs();
        Files.writeString(corruptPayload.toPath(), "{ this is not valid json");

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        assertThrows(NiFiClientException.class, () -> getClientUtil().uploadMigrationPayload(connector.getId(), corruptPayload));

        assertConnectorFresh(connector.getId());
    }

    @Test
    public void testMigrationInterruptedByRestartLeavesConnectorFresh() throws Exception {
        final File outputFile = new File("target/migration/failure-restart-output.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("FailureRestartSource", registryClient, false, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final String sourceGroupId = sourceFixture.processGroup().getId();
        final String originalName = getNifiClient().getProcessGroupClient().getProcessGroup(sourceGroupId).getComponent().getName();

        final ConnectorEntity connector = getClientUtil().createConnector("FailingConfigurationMigrationConnector");
        final String connectorId = connector.getId();
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceGroupId);

        Thread.sleep(1000L);
        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        setupClient();

        waitFor(() -> {
            try {
                getNifiClient().getConnectorClient().getConnector(connectorId);
                return true;
            } catch (final Exception e) {
                return false;
            }
        });

        assertThrows(NiFiClientException.class, () -> getNifiClient().getConnectorClient().getMigrationStatus(connectorId, requestEntity.getRequest().getRequestId()));

        waitFor(() -> isConnectorFresh(connectorId));
        assertConnectorFresh(connectorId);
        assertSourceUntouched(sourceFixture, originalName);
    }

    private boolean isConnectorFresh(final String connectorId) {
        try {
            final ConnectorEntity connectorEntity = getNifiClient().getConnectorClient().getConnector(connectorId);
            final String managedGroupId = connectorEntity.getComponent().getManagedProcessGroupId();
            final ProcessGroupFlowEntity flowEntity = getNifiClient().getConnectorClient().getFlow(connectorId, managedGroupId);
            if (flowEntity.getProcessGroupFlow().getFlow().getProcessors() != null && !flowEntity.getProcessGroupFlow().getFlow().getProcessors().isEmpty()) {
                return false;
            }
            if (flowEntity.getProcessGroupFlow().getFlow().getConnections() != null && !flowEntity.getProcessGroupFlow().getFlow().getConnections().isEmpty()) {
                return false;
            }

            return getNifiClient().getConnectorClient().getAssets(connectorId).getAssets() == null
                    || getNifiClient().getConnectorClient().getAssets(connectorId).getAssets().isEmpty();
        } catch (final Exception e) {
            return false;
        }
    }
}
