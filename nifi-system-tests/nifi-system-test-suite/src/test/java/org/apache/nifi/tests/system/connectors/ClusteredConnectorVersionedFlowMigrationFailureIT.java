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

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.MigrationPayloadEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusteredConnectorVersionedFlowMigrationFailureIT extends ConnectorVersionedFlowMigrationFailureIT {
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testUploadedPayloadWithTooManyLocalNodeStatesFails() throws Exception {
        final File outputFile = new File("target/migration/cluster-topology-output.txt");
        final File exportFile = new File("target/migration/cluster-topology-export.json");
        outputFile.delete();
        exportFile.delete();

        final SourceFixture sourceFixture = createSourceFixture("ClusterTopologySource", registerClient(), false, outputFile, false);
        getClientUtil().startProcessGroupComponents(sourceFixture.processGroup().getId());
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        getClientUtil().stopProcessGroupComponents(sourceFixture.processGroup().getId());
        getClientUtil().emptyQueue(sourceFixture.connection().getId());

        getNifiClient().getProcessGroupClient().exportProcessGroup(sourceFixture.processGroup().getId(), true, true, exportFile);

        // Inflate the LOCAL state to three node states even though the destination cluster only has two
        // connected nodes; the cluster-topology rule must reject the payload.
        getClientUtil().rewriteMigrationPayloadWithNodeStates(exportFile, "StatefulCountProcessor", List.of("1", "2", "3"));

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final MigrationPayloadEntity payloadEntity = getClientUtil().uploadMigrationPayload(connector.getId(), exportFile);
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromPayload(connector.getId(), payloadEntity.getPayload().getPayloadId());
        final MigrationRequestEntity completedRequest = getClientUtil().waitForMigrationFailure(connector.getId(), requestEntity.getRequest().getRequestId());
        assertTrue(completedRequest.getRequest().getFailureReason().contains("connected node"));

        assertConnectorFresh(connector.getId());
    }

    @Test
    public void testAsymmetricPerNodeMigrationFailureSurfacesInMergedResponse() throws Exception {
        final File outputFile = new File("target/migration/asymmetric-failure-output.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("AsymmetricFailureSource", registryClient, false, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        // AsymmetricFailureMigrationConnector throws on node 2 only; node 1 succeeds. The merged response
        // returned by the migration-request endpoint must surface node 2's failure, otherwise an operator polling
        // the cluster would mistakenly see success.
        final ConnectorEntity connector = getClientUtil().createConnector("AsymmetricFailureMigrationConnector");
        final String connectorId = connector.getId();

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        final MigrationRequestEntity completedRequest = getClientUtil().waitForMigrationFailure(connectorId, requestEntity.getRequest().getRequestId());

        final String failureReason = completedRequest.getRequest().getFailureReason();
        assertNotNull(failureReason);
        assertTrue(failureReason.contains("node 2"), failureReason);
    }
}
