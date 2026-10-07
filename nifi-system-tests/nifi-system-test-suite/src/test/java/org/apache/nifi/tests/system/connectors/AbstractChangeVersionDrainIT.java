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
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.tests.system.nar.NarUploadUtil;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Version-change test support.
 */
public abstract class AbstractChangeVersionDrainIT extends AbstractConnectorUpgradeDrainIT {

    @Override
    protected boolean isAllowFactoryReuse() {
        return true;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return false;
    }

    @BeforeEach
    public void uploadDrainVersions() throws Exception {
        uploadDrainNars(DRAIN_V1, DRAIN_V2);
    }

    /**
     * Deletes the uploaded NARs along with the flow, because the reused instance is shared with tests that expect no uploaded NARs.
     */
    @Override
    protected void destroyFlow() throws NiFiClientException, IOException, InterruptedException {
        super.destroyFlow();

        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        for (final NarSummaryEntity nar : getNifiClient().getControllerClient().getNarSummaries().getNarSummaries()) {
            narUploadUtil.deleteNar(nar.getNarSummary());
        }
    }

    @Override
    protected String createOutgoingConnector(final Map<String, String> properties) throws Exception {
        return createDrainConnector(DRAIN_V1, properties);
    }

    @Override
    protected void triggerUpgrade(final String connectorId) throws Exception {
        changeVersion(connectorId, DRAIN_V2);
    }

    @Override
    protected ExpectedBundle outgoingBundle() {
        return new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V1);
    }

    @Override
    protected ExpectedBundle incomingBundle() {
        return new ExpectedBundle(DRAIN_BUNDLE_ARTIFACT, DRAIN_V2);
    }

    protected void assertFlowEvolutionStatus(final String connectorId, final FlowEvolutionStatus expectedStatus) throws NiFiClientException, IOException {
        assertEquals(expectedStatus.name(), getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getFlowEvolutionStatus());
    }

    protected void assertStoppedPending(final String connectorId, final String validationError) throws Exception {
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        if (validationError == null) {
            return;
        }
        final String errors = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getValidationErrors().toString();
        assertTrue(errors.contains(validationError), errors);
    }
}
