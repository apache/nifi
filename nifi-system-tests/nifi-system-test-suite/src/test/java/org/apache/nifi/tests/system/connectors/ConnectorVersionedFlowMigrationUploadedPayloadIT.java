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

import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.MigrationPayloadEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConnectorVersionedFlowMigrationUploadedPayloadIT extends ConnectorVersionedFlowMigrationLocalIT {
    @Test
    public void testMigrateConnectorFromUploadedPayload() throws Exception {
        final File outputFile = new File("target/migration/uploaded-output.txt");
        final File exportFile = new File("target/migration/uploaded-source.json");
        outputFile.delete();
        exportFile.delete();

        final SourceFixture sourceFixture = createSourceFixture("UploadedMigrationSource", registerClient(), false, outputFile, false);
        getClientUtil().startProcessGroupComponents(sourceFixture.processGroup().getId());
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        getClientUtil().stopProcessGroupComponents(sourceFixture.processGroup().getId());
        getClientUtil().emptyQueue(sourceFixture.connection().getId());

        getNifiClient().getProcessGroupClient().exportProcessGroup(sourceFixture.processGroup().getId(), true, true, exportFile);
        final ProcessGroupEntity sourceProcessGroup = getNifiClient().getProcessGroupClient().getProcessGroup(sourceFixture.processGroup().getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(sourceProcessGroup);

        outputFile.delete();

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();
        final MigrationPayloadEntity payloadEntity = getClientUtil().uploadMigrationPayload(connectorId, exportFile);
        assertNotNull(payloadEntity.getPayload());

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromPayload(connectorId, payloadEntity.getPayload().getPayloadId());
        getClientUtil().waitForMigrationSuccess(connectorId, requestEntity.getRequest().getRequestId());

        getClientUtil().startConnector(connectorId);
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        assertEquals(Files.readString(SAMPLE_ASSET_FILE.toPath()).trim(), Files.readString(outputFile.toPath()).trim());
    }
}
