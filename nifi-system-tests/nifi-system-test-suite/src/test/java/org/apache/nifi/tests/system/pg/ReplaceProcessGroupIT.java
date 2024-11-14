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
package org.apache.nifi.tests.system.pg;

import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReplaceProcessGroupIT  extends NiFiSystemIT {

    @Test
    public void testReplaceProcessGroup() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity emptyProcessGroup = getClientUtil().createProcessGroup("My Group", "root");

        final VersionedLabel versionedLabel = new VersionedLabel();
        versionedLabel.setIdentifier(UUID.randomUUID().toString());
        versionedLabel.setLabel("New Label");
        versionedLabel.setPosition(new Position(0, 0));
        versionedLabel.setWidth(100.00);
        versionedLabel.setHeight(100.00);

        final VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        versionedProcessGroup.getLabels().add(versionedLabel);

        final RegisteredFlowSnapshot snapshot = new RegisteredFlowSnapshot();
        snapshot.setFlowContents(versionedProcessGroup);

        final ProcessGroupImportEntity importEntity = new ProcessGroupImportEntity();
        importEntity.setVersionedFlowSnapshot(snapshot);
        importEntity.setProcessGroupRevision(emptyProcessGroup.getRevision());

        final String pgId = emptyProcessGroup.getId();
        final ProcessGroupReplaceRequestEntity createdReplaceRequestEntity =
                getNifiClient().getProcessGroupClient().replaceProcessGroup(pgId, importEntity);
        final String requestId = createdReplaceRequestEntity.getRequest().getRequestId();

        waitForReplaceRequest(pgId, requestId);

        final ProcessGroupReplaceRequestEntity replaceRequest = getNifiClient().getProcessGroupClient().getProcessGroupReplaceRequest(pgId, requestId);
        assertNull(replaceRequest.getRequest().getFailureReason());

        final ProcessGroupFlowEntity replacedPgFlowEntity = getNifiClient().getFlowClient().getProcessGroup(pgId);
        assertNotNull(replacedPgFlowEntity);

        final ProcessGroupFlowDTO replacedPgFlowDTO = replacedPgFlowEntity.getProcessGroupFlow();
        assertNotNull(replacedPgFlowDTO);
        assertNotNull(replacedPgFlowDTO.getFlow());
        assertNotNull(replacedPgFlowDTO.getFlow().getLabels());
        assertEquals(1, replacedPgFlowDTO.getFlow().getLabels().size());

        final LabelEntity labelEntity = replacedPgFlowDTO.getFlow().getLabels().stream().findFirst().orElse(null);
        assertNotNull(labelEntity);
        assertNotNull(labelEntity.getComponent());
        assertEquals(versionedLabel.getLabel(), labelEntity.getComponent().getLabel());
    }

    private void waitForReplaceRequest(final String pgId, final String requestId) throws InterruptedException {
        waitFor(() -> {
            try {
                final ProcessGroupReplaceRequestEntity replaceRequest =
                        getNifiClient().getProcessGroupClient().getProcessGroupReplaceRequest(pgId, requestId);
                if (replaceRequest == null) {
                    return false;
                }
                return replaceRequest.getRequest().isComplete();
            } catch (final Exception e) {
                return false;
            }
        });
    }
}
