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
package org.apache.nifi.web.api;

import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.web.NiFiServiceFacade;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestVersionsResource {

    @InjectMocks
    private VersionsResource versionsResource = new VersionsResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    public void testExportFlowVersion() {
        final String groupId = UUID.randomUUID().toString();
        final RegisteredFlowSnapshot versionedFlowSnapshot = mock(RegisteredFlowSnapshot.class);
        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(versionedFlowSnapshot);
        when(serviceFacade.getVersionedFlowSnapshotByGroupId(groupId)).thenReturn(snapshotContainer);

        final String flowName = "flowname";
        final String flowVersion = "1";
        final VersionedProcessGroup versionedProcessGroup = mock(VersionedProcessGroup.class);
        final RegisteredFlowSnapshotMetadata snapshotMetadata = mock(RegisteredFlowSnapshotMetadata.class);
        when(versionedFlowSnapshot.getFlowContents()).thenReturn(versionedProcessGroup);
        when(versionedProcessGroup.getName()).thenReturn(flowName);
        when(versionedFlowSnapshot.getSnapshotMetadata()).thenReturn(snapshotMetadata);
        when(snapshotMetadata.getVersion()).thenReturn(flowVersion);

        final VersionedProcessGroup innerVersionedProcessGroup = mock(VersionedProcessGroup.class);
        final VersionedProcessGroup innerInnerVersionedProcessGroup = mock(VersionedProcessGroup.class);
        when(versionedProcessGroup.getProcessGroups()).thenReturn(Collections.singleton(innerVersionedProcessGroup));
        when(innerVersionedProcessGroup.getProcessGroups()).thenReturn(Collections.singleton(innerInnerVersionedProcessGroup));

        final Response response = versionsResource.exportFlowVersion(groupId);

        final RegisteredFlowSnapshot resultEntity = (RegisteredFlowSnapshot)response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(versionedFlowSnapshot, resultEntity);

        verify(versionedFlowSnapshot).setFlow(null);
        verify(versionedFlowSnapshot).setBucket(null);
        verify(versionedFlowSnapshot).setSnapshotMetadata(null);
        verify(versionedProcessGroup).setVersionedFlowCoordinates(null);
        verify(innerVersionedProcessGroup).setVersionedFlowCoordinates(null);
        verify(innerInnerVersionedProcessGroup).setVersionedFlowCoordinates(null);
    }

}