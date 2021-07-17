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

import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.Response;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestProcessGroupResource {

    @InjectMocks
    private ProcessGroupResource processGroupResource = new ProcessGroupResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    public void testExportProcessGroup() {
        final String groupId = UUID.randomUUID().toString();
        final VersionedFlowSnapshot versionedFlowSnapshot = mock(VersionedFlowSnapshot.class);

        when(serviceFacade.getCurrentFlowSnapshotByGroupId(groupId)).thenReturn(versionedFlowSnapshot);

        final String flowName = "flowname";
        final VersionedProcessGroup versionedProcessGroup = mock(VersionedProcessGroup.class);
        when(versionedFlowSnapshot.getFlowContents()).thenReturn(versionedProcessGroup);
        when(versionedProcessGroup.getName()).thenReturn(flowName);

        final Response response = processGroupResource.exportProcessGroup(groupId);

        final VersionedFlowSnapshot resultEntity = (VersionedFlowSnapshot)response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(versionedFlowSnapshot, resultEntity);
    }

}