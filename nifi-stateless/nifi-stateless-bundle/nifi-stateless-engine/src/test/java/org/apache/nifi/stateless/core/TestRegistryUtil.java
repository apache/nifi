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

package org.apache.nifi.stateless.core;

import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRegistryUtil {
    private static final String registryUrl = "http://localhost:18080";
    private static final String rootBucketId = UUID.randomUUID().toString();
    private static final String rootFlowId = UUID.randomUUID().toString();
    private static final int rootVersion = 1;

    private static final String childBucketId = UUID.randomUUID().toString();
    private static final String childFlowId = UUID.randomUUID().toString();
    private static final int childVersion = 1;

    @Test
    public void testRegistryUrlCreation() throws NiFiRegistryException, IOException {
        NiFiRegistryClient registryClient = mock(NiFiRegistryClient.class);
        FlowSnapshotClient flowSnapshotClient = mock(FlowSnapshotClient.class);

        RegistryUtil registryUtil = new RegistryUtil(registryClient, registryUrl, null);

        when(registryClient.getFlowSnapshotClient()).thenReturn(flowSnapshotClient);
        when(flowSnapshotClient.get(rootBucketId, rootFlowId, rootVersion)).thenAnswer(
                invocation -> buildVfs());
        when(flowSnapshotClient.get(childBucketId, childFlowId, childVersion)).thenAnswer(
                invocation -> buildVfs());

        VersionedFlowSnapshot snapshot = registryUtil.getFlowContents(rootBucketId, rootFlowId, rootVersion, true, null);
        VersionedFlowCoordinates coordinates = snapshot.getFlowContents().getVersionedFlowCoordinates();

        String formattedUrl = registryUtil.getBaseRegistryUrl(coordinates.getStorageLocation());
        assertEquals(formattedUrl, registryUrl);
    }

    private VersionedFlowSnapshot buildVfs(){
        VersionedFlowSnapshot vfs = new VersionedFlowSnapshot();
        VersionedProcessGroup vpg1 = new VersionedProcessGroup();
        VersionedProcessGroup vpg2 = new VersionedProcessGroup();
        VersionedFlowCoordinates vfc1 = new VersionedFlowCoordinates();
        VersionedFlowCoordinates vfc2 = new VersionedFlowCoordinates();

        String storageLocation1 = String.format("%s/nifi-registry-api/buckets/%s/flows/%s/versions/%s", registryUrl, rootBucketId, rootFlowId, rootVersion);
        vfc1.setStorageLocation(storageLocation1);
        vfc1.setBucketId(rootBucketId);
        vfc1.setFlowId(rootFlowId);
        vfc1.setVersion(rootVersion);

        String storageLocation2 = String.format("%s/nifi-registry-api/buckets/%s/flows/%s/versions/%s", registryUrl, childBucketId, childFlowId, childVersion);
        vfc2.setStorageLocation(storageLocation2);
        vfc2.setBucketId(rootBucketId);
        vfc2.setFlowId(rootFlowId);
        vfc2.setVersion(rootVersion);

        vpg1.setVersionedFlowCoordinates(vfc1);
        vpg1.setProcessGroups(Collections.singleton(vpg1));

        vpg2.setVersionedFlowCoordinates(vfc2);
        vfs.setFlowContents(vpg2);
        return vfs;
    }
}