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
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRegistryUtil {
    private static final String BASE_REGISTRY_URL = "https://localhost:18443/context-path";

    private static final String STORAGE_LOCATION_FORMAT = "%s/nifi-registry-api/buckets/%s/flows/%s/versions/%s";

    private static final String ROOT_BUCKET_ID = UUID.randomUUID().toString();
    private static final String ROOT_FLOW_ID = UUID.randomUUID().toString();
    private static final int ROOT_VERSION = 1;

    private static final String CHILD_BUCKET_ID = UUID.randomUUID().toString();
    private static final String CHILD_FLOW_ID = UUID.randomUUID().toString();
    private static final int CHILD_VERSION = 2;

    @Test
    public void testGetBaseRegistryUrl() throws NiFiRegistryException, IOException {
        final NiFiRegistryClient registryClient = mock(NiFiRegistryClient.class);
        final RegistryUtil registryUtil = new RegistryUtil(registryClient, BASE_REGISTRY_URL, null);

        final FlowSnapshotClient flowSnapshotClient = mock(FlowSnapshotClient.class);
        when(registryClient.getFlowSnapshotClient()).thenReturn(flowSnapshotClient);

        final VersionedProcessGroup childVersionedProcessGroup = getChildVersionedProcessGroup();
        final VersionedFlowSnapshot rootSnapshot = buildRootSnapshot(Collections.singleton(childVersionedProcessGroup));
        final String rootRegistryUrl = registryUtil.getBaseRegistryUrl(rootSnapshot.getFlowContents().getVersionedFlowCoordinates().getStorageLocation());
        assertEquals(BASE_REGISTRY_URL, rootRegistryUrl);

        final VersionedFlowSnapshot childSnapshot = new VersionedFlowSnapshot();
        childSnapshot.setFlowContents(childVersionedProcessGroup);
        final String childRegistryUrl = registryUtil.getBaseRegistryUrl(childVersionedProcessGroup.getVersionedFlowCoordinates().getStorageLocation());
        assertEquals(BASE_REGISTRY_URL, childRegistryUrl);

        when(flowSnapshotClient.get(eq(ROOT_BUCKET_ID), eq(ROOT_FLOW_ID), eq(ROOT_VERSION))).thenReturn(rootSnapshot);
        when(flowSnapshotClient.get(eq(CHILD_BUCKET_ID), eq(CHILD_FLOW_ID), eq(CHILD_VERSION))).thenReturn(childSnapshot);

        final VersionedFlowSnapshot flowSnapshot = registryUtil.getFlowContents(ROOT_BUCKET_ID, ROOT_FLOW_ID, ROOT_VERSION, true, null);
        assertEquals(rootSnapshot, flowSnapshot);
    }

    private VersionedFlowSnapshot buildRootSnapshot(final Set<VersionedProcessGroup> childGroups){
        final String storageLocation = String.format(STORAGE_LOCATION_FORMAT, BASE_REGISTRY_URL, ROOT_BUCKET_ID, ROOT_FLOW_ID, ROOT_VERSION);
        final VersionedFlowCoordinates coordinates = new VersionedFlowCoordinates();
        coordinates.setStorageLocation(storageLocation);
        coordinates.setBucketId(ROOT_BUCKET_ID);
        coordinates.setFlowId(ROOT_FLOW_ID);
        coordinates.setVersion(String.valueOf(ROOT_VERSION));

        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setVersionedFlowCoordinates(coordinates);
        group.setProcessGroups(childGroups);

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setFlowContents(group);
        return snapshot;
    }

    private VersionedProcessGroup getChildVersionedProcessGroup() {
        final String storageLocation = String.format(STORAGE_LOCATION_FORMAT, BASE_REGISTRY_URL, CHILD_BUCKET_ID, CHILD_FLOW_ID, CHILD_VERSION);
        final VersionedFlowCoordinates coordinates = new VersionedFlowCoordinates();
        coordinates.setStorageLocation(storageLocation);
        coordinates.setBucketId(CHILD_BUCKET_ID);
        coordinates.setFlowId(CHILD_FLOW_ID);
        coordinates.setVersion(String.valueOf(CHILD_VERSION));

        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setVersionedFlowCoordinates(coordinates);

        return group;
    }
}