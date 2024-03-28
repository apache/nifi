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
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRegistryUtil {
    private String registryUrl = "http://localhost:18080";
    final String bucketId = UUID.randomUUID().toString();
    final String flowId = UUID.randomUUID().toString();
    final int version = 1;

    @Test
    public void testRegistryUrlCreation() throws NiFiRegistryException, IOException {
        RegistryUtil registryUtil = mock(RegistryUtil.class);
        when(registryUtil.getFlowContents(bucketId, flowId, version, true, null)).thenAnswer(
                invocation -> createVfs());
        VersionedFlowSnapshot snapshot = registryUtil.getFlowContents(bucketId, flowId, version, true, null);
        VersionedFlowCoordinates coordinates = snapshot.getFlowContents().getVersionedFlowCoordinates();
        URL storageLocation = new URL(coordinates.getStorageLocation());
        final String formattedUrl = storageLocation.getProtocol()+"://"+storageLocation.getAuthority();
        assertEquals(formattedUrl, registryUrl);
    }

    private VersionedFlowSnapshot createVfs(){
        VersionedFlowSnapshot vfs = new VersionedFlowSnapshot();
        VersionedProcessGroup vpg = new VersionedProcessGroup();
        VersionedFlowCoordinates vfc = new VersionedFlowCoordinates();
        String storageLocation = String.format(registryUrl+"/nifi-registry-api/buckets/%s/flows/%s/versions/%s", bucketId, flowId, version);
        vfc.setStorageLocation(storageLocation);
        vpg.setVersionedFlowCoordinates(vfc);
        vfs.setFlowContents(vpg);
        return vfs;
    }
}