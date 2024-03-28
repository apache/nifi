package org.apache.nifi.stateless.core;

import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class TestRegistryUtil {

    private String registryUrl = "http://localhost:18080";
    final String bucketId = UUID.randomUUID().toString();
    final String flowId = UUID.randomUUID().toString();
    final int version = 1;
    final String storageLocation = "http://localhost:18080/nifi-registry-api/buckets/"+bucketId+"/flows/"+flowId+"/versions/"+version;

    @Test
    public void testRegistryUrlCreation() throws NiFiRegistryException, IOException {
        RegistryUtil registryUtil = mock(RegistryUtil.class);
        Mockito.when(registryUtil.getFlowContents(bucketId, flowId, version, true, null)).thenAnswer(
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
        vfc.setStorageLocation(storageLocation);
        vpg.setVersionedFlowCoordinates(vfc);
        vfs.setFlowContents(vpg);
        return vfs;
    }
}