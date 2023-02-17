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
package org.apache.nifi.registry.provider.flow;

import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.flow.MetadataAwareFlowPersistenceProvider;
import org.apache.nifi.registry.metadata.BucketMetadata;
import org.apache.nifi.registry.metadata.FlowMetadata;
import org.apache.nifi.registry.metadata.FlowSnapshotMetadata;
import org.apache.nifi.registry.service.MetadataService;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFlowMetadataSynchronizer {

    private MetadataService metadataService;
    private MetadataAwareFlowPersistenceProvider metadataAwareflowPersistenceProvider;
    private FlowPersistenceProvider standardFlowPersistenceProvider;
    private List<BucketMetadata> metadata;
    private FlowMetadataSynchronizer synchronizer;

    @Before
    public void setup() {
        metadataService = mock(MetadataService.class);
        metadataAwareflowPersistenceProvider = mock(MetadataAwareFlowPersistenceProvider.class);
        standardFlowPersistenceProvider = mock(FlowPersistenceProvider.class);
        synchronizer = new FlowMetadataSynchronizer(metadataService, metadataAwareflowPersistenceProvider);

        final FlowSnapshotMetadata snapshotMetadata1 = new FlowSnapshotMetadata();
        snapshotMetadata1.setVersion(1);
        snapshotMetadata1.setAuthor("user1");
        snapshotMetadata1.setComments("This is v1");
        snapshotMetadata1.setCreated(System.currentTimeMillis());

        final FlowSnapshotMetadata snapshotMetadata2 = new FlowSnapshotMetadata();
        snapshotMetadata2.setVersion(2);
        snapshotMetadata2.setAuthor("user1");
        snapshotMetadata2.setComments("This is v2");
        snapshotMetadata2.setCreated(System.currentTimeMillis());

        final List<FlowSnapshotMetadata> snapshotMetadata = Arrays.asList(snapshotMetadata1, snapshotMetadata2);

        final FlowMetadata flowMetadata1 = new FlowMetadata();
        flowMetadata1.setIdentifier("1");
        flowMetadata1.setName("Flow 1");
        flowMetadata1.setDescription("This is flow 1");
        flowMetadata1.setFlowSnapshotMetadata(snapshotMetadata);

        final List<FlowMetadata> flowMetadata = Arrays.asList(flowMetadata1);

        final BucketMetadata bucketMetadata = new BucketMetadata();
        bucketMetadata.setIdentifier("1");
        bucketMetadata.setName("Bucket 1");
        bucketMetadata.setDescription("This is bucket 1");
        bucketMetadata.setFlowMetadata(flowMetadata);

        metadata = Arrays.asList(bucketMetadata);
        when(metadataAwareflowPersistenceProvider.getMetadata()).thenReturn(metadata);
    }

    @Test
    public void testWhenMetadataAwareAndHasDataShouldSynchronize() {
        when(metadataService.getAllBuckets()).thenReturn(Collections.emptyList());

        synchronizer.synchronize();
        verify(metadataService, times(1)).createBucket(any(BucketEntity.class));
        verify(metadataService, times(1)).createFlow(any(FlowEntity.class));
        verify(metadataService, times(2)).createFlowSnapshot(any(FlowSnapshotEntity.class));
    }

    @Test
    public void testWhenMetadataAwareAndDatabaseNotEmptyShouldNotSynchronize() {
        final BucketEntity bucketEntity = new BucketEntity();
        bucketEntity.setId("1");
        when(metadataService.getAllBuckets()).thenReturn(Collections.singletonList(bucketEntity));

        synchronizer.synchronize();
        verify(metadataService, times(0)).createBucket(any(BucketEntity.class));
        verify(metadataService, times(0)).createFlow(any(FlowEntity.class));
        verify(metadataService, times(0)).createFlowSnapshot(any(FlowSnapshotEntity.class));
    }

    @Test
    public void testWhenNotMetadataAwareShouldNotSynchronize() {
        when(metadataService.getAllBuckets()).thenReturn(Collections.emptyList());
        synchronizer = new FlowMetadataSynchronizer(metadataService, standardFlowPersistenceProvider);
        synchronizer.synchronize();

        verify(metadataService, times(0)).createBucket(any(BucketEntity.class));
        verify(metadataService, times(0)).createFlow(any(FlowEntity.class));
        verify(metadataService, times(0)).createFlowSnapshot(any(FlowSnapshotEntity.class));
    }
}
