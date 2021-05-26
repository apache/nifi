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
package org.apache.nifi.registry.toolkit.persistence;

import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.provider.flow.StandardFlowSnapshotContext;
import org.apache.nifi.registry.service.MetadataService;
import org.apache.nifi.registry.service.mapper.BucketMappings;
import org.apache.nifi.registry.service.mapper.FlowMappings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class FlowPersistenceProviderMigratorTest {
    private MetadataService metadataService;
    private FlowPersistenceProvider fromProvider;
    private FlowPersistenceProvider toProvider;

    private Map<String, BucketEntity> buckets;
    private Map<String, Map<String, FlowEntity>> bucketFlows;
    private Map<String, BucketEntity> flowBuckets;
    private Map<String, List<FlowSnapshotEntity>> flowSnapshots;

    @Before
    public void setup() {
        metadataService = mock(MetadataService.class);
        fromProvider = mock(FlowPersistenceProvider.class);
        toProvider = mock(FlowPersistenceProvider.class);

        buckets = new TreeMap<>();
        bucketFlows = new HashMap<>();
        flowBuckets = new HashMap<>();
        flowSnapshots = new HashMap<>();

        when(metadataService.getAllBuckets()).thenAnswer(invocation -> new ArrayList<>(buckets.values()));
        when(metadataService.getFlowsByBucket(anyString())).thenAnswer(invocation -> new ArrayList<>(bucketFlows.get(invocation.<String>getArgument(0)).values()));
        when(metadataService.getSnapshots(anyString())).thenAnswer(invocation -> new ArrayList<>(flowSnapshots.get(invocation.<String>getArgument(0))));
        when(fromProvider.getFlowContent(anyString(), anyString(), anyInt())).thenAnswer(invocation -> {
            FlowSnapshotEntity flowSnapshotEntity = flowSnapshots.get(invocation.<String>getArgument(1)).get(invocation.<Integer>getArgument(2) - 1);
            assertEquals(invocation.getArgument(2), flowSnapshotEntity.getVersion());
            FlowEntity flowEntity = bucketFlows.get(invocation.<String>getArgument(0)).get(invocation.<String>getArgument(1));
            assertEquals(invocation.getArgument(0), flowEntity.getBucketId());
            assertNotNull(buckets.get(invocation.<String>getArgument(0)));
            return getContent(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
        });
    }

    private byte[] getContent(String bucketId, String flowId, int version) {
        return (bucketId + "-" + flowId + "-" + version).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testMigration() {
        createBucket("bucket1");
        BucketEntity bucket2 = createBucket("bucket2");
        BucketEntity bucket3 = createBucket("bucket3");

        FlowEntity flow1 = createFlow(bucket2, "flow1");
        FlowEntity flow2 = createFlow(bucket3, "flow2");
        FlowEntity flow3 = createFlow(bucket3, "flow3");

        List<FlowSnapshotEntity> snapshots = Arrays.asList(
                createSnapshot(flow1, 1),
                createSnapshot(flow2, 1),
                createSnapshot(flow2, 2),
                createSnapshot(flow3, 1),
                createSnapshot(flow3, 2),
                createSnapshot(flow3, 3));

        new FlowPersistenceProviderMigrator().doMigrate(metadataService, fromProvider, toProvider);

        for (FlowSnapshotEntity snapshot : snapshots) {
            verifyMigrate(snapshot);
        }

        verifyNoMoreInteractions(toProvider);
    }

    private BucketEntity createBucket(String id) {
        BucketEntity bucketEntity = new BucketEntity();
        bucketEntity.setId(id);
        bucketEntity.setName(id + "Name");
        bucketEntity.setCreated(new Date());
        buckets.put(id, bucketEntity);
        bucketFlows.put(id, new TreeMap<>());
        return bucketEntity;
    }

    private FlowEntity createFlow(BucketEntity bucketEntity, String id) {
        FlowEntity flowEntity = new FlowEntity();
        flowEntity.setBucketId(bucketEntity.getId());
        flowEntity.setId(id);
        flowEntity.setName(id + "Name");
        flowEntity.setCreated(new Date());
        flowEntity.setModified(new Date());
        bucketFlows.get(bucketEntity.getId()).put(id, flowEntity);
        flowBuckets.put(id, bucketEntity);
        flowSnapshots.put(id, new ArrayList<>());
        return flowEntity;
    }

    private FlowSnapshotEntity createSnapshot(FlowEntity flowEntity, int version) {
        FlowSnapshotEntity flowSnapshotEntity = new FlowSnapshotEntity();
        flowSnapshotEntity.setFlowId(flowEntity.getId());
        flowSnapshotEntity.setVersion(version);
        flowSnapshotEntity.setCreated(new Date());
        flowSnapshots.get(flowEntity.getId()).add(flowSnapshotEntity);
        return flowSnapshotEntity;
    }

    private void verifyMigrate(FlowSnapshotEntity flowSnapshotEntity) {
        BucketEntity bucketEntity = flowBuckets.get(flowSnapshotEntity.getFlowId());
        FlowEntity flowEntity = bucketFlows.get(bucketEntity.getId()).get(flowSnapshotEntity.getFlowId());
        verify(toProvider).saveFlowContent(eq(new StandardFlowSnapshotContext.Builder(
                BucketMappings.map(bucketEntity),
                FlowMappings.map(bucketEntity, flowEntity),
                FlowMappings.map(bucketEntity, flowSnapshotEntity)).build()),
                AdditionalMatchers.aryEq(getContent(bucketEntity.getId(), flowSnapshotEntity.getFlowId(), flowSnapshotEntity.getVersion())));
    }
}
