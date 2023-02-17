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
package org.apache.nifi.registry.db.migration;

import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestLegacyEntityMapper {

    @Test
    public void testMapLegacyEntities() {
        final BucketEntityV1 bucketEntityV1 = new BucketEntityV1();
        bucketEntityV1.setId("1");
        bucketEntityV1.setName("Bucket1");
        bucketEntityV1.setDescription("This is bucket 1");
        bucketEntityV1.setCreated(new Date());

        final BucketEntity bucketEntity = LegacyEntityMapper.createBucketEntity(bucketEntityV1);
        assertNotNull(bucketEntity);
        assertEquals(bucketEntityV1.getId(), bucketEntity.getId());
        assertEquals(bucketEntityV1.getName(), bucketEntity.getName());
        assertEquals(bucketEntityV1.getDescription(), bucketEntity.getDescription());
        assertEquals(bucketEntityV1.getCreated(), bucketEntity.getCreated());

        final FlowEntityV1 flowEntityV1 = new FlowEntityV1();
        flowEntityV1.setId("1");
        flowEntityV1.setBucketId(bucketEntityV1.getId());
        flowEntityV1.setName("Flow1");
        flowEntityV1.setDescription("This is flow1");
        flowEntityV1.setCreated(new Date());
        flowEntityV1.setModified(new Date());

        final FlowEntity flowEntity = LegacyEntityMapper.createFlowEntity(flowEntityV1);
        assertNotNull(flowEntity);
        assertEquals(flowEntityV1.getId(), flowEntity.getId());
        assertEquals(flowEntityV1.getBucketId(), flowEntity.getBucketId());
        assertEquals(flowEntityV1.getName(), flowEntity.getName());
        assertEquals(flowEntityV1.getDescription(), flowEntity.getDescription());
        assertEquals(flowEntityV1.getCreated(), flowEntity.getCreated());
        assertEquals(flowEntityV1.getModified(), flowEntity.getModified());

        final FlowSnapshotEntityV1 flowSnapshotEntityV1 = new FlowSnapshotEntityV1();
        flowSnapshotEntityV1.setFlowId(flowEntityV1.getId());
        flowSnapshotEntityV1.setVersion(1);
        flowSnapshotEntityV1.setComments("This is v1");
        flowSnapshotEntityV1.setCreated(new Date());
        flowSnapshotEntityV1.setCreatedBy("user1");

        final FlowSnapshotEntity flowSnapshotEntity = LegacyEntityMapper.createFlowSnapshotEntity(flowSnapshotEntityV1);
        assertNotNull(flowSnapshotEntity);
        assertEquals(flowSnapshotEntityV1.getFlowId(), flowSnapshotEntity.getFlowId());
        assertEquals(flowSnapshotEntityV1.getVersion(), flowSnapshotEntity.getVersion());
        assertEquals(flowSnapshotEntityV1.getComments(), flowSnapshotEntity.getComments());
        assertEquals(flowSnapshotEntityV1.getCreatedBy(), flowSnapshotEntity.getCreatedBy());
        assertEquals(flowSnapshotEntityV1.getCreated(), flowSnapshotEntity.getCreated());
    }

}
