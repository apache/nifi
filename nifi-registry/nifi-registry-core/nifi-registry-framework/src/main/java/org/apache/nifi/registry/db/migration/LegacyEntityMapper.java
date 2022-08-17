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
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;

/**
 * Utility methods to map legacy DB entities to current DB entities.
 *
 * The initial implementations of these mappings will be almost a direct translation, but if future changes are made
 * to the original tables these methods will handle the translation from old entity to new entity.
 */
public class LegacyEntityMapper {

    public static BucketEntity createBucketEntity(final BucketEntityV1 bucketEntityV1) {
        final BucketEntity bucketEntity = new BucketEntity();
        bucketEntity.setId(bucketEntityV1.getId());
        bucketEntity.setName(bucketEntityV1.getName());
        bucketEntity.setDescription(bucketEntityV1.getDescription());
        bucketEntity.setCreated(bucketEntityV1.getCreated());
        return bucketEntity;
    }

    public static FlowEntity createFlowEntity(final FlowEntityV1 flowEntityV1) {
        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setId(flowEntityV1.getId());
        flowEntity.setName(flowEntityV1.getName());
        flowEntity.setDescription(flowEntityV1.getDescription());
        flowEntity.setCreated(flowEntityV1.getCreated());
        flowEntity.setModified(flowEntityV1.getModified());
        flowEntity.setBucketId(flowEntityV1.getBucketId());
        flowEntity.setType(BucketItemEntityType.FLOW);
        return flowEntity;
    }

    public static FlowSnapshotEntity createFlowSnapshotEntity(final FlowSnapshotEntityV1 flowSnapshotEntityV1) {
        final FlowSnapshotEntity flowSnapshotEntity = new FlowSnapshotEntity();
        flowSnapshotEntity.setFlowId(flowSnapshotEntityV1.getFlowId());
        flowSnapshotEntity.setVersion(flowSnapshotEntityV1.getVersion());
        flowSnapshotEntity.setComments(flowSnapshotEntityV1.getComments());
        flowSnapshotEntity.setCreated(flowSnapshotEntityV1.getCreated());
        flowSnapshotEntity.setCreatedBy(flowSnapshotEntityV1.getCreatedBy());
        return flowSnapshotEntity;
    }

}
