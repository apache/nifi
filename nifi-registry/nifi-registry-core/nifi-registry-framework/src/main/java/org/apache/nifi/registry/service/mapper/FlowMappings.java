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
package org.apache.nifi.registry.service.mapper;

import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.diff.ComponentDifference;
import org.apache.nifi.registry.diff.ComponentDifferenceGroup;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.diff.FlowDifference;

import java.util.Date;

/**
 * Mappings between flow related DB entities and data model.
 */
public class FlowMappings {

    // --- Map flows

    public static FlowEntity map(final VersionedFlow versionedFlow) {
        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setId(versionedFlow.getIdentifier());
        flowEntity.setName(versionedFlow.getName());
        flowEntity.setDescription(versionedFlow.getDescription());
        flowEntity.setCreated(new Date(versionedFlow.getCreatedTimestamp()));
        flowEntity.setModified(new Date(versionedFlow.getModifiedTimestamp()));
        flowEntity.setType(BucketItemEntityType.FLOW);
        return flowEntity;
    }

    public static VersionedFlow map(final BucketEntity bucketEntity, final FlowEntity flowEntity) {
        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setIdentifier(flowEntity.getId());
        versionedFlow.setBucketIdentifier(flowEntity.getBucketId());
        versionedFlow.setName(flowEntity.getName());
        versionedFlow.setDescription(flowEntity.getDescription());
        versionedFlow.setCreatedTimestamp(flowEntity.getCreated().getTime());
        versionedFlow.setModifiedTimestamp(flowEntity.getModified().getTime());
        versionedFlow.setVersionCount(flowEntity.getSnapshotCount());

        if (bucketEntity != null) {
            versionedFlow.setBucketName(bucketEntity.getName());
        } else {
            versionedFlow.setBucketName(flowEntity.getBucketName());
        }

        return versionedFlow;
    }

    // --- Map snapshots

    public static FlowSnapshotEntity map(final VersionedFlowSnapshotMetadata versionedFlowSnapshot) {
        final FlowSnapshotEntity flowSnapshotEntity = new FlowSnapshotEntity();
        flowSnapshotEntity.setFlowId(versionedFlowSnapshot.getFlowIdentifier());
        flowSnapshotEntity.setVersion(versionedFlowSnapshot.getVersion());
        flowSnapshotEntity.setComments(versionedFlowSnapshot.getComments());
        flowSnapshotEntity.setCreated(new Date(versionedFlowSnapshot.getTimestamp()));
        flowSnapshotEntity.setCreatedBy(versionedFlowSnapshot.getAuthor());
        return flowSnapshotEntity;
    }

    public static VersionedFlowSnapshotMetadata map(final BucketEntity bucketEntity, final FlowSnapshotEntity flowSnapshotEntity) {
        final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setFlowIdentifier(flowSnapshotEntity.getFlowId());
        metadata.setVersion(flowSnapshotEntity.getVersion());
        metadata.setComments(flowSnapshotEntity.getComments());
        metadata.setTimestamp(flowSnapshotEntity.getCreated().getTime());
        metadata.setAuthor(flowSnapshotEntity.getCreatedBy());

        if (bucketEntity != null) {
            metadata.setBucketIdentifier(bucketEntity.getId());
        }

        return metadata;
    }

    // --- Flow Differences

    public static ComponentDifference map(final FlowDifference flowDifference){
        ComponentDifference diff = new ComponentDifference();
        diff.setChangeDescription(flowDifference.getDescription());
        diff.setDifferenceType(flowDifference.getDifferenceType().toString());
        diff.setDifferenceTypeDescription(flowDifference.getDifferenceType().getDescription());
        diff.setValueA(getValueDescription(flowDifference.getValueA()));
        diff.setValueB(getValueDescription(flowDifference.getValueB()));
        return diff;
    }

    public static ComponentDifferenceGroup map(VersionedComponent versionedComponent){
        ComponentDifferenceGroup grouping = new ComponentDifferenceGroup();
        grouping.setComponentId(versionedComponent.getIdentifier());
        grouping.setComponentName(versionedComponent.getName());
        grouping.setProcessGroupId(versionedComponent.getGroupIdentifier());
        grouping.setComponentType(versionedComponent.getComponentType().getTypeName());
        return grouping;
    }

    private static String getValueDescription(Object valueA){
        if(valueA instanceof VersionedComponent){
            return ((VersionedComponent) valueA).getIdentifier();
        }
        if(valueA!= null){
            return valueA.toString();
        }
        return null;
    }

}
