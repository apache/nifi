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
package org.apache.nifi.registry.flow;

import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

final class NifiRegistryUtil {
    NifiRegistryUtil() {
        // Not to be instantiated
    }

    static FlowRegistryBucket convert(final Bucket bucket) {
        FlowRegistryBucket result = new FlowRegistryBucket();
        result.setIdentifier(bucket.getIdentifier());
        result.setName(bucket.getName());
        result.setDescription(bucket.getDescription());
        result.setCreatedTimestamp(bucket.getCreatedTimestamp());
        result.setPermissions(convert(bucket.getPermissions()));
        return result;
    }

    static Bucket convert(final FlowRegistryBucket bucket) {
        if (bucket == null) {
            return null;
        }

        Bucket result = new Bucket();
        result.setIdentifier(bucket.getIdentifier());
        result.setName(bucket.getName());
        result.setDescription(bucket.getDescription());
        result.setCreatedTimestamp(bucket.getCreatedTimestamp());
        result.setPermissions(convert(bucket.getPermissions()));
        return result;
    }

    static FlowRegistryPermissions convert(final Permissions permissions) {
        if (permissions == null) {
            return null;
        }

        final FlowRegistryPermissions result = new FlowRegistryPermissions();
        result.setCanRead(permissions.getCanRead());
        result.setCanWrite(permissions.getCanWrite());
        result.setCanDelete(permissions.getCanDelete());
        return result;
    }

    static Permissions convert(FlowRegistryPermissions permissions) {
        if (permissions == null) {
            return null;
        }

        final Permissions result = new Permissions();
        result.setCanRead(permissions.getCanRead());
        result.setCanWrite(permissions.getCanWrite());
        result.setCanDelete(permissions.getCanDelete());
        return result;
    }

    static RegisteredFlowSnapshotMetadata convert(final VersionedFlowSnapshotMetadata metadata) {
        final RegisteredFlowSnapshotMetadata result = new RegisteredFlowSnapshotMetadata();
        result.setBucketIdentifier(metadata.getBucketIdentifier());
        result.setFlowIdentifier(metadata.getFlowIdentifier());
        result.setVersion(String.valueOf(metadata.getVersion()));
        result.setTimestamp(metadata.getTimestamp());
        result.setAuthor(metadata.getAuthor());
        result.setComments(metadata.getComments());
        return result;
    }

    static VersionedFlowSnapshotMetadata convert(final RegisteredFlowSnapshotMetadata metadata) {
        final VersionedFlowSnapshotMetadata result = new VersionedFlowSnapshotMetadata();
        result.setBucketIdentifier(metadata.getBucketIdentifier());
        result.setFlowIdentifier(metadata.getFlowIdentifier());
        result.setVersion(Integer.parseInt(metadata.getVersion()));
        result.setTimestamp(metadata.getTimestamp());
        result.setAuthor(metadata.getAuthor());
        result.setComments(metadata.getComments());
        return result;
    }

    static RegisteredFlowVersionInfo convert(final RevisionInfo revisionInfo) {
        if (revisionInfo == null) {
            return null;
        }

        // Some of the fields are not used in NiFi
        final RegisteredFlowVersionInfo result = new RegisteredFlowVersionInfo();
        result.setVersion(revisionInfo.getVersion());
        return result;
    }

    static RevisionInfo convert(final RegisteredFlowVersionInfo versionInfo) {
        if (versionInfo == null) {
            return null;
        }

        final RevisionInfo result = new RevisionInfo();
        result.setVersion(versionInfo.getVersion());
        return result;
    }

    static RegisteredFlow convert(final VersionedFlow flow) {
        RegisteredFlow result = new RegisteredFlow();
        result.setIdentifier(flow.getIdentifier());
        result.setName(flow.getName());
        result.setDescription(flow.getDescription());
        result.setBucketIdentifier(flow.getBucketIdentifier());
        result.setBucketName(flow.getBucketName());
        result.setCreatedTimestamp(flow.getCreatedTimestamp());
        result.setLastModifiedTimestamp(flow.getModifiedTimestamp());
        result.setPermissions(convert(flow.getPermissions()));
        result.setVersionCount(flow.getVersionCount());
        result.setVersionInfo(convert(flow.getRevision()));
        return result;
    }

    static VersionedFlow convert(final RegisteredFlow flow) {
        if (flow == null) {
            return null;
        }

        final VersionedFlow result = new VersionedFlow();
        result.setIdentifier(flow.getIdentifier());
        result.setName(flow.getName());
        result.setDescription(flow.getDescription());
        result.setBucketIdentifier(flow.getBucketIdentifier());
        result.setBucketName(flow.getBucketName());
        result.setCreatedTimestamp(flow.getCreatedTimestamp());
        result.setModifiedTimestamp(flow.getLastModifiedTimestamp());
        result.setPermissions(convert(flow.getPermissions()));
        result.setVersionCount(flow.getVersionCount());
        result.setRevision(convert(flow.getVersionInfo()));
        return result;
    }

    static RegisteredFlowSnapshot convert(final VersionedFlowSnapshot flowSnapshot) {
        RegisteredFlowSnapshot result = new RegisteredFlowSnapshot();
        result.setSnapshotMetadata(convert(flowSnapshot.getSnapshotMetadata()));
        result.setFlow(convert(flowSnapshot.getFlow()));
        result.setBucket(convert(flowSnapshot.getBucket()));
        result.setFlowContents(flowSnapshot.getFlowContents());
        result.setExternalControllerServices(flowSnapshot.getExternalControllerServices());
        result.setParameterContexts(flowSnapshot.getParameterContexts());
        result.setFlowEncodingVersion(flowSnapshot.getFlowEncodingVersion());
        result.setParameterProviders(flowSnapshot.getParameterProviders());
        return result;
    }

    static VersionedFlowSnapshot convert(final RegisteredFlowSnapshot flowSnapshot) {
        VersionedFlowSnapshot result = new VersionedFlowSnapshot();
        result.setSnapshotMetadata(convert(flowSnapshot.getSnapshotMetadata()));
        result.setFlow(convert(flowSnapshot.getFlow()));
        result.setBucket(convert(flowSnapshot.getBucket()));
        result.setFlowContents(flowSnapshot.getFlowContents());
        result.setExternalControllerServices(flowSnapshot.getExternalControllerServices());
        result.setParameterContexts(flowSnapshot.getParameterContexts());
        result.setFlowEncodingVersion(flowSnapshot.getFlowEncodingVersion());
        result.setParameterProviders(flowSnapshot.getParameterProviders());
        return result;
    }
}
