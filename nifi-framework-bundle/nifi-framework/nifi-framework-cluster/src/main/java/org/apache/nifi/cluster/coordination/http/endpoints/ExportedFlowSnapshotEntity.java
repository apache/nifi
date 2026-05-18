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

package org.apache.nifi.cluster.coordination.http.endpoints;

import jakarta.xml.bind.annotation.XmlTransient;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.web.api.entity.Entity;

import java.util.Map;

/**
 * Internal {@link Entity} wrapper that allows {@link ExportProcessGroupEndpointMerger} to participate in the
 * {@code NodeResponse(NodeResponse, Entity)} merging contract while preserving the on-the-wire JSON format of
 * {@code GET /process-groups/{id}/download}.
 *
 * <p>NiFi's web layer configures Jackson with the JAXB annotation introspector only, so Jackson-only mechanisms
 * such as {@code @JsonValue} or {@code @JsonSerialize} are not honored. Instead, every property of the wrapped
 * {@link RegisteredFlowSnapshot} is exposed via a delegating getter so JAXB property discovery produces the same
 * JSON structure as serializing a {@code RegisteredFlowSnapshot} directly.
 */
class ExportedFlowSnapshotEntity extends Entity {

    private final RegisteredFlowSnapshot snapshot;

    ExportedFlowSnapshotEntity(final RegisteredFlowSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @XmlTransient
    public RegisteredFlowSnapshot getSnapshot() {
        return snapshot;
    }

    public RegisteredFlowSnapshotMetadata getSnapshotMetadata() {
        return snapshot.getSnapshotMetadata();
    }

    public RegisteredFlow getFlow() {
        return snapshot.getFlow();
    }

    public FlowRegistryBucket getBucket() {
        return snapshot.getBucket();
    }

    public VersionedProcessGroup getFlowContents() {
        return snapshot.getFlowContents();
    }

    public Map<String, ExternalControllerServiceReference> getExternalControllerServices() {
        return snapshot.getExternalControllerServices();
    }

    public Map<String, VersionedParameterContext> getParameterContexts() {
        return snapshot.getParameterContexts();
    }

    public String getFlowEncodingVersion() {
        return snapshot.getFlowEncodingVersion();
    }

    public Map<String, ParameterProviderReference> getParameterProviders() {
        return snapshot.getParameterProviders();
    }

    public boolean isLatest() {
        return snapshot.isLatest();
    }
}
