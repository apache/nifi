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

import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Map;

public class RegisteredFlowSnapshot {
    private RegisteredFlowSnapshotMetadata snapshotMetadata;
    private RegisteredFlow flow;
    private FlowRegistryBucket bucket;
    private VersionedProcessGroup flowContents;
    private Map<String, ExternalControllerServiceReference> externalControllerServices;
    private Map<String, VersionedParameterContext> parameterContexts;
    private String flowEncodingVersion;
    private Map<String, ParameterProviderReference> parameterProviders;
    private boolean latest;

    public RegisteredFlowSnapshotMetadata getSnapshotMetadata() {
        return snapshotMetadata;
    }

    public RegisteredFlow getFlow() {
        return flow;
    }

    public FlowRegistryBucket getBucket() {
        return bucket;
    }

    public VersionedProcessGroup getFlowContents() {
        return flowContents;
    }

    public Map<String, ExternalControllerServiceReference> getExternalControllerServices() {
        return externalControllerServices;
    }

    public Map<String, VersionedParameterContext> getParameterContexts() {
        return parameterContexts;
    }

    public String getFlowEncodingVersion() {
        return flowEncodingVersion;
    }

    public boolean isLatest() {
        return latest;
    }

    public void setSnapshotMetadata(final RegisteredFlowSnapshotMetadata snapshotMetadata) {
        this.snapshotMetadata = snapshotMetadata;
    }

    public void setFlow(RegisteredFlow flow) {
        this.flow = flow;
    }

    public void setBucket(FlowRegistryBucket bucket) {
        this.bucket = bucket;
    }

    public void setFlowContents(VersionedProcessGroup flowContents) {
        this.flowContents = flowContents;
    }

    public void setExternalControllerServices(final Map<String, ExternalControllerServiceReference> externalControllerServices) {
        this.externalControllerServices = externalControllerServices;
    }

    public void setParameterContexts(final Map<String, VersionedParameterContext> parameterContexts) {
        this.parameterContexts = parameterContexts;
    }

    public void setFlowEncodingVersion(String flowEncodingVersion) {
        this.flowEncodingVersion = flowEncodingVersion;
    }

    public Map<String, ParameterProviderReference> getParameterProviders() {
        return parameterProviders;
    }

    public void setParameterProviders(final Map<String, ParameterProviderReference> parameterProviders) {
        this.parameterProviders = parameterProviders;
    }

    public void setLatest(final boolean latest) {
        this.latest = latest;
    }
}
