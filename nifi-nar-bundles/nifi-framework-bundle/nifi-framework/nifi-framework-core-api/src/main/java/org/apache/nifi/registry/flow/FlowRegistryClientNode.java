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

import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface FlowRegistryClientNode extends ComponentNode {

    String getDescription();

    void setDescription(String description);

    boolean isStorageLocationApplicable(String location);

    Set<FlowRegistryBucket> getBuckets(FlowRegistryClientUserContext context) throws FlowRegistryException, IOException;
    FlowRegistryBucket getBucket(FlowRegistryClientUserContext context, String bucketId) throws FlowRegistryException, IOException;

    RegisteredFlow registerFlow(FlowRegistryClientUserContext context, RegisteredFlow flow) throws FlowRegistryException, IOException;
    RegisteredFlow deregisterFlow(FlowRegistryClientUserContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;

    RegisteredFlow getFlow(FlowRegistryClientUserContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;
    Set<RegisteredFlow> getFlows(FlowRegistryClientUserContext context, String bucketId) throws FlowRegistryException, IOException;

    FlowSnapshotContainer getFlowContents(FlowRegistryClientUserContext context, String bucketId, String flowId, int version, boolean fetchRemoteFlows) throws FlowRegistryException, IOException;
    RegisteredFlowSnapshot registerFlowSnapshot(
            FlowRegistryClientUserContext context,
            RegisteredFlow flow,
            VersionedProcessGroup snapshot,
            Map<String, ExternalControllerServiceReference> externalControllerServices,
            Map<String, VersionedParameterContext> parameterContexts,
            Map<String, ParameterProviderReference> parameterProviderReferences, String comments,
            int expectedVersion
    ) throws FlowRegistryException, IOException;

    Set<RegisteredFlowSnapshotMetadata> getFlowVersions(FlowRegistryClientUserContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;
    int getLatestVersion(FlowRegistryClientUserContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;

    void setComponent(LoggableComponent<FlowRegistryClient> component);
}
