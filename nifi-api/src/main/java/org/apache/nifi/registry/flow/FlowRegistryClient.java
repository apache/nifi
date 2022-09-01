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

import org.apache.nifi.components.ConfigurableComponent;

import java.io.IOException;
import java.util.Set;

public interface FlowRegistryClient extends ConfigurableComponent {
    void initialize(FlowRegistryClientInitializationContext context);

    Set<FlowRegistryBucket> getBuckets(FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException;
    FlowRegistryBucket getBucket(FlowRegistryClientConfigurationContext context, String bucketId) throws FlowRegistryException, IOException;

    RegisteredFlow registerFlow(FlowRegistryClientConfigurationContext context, RegisteredFlow flow) throws FlowRegistryException, IOException;
    RegisteredFlow deregisterFlow(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;

    RegisteredFlow getFlow(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;
    Set<RegisteredFlow> getFlows(FlowRegistryClientConfigurationContext context, String bucketId) throws FlowRegistryException, IOException;

    RegisteredFlowSnapshot getFlowContents(FlowRegistryClientConfigurationContext context, String bucketId, String flowId, int version) throws FlowRegistryException, IOException;
    RegisteredFlowSnapshot registerFlowSnapshot(FlowRegistryClientConfigurationContext context, RegisteredFlowSnapshot flowSnapshot) throws FlowRegistryException, IOException;

    Set<RegisteredFlowSnapshotMetadata> getFlowVersions(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;
    int getLatestVersion(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;
}
