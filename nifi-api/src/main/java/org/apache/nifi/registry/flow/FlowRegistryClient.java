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

/**
 * <p>
 * Represents and external source, where flows might be stored and retrieved from. The interface provides
 * the possibility to have multiple different types of registries backing NiFi.
 * </p>
 *
 <p>
 * <code>FlowRegistryClient</code>s are discovered using Java's
 * <code>ServiceLoader</code> mechanism. As a result, all implementations must
 * follow these rules:
 *
 * <ul>
 * <li>The implementation must implement this interface.</li>
 * <li>The implementation must have a file named
 * org.apache.nifi.registry.flow.FlowRegistryClient located within the jar's
 * <code>META-INF/services</code> directory. This file contains a list of
 * fully-qualified class names of all <code>FlowRegistryClient</code>s in the
 * jar, one-per-line.
 * <li>The implementation must support a default constructor.</li>
 * </ul>
 * </p>
 *
 * <p>
 * <code>FlowRegistryClient</code> instances are always considered "active" and approachable when the validation status is considered as valid.
 * Therefore after initialize, implementations must expect incoming requests any time.
 * </p>
 *
 * <p>
 * The argument list of the request methods contain instances <code>FlowRegistryClientConfigurationContext</code>, which always contains the current
 * state of the properties. Caching properties between method calls is not recommended
 * </p>
 *
 */
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
