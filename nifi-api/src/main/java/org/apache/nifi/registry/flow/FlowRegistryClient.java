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
 * state of the properties. Caching properties between method calls is not recommended.
 * </p>
 *
 */
public interface FlowRegistryClient extends ConfigurableComponent {
    void initialize(FlowRegistryClientInitializationContext context);

    /**
     * Decides if the given location is applicable by the repository instance. The format depends on the implementation.
     *
     * @param context Configuration context.
     * @param location The location to check.
     *
     * @return True in case of the given storage location is applicable, false otherwise.
     */
    boolean isStorageLocationApplicable(FlowRegistryClientConfigurationContext context, String location);

    /**
     * Gets the buckets for the specified user.
     *
     * @param context Configuration context.
     *
     * @return Buckets for this user.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Set<FlowRegistryBucket> getBuckets(FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException;

    /**
     * Gets the bucket with the given id.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     * @return The bucket with the given id.
     *
     * @throws NoSuchBucketException If there is no bucket in the Registry with the given id.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    FlowRegistryBucket getBucket(FlowRegistryClientConfigurationContext context, String bucketId) throws FlowRegistryException, IOException;

    /**
     * Registers the given RegisteredFlow into the the Flow Registry.
     *
     * @param context Configuration context.
     * @param flow The RegisteredFlow to add to the Registry.
     *
     * @return The fully populated RegisteredFlow
     *
     * @throws FlowAlreadyExistsException If a Flow with the given identifier already exists in the registry.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlow registerFlow(FlowRegistryClientConfigurationContext context, RegisteredFlow flow) throws FlowRegistryException, IOException;


    /**
     * Deletes the specified flow from the Flow Registry.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     * @param flowId The id of the flow.
     *
     * @return The deleted Flow.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlow deregisterFlow(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;


    /**
     * Retrieves a flow by bucket id and Flow id.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     * @param flowId The id of the Flow.
     *
     * @return The Flow for the given bucket and Flow id's.
     *
     * @throws NoSuchFlowException If there is no Flow in the bucket with the given id.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlow getFlow(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;

    /**
     * Retrieves the set of all Flows for the specified bucket.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     *
     * @return The set of all Flows from the specified bucket.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Set<RegisteredFlow> getFlows(FlowRegistryClientConfigurationContext context, String bucketId) throws FlowRegistryException, IOException;

    /**
     * Retrieves the contents of the flow with the given Bucket id, Flow id, and version, from the Registry.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     * @param flowId The id of the Flow.
     * @param version The version to retrieve.
     *
     * @return The contents of the Flow from the Flow Registry.
     *
     * @throws NoSuchFlowVersionException If there is no version of the Flow with the given version number.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlowSnapshot getFlowContents(FlowRegistryClientConfigurationContext context, String bucketId, String flowId, int version) throws FlowRegistryException, IOException;

    /**
     * Adds the given snapshot to the Flow Registry for the given Flow.
     *
     * @param context Configuration context.
     * @param flowSnapshot The flow snapshot to register.
     *
     * @return The flow snapshot.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlowSnapshot registerFlowSnapshot(FlowRegistryClientConfigurationContext context, RegisteredFlowSnapshot flowSnapshot) throws FlowRegistryException, IOException;


    /**
     * Retrieves the set of all versions of the specified flow.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     * @param flowId The id of the flow.
     *
     * @return The set of all versions of the specified flow.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Set<RegisteredFlowSnapshotMetadata> getFlowVersions(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;

    /**
     * Returns the latest (most recent) version of the Flow in the Flow Registry for the given bucket and Flow.
     *
     * @param context Configuration context.
     * @param bucketId The id of the bucket.
     * @param flowId The id of the flow.
     *
     * @return The latest version of the Flow.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    int getLatestVersion(FlowRegistryClientConfigurationContext context, String bucketId, String flowId) throws FlowRegistryException, IOException;
}
