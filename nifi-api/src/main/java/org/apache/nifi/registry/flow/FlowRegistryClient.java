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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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

    String DEFAULT_BRANCH_NAME = "main";

    void initialize(FlowRegistryClientInitializationContext context);

    /**
     * Decides if the given location is applicable by the repository instance. The format depends on the implementation.
     *
     * @param context Configuration context.
     * @param location The location of versioned flow to check.
     *
     * @return True in case of the given storage location is applicable, false otherwise. An applicable location does not
     * mean that the flow specified by the location is stored in the registry. Depending on the implementation it might be
     * merely a static check on the format of the location string. In case of uncertainty, NiFi tries to load the versioned
     * flow from registries with true return value first.
     */
    boolean isStorageLocationApplicable(FlowRegistryClientConfigurationContext context, String location);

    /**
     * Indicates if the registry supports branching.
     *
     * If the registry does not supporting branching, then this method should return false and the registry client should use
     * the default implementations of getBranches and getDefaultBranch.
     *
     * If the registry does support branching, then this method should return true and the registry client should use
     * override getBranches and getDefaultBranch to provide appropriate implementations.
     *
     * @param context Configuration context
     * @return true if the registry supports branching, false otherwise
     */
    default boolean isBranchingSupported(final FlowRegistryClientConfigurationContext context) {
        return false;
    }

    /**
     * Get the available branches. Should return at least one branch that matches the response of getDefaultBranch.
     *
     * @param context Configuration context
     * @return the set of available branches
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    default Set<FlowRegistryBranch> getBranches(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException {
        return Set.of(getDefaultBranch(context));
    }

    /**
     * Gets the default branch. Must return a non-null FlowRegistryBranch instance with a non-null name.
     * The interface provides a default implementation which will return a branch named 'main'.
     *
     * @param context Configuration context
     * @return the default branch
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    default FlowRegistryBranch getDefaultBranch(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException {
        final FlowRegistryBranch branch = new FlowRegistryBranch();
        branch.setName(DEFAULT_BRANCH_NAME);
        return branch;
    }

    /**
     * Gets the buckets for the specified user.
     *
     * @param context Configuration context.
     * @param branch the branch
     *
     * @return Buckets for this user. In case there are no available buckets for the user the result will be an empty set.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Set<FlowRegistryBucket> getBuckets(FlowRegistryClientConfigurationContext context, String branch) throws FlowRegistryException, IOException;

    /**
     * Gets the bucket with the given id.
     *
     * @param context Configuration context.
     * @param bucketLocation The location of the bucket.
     * @return The bucket with the given id.
     *
     * @throws NoSuchBucketException If there is no bucket in the Registry with the given id.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    FlowRegistryBucket getBucket(FlowRegistryClientConfigurationContext context, BucketLocation bucketLocation) throws FlowRegistryException, IOException;

    /**
     * Registers the given RegisteredFlow into the Flow Registry.
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
     * @param flowLocation the location of the flow
     *
     * @return The deleted Flow.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlow deregisterFlow(FlowRegistryClientConfigurationContext context, FlowLocation flowLocation) throws FlowRegistryException, IOException;

    /**
     * Retrieves a flow by bucket id and Flow id.
     *
     * @param context Configuration context.
     * @param flowLocation the location of the flow
     *
     * @return The Flow for the given bucket and Flow id's.
     *
     * @throws NoSuchFlowException If there is no Flow in the bucket with the given id.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlow getFlow(FlowRegistryClientConfigurationContext context, FlowLocation flowLocation) throws FlowRegistryException, IOException;

    /**
     * Retrieves the set of all Flows for the specified bucket.
     *
     * @param context Configuration context.
     * @param bucketLocation the location of the bucket
     *
     * @return The set of all Flows from the specified bucket. In case there are no available flows in the bucket the result will be an empty set.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Set<RegisteredFlow> getFlows(FlowRegistryClientConfigurationContext context, BucketLocation bucketLocation) throws FlowRegistryException, IOException;

    /**
     * Retrieves the contents of the flow snapshot with the given Bucket id, Flow id, and version, from the Registry.
     *
     * @param context Configuration context.
     * @param flowVersionLocation the location of the flow version
     *
     * @return The contents of the Flow from the Flow Registry.
     *
     * @throws NoSuchFlowVersionException If there is no version of the Flow with the given version number.
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlowSnapshot getFlowContents(FlowRegistryClientConfigurationContext context, FlowVersionLocation flowVersionLocation) throws FlowRegistryException, IOException;

    /**
     * Adds the given snapshot to the Flow Registry for the given Flow.
     *
     * @param context Configuration context.
     * @param flowSnapshot The flow snapshot to register.
     * @param action The register action
     *
     * @return The flow snapshot.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    RegisteredFlowSnapshot registerFlowSnapshot(FlowRegistryClientConfigurationContext context, RegisteredFlowSnapshot flowSnapshot, RegisterAction action) throws FlowRegistryException, IOException;

    /**
     * Retrieves the set of all versions of the specified flow.
     *
     * @param context Configuration context.
     * @param flowLocation the location of the flow
     *
     * @return The set of all versions of the specified flow. In case there are no available versions for the specified flow the result will be an empty set.
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Set<RegisteredFlowSnapshotMetadata> getFlowVersions(FlowRegistryClientConfigurationContext context, FlowLocation flowLocation) throws FlowRegistryException, IOException;

    /**
     * Returns the latest (most recent) version of the Flow in the Flow Registry for the given bucket and Flow.
     *
     * @param context Configuration context.
     * @param flowLocation the location of the flow
     *
     * @return an Optional containing the latest version of the flow, or empty if no versions exist for the flow
     *
     * @throws FlowRegistryException If an issue happens during processing the request.
     * @throws IOException If there is issue with the communication between NiFi and the Flow Registry.
     */
    Optional<String> getLatestVersion(FlowRegistryClientConfigurationContext context, FlowLocation flowLocation) throws FlowRegistryException, IOException;

    /**
     * Generates the id for registering a flow.
     *
     * @param flowName the name of the flow
     * @return the generated id
     */
    default String generateFlowId(final String flowName) {
        return UUID.randomUUID().toString();
    }

}
