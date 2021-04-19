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
package org.apache.nifi.registry.web.service;

import org.apache.nifi.registry.RegistryConfiguration;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.authorization.Resource;
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.registry.extension.repo.ExtensionRepoBucket;
import org.apache.nifi.registry.extension.repo.ExtensionRepoExtensionMetadata;
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersion;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.security.authorization.RequestAction;

import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

public interface ServiceFacade {

    // ---------------------- Bucket methods ----------------------------------------------

    Bucket createBucket(Bucket bucket);

    Bucket getBucket(String bucketIdentifier);

    List<Bucket> getBuckets();

    Bucket updateBucket(Bucket bucket);

    Bucket deleteBucket(String bucketIdentifier, RevisionInfo revisionInfo);

    // ---------------------- BucketItem methods ----------------------------------------------

    List<BucketItem> getBucketItems(String bucketIdentifier);

    List<BucketItem> getBucketItems();

    // ---------------------- Flow methods ----------------------------------------------

    VersionedFlow createFlow(String bucketIdentifier, VersionedFlow versionedFlow);

    VersionedFlow getFlow(String bucketIdentifier, String flowIdentifier);

    VersionedFlow getFlow(String flowIdentifier);

    List<VersionedFlow> getFlows(String bucketId);

    VersionedFlow updateFlow(VersionedFlow versionedFlow);

    VersionedFlow deleteFlow(String bucketIdentifier, String flowIdentifier, RevisionInfo revisionInfo);

    // ---------------------- Flow Snapshot methods ----------------------------------------------

    VersionedFlowSnapshot createFlowSnapshot(VersionedFlowSnapshot flowSnapshot);

    VersionedFlowSnapshot getFlowSnapshot(String bucketIdentifier, String flowIdentifier, Integer version);

    VersionedFlowSnapshot getFlowSnapshot(String flowIdentifier, Integer version);

    VersionedFlowSnapshot getLatestFlowSnapshot(String bucketIdentifier, String flowIdentifier);

    VersionedFlowSnapshot getLatestFlowSnapshot(String flowIdentifier);

    VersionedFlowSnapshot importVersionedFlowSnapshot(VersionedFlowSnapshot versionedFlowSnapshot, String bucketIdentifier, String flowIdentifier, String comments);

    ExportedVersionedFlowSnapshot exportFlowSnapshot(String bucketIdentifier, String flowIdentifier, Integer versionNumber);

    SortedSet<VersionedFlowSnapshotMetadata> getFlowSnapshots(String bucketIdentifier, String flowIdentifier);

    SortedSet<VersionedFlowSnapshotMetadata> getFlowSnapshots(String flowIdentifier);

    VersionedFlowSnapshotMetadata getLatestFlowSnapshotMetadata(String bucketIdentifier, String flowIdentifier);

    VersionedFlowSnapshotMetadata getLatestFlowSnapshotMetadata(String flowIdentifier);

    VersionedFlowDifference getFlowDiff(String bucketIdentifier, String flowIdentifier, Integer versionA, Integer versionB);

    // ---------------------- Bundle methods ----------------------------------------------

    List<Bundle> getBundles(BundleFilterParams filterParams);

    List<Bundle> getBundlesByBucket(String bucketIdentifier);

    Bundle getBundle(String bundleIdentifier);

    Bundle deleteBundle(String bundleIdentifier);

    // ---------------------- Bundle Version methods ----------------------------------------------

    BundleVersion createBundleVersion(String bucketIdentifier, BundleType bundleType, InputStream inputStream, String clientSha256) throws IOException;

    SortedSet<BundleVersionMetadata> getBundleVersions(BundleVersionFilterParams filterParams);

    SortedSet<BundleVersionMetadata> getBundleVersions(String bundleIdentifier);

    BundleVersion getBundleVersion(String bundleId, String version);

    StreamingContent getBundleVersionContent(String bundleId, String version);

    BundleVersion deleteBundleVersion(String bundleId, String version);

    // ---------------------- Extension methods ----------------------------------------------

    SortedSet<ExtensionMetadata> getExtensionMetadata(ExtensionFilterParams filterParams);

    SortedSet<ExtensionMetadata> getExtensionMetadata(ProvidedServiceAPI serviceAPI);

    SortedSet<ExtensionMetadata> getExtensionMetadata(String bundleIdentifier, String version);

    Extension getExtension(String bundleIdentifier, String version, String name);

    StreamingOutput getExtensionDocs(String bundleIdentifier, String version, String name);

    StreamingOutput getAdditionalDetailsDocs(String bundleIdentifier, String version, String name);

    SortedSet<TagCount> getExtensionTags();

    // ---------------------- Extension Repository methods ----------------------------------------------

    SortedSet<ExtensionRepoBucket> getExtensionRepoBuckets(URI baseUri);

    SortedSet<ExtensionRepoGroup> getExtensionRepoGroups(URI baseUri, String bucketName);

    SortedSet<ExtensionRepoArtifact> getExtensionRepoArtifacts(URI baseUri, String bucketName, String groupId);

    SortedSet<ExtensionRepoVersionSummary> getExtensionRepoVersions(URI baseUri, String bucketName, String groupId, String artifactId);

    ExtensionRepoVersion getExtensionRepoVersion(URI baseUri, String bucketName, String groupId, String artifactId, String version);

    StreamingContent getExtensionRepoVersionContent(String bucketName, String groupId, String artifactId, String version);

    String getExtensionRepoVersionSha256(String bucketName, String groupId, String artifactId, String version);

    List<ExtensionRepoExtensionMetadata> getExtensionRepoExtensions(URI baseUri, String bucketName, String groupId, String artifactId, String version);

    Extension getExtensionRepoExtension(URI baseUri, String bucketName, String groupId, String artifactId, String version, String extensionName);

    StreamingOutput getExtensionRepoExtensionDocs(URI baseUri, String bucketName, String groupId, String artifactId, String version, String extensionName);

    StreamingOutput getExtensionRepoExtensionAdditionalDocs(URI baseUri, String bucketName, String groupId, String artifactId, String version, String extensionName);

    // ---------------------- Field methods ---------------------------------------------

    Set<String> getBucketFields();

    Set<String> getBucketItemFields();

    Set<String> getFlowFields();

    // ---------------------- User methods ----------------------------------------------

    User createUser(User user);

    List<User> getUsers();

    User getUser(String identifier);

    User updateUser(User user);

    User deleteUser(String identifier, RevisionInfo revisionInfo);

    // ---------------------- User Group methods --------------------------------------

    UserGroup createUserGroup(UserGroup userGroup);

    List<UserGroup> getUserGroups();

    UserGroup getUserGroup(String identifier);

    UserGroup updateUserGroup(UserGroup userGroup);

    UserGroup deleteUserGroup(String identifier, RevisionInfo revisionInfo);

    // ---------------------- Access Policy methods ----------------------------------------

    AccessPolicy createAccessPolicy(AccessPolicy accessPolicy);

    AccessPolicy getAccessPolicy(String identifier);

    AccessPolicy getAccessPolicy(String resource, RequestAction action);

    List<AccessPolicy> getAccessPolicies();

    AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy);

    AccessPolicy deleteAccessPolicy(String identifier, RevisionInfo revisionInfo);

    List<Resource> getResources();

    // ---------------------- Permission methods ----------------------------------------

    CurrentUser getCurrentUser();

    // ----------------------  Configuration methods ------------------------------------

    RegistryConfiguration getRegistryConfiguration();

}
