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

import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.registry.revision.api.InvalidRevisionException;
import org.apache.nifi.registry.revision.entity.RevisableEntity;
import org.apache.nifi.registry.revision.entity.RevisableEntityService;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.security.authorization.AuthorizableLookup;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.resource.Authorizable;
import org.apache.nifi.registry.security.authorization.resource.ResourceType;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.service.AuthorizationService;
import org.apache.nifi.registry.service.RegistryService;
import org.apache.nifi.registry.service.extension.ExtensionService;
import org.apache.nifi.registry.web.link.LinkService;
import org.apache.nifi.registry.web.security.PermissionsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.ws.rs.core.Link;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A wrapper around the service layer that applies validation, authorization, and revision management to all services.
 *
 * All REST resources should access the service layer through this facade.
 */
@Service
@Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Throwable.class)
public class StandardServiceFacade implements ServiceFacade {

    private static final String INVALID_REVISION_MSG = "The %s you attempted to %s with id '%s' is out of date with the server. " +
            "You may need to refresh your client and try again.";

    public static final String USER_GROUP_ENTITY_TYPE = "User Group";
    public static final String USER_ENTITY_TYPE = "User";
    public static final String ACCESS_POLICY_ENTITY_TYPE = "Access Policy";
    public static final String VERSIONED_FLOW_ENTITY_TYPE = "Versioned Flow";
    public static final String BUCKET_ENTITY_TYPE = "Bucket";

    private final RegistryService registryService;
    private final ExtensionService extensionService;
    private final AuthorizationService authorizationService;
    private final AuthorizableLookup authorizableLookup;
    private final RevisableEntityService entityService;
    private final RevisionFeature revisionFeature;
    private final PermissionsService permissionsService;
    private final LinkService linkService;

    private static final int LATEST_VERSION = -1;

    @Autowired
    public StandardServiceFacade(final RegistryService registryService,
                                 final ExtensionService extensionService,
                                 final AuthorizationService authorizationService,
                                 final AuthorizableLookup authorizableLookup,
                                 final RevisableEntityService entityService,
                                 final RevisionFeature revisionFeature,
                                 final PermissionsService permissionsService,
                                 final LinkService linkService) {
        this.registryService = registryService;
        this.extensionService = extensionService;
        this.authorizationService = authorizationService;
        this.authorizableLookup = authorizableLookup;
        this.entityService = entityService;
        this.revisionFeature = revisionFeature;
        this.permissionsService = permissionsService;
        this.linkService = linkService;
    }

    private String currentUserIdentity() {
        return NiFiUserUtils.getNiFiUserIdentity();
    }

    // ---------------------- Bucket methods ----------------------------------------------

    @Override
    public Bucket createBucket(final Bucket bucket) {
        authorizeBucketsAccess(RequestAction.WRITE);
        validateCreationOfRevisableEntity(bucket, BUCKET_ENTITY_TYPE);
        validateIdentifierNotPresent(bucket, BUCKET_ENTITY_TYPE);

        bucket.setIdentifier(UUID.randomUUID().toString());

        final Bucket createdBucket = createRevisableEntity(bucket, BUCKET_ENTITY_TYPE, currentUserIdentity(),
                () -> registryService.createBucket(bucket));
        permissionsService.populateBucketPermissions(createdBucket);
        linkService.populateLinks(createdBucket);
        return createdBucket;
    }

    @Override
    public Bucket getBucket(final String bucketIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final Bucket bucket = entityService.get(() -> registryService.getBucket(bucketIdentifier));
        permissionsService.populateBucketPermissions(bucket);
        linkService.populateLinks(bucket);
        return bucket;
    }

    @Override
    public List<Bucket> getBuckets() {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return Collections.emptyList();
        }

        final List<Bucket> buckets = entityService.getEntities(() -> registryService.getBuckets(authorizedBucketIds));
        permissionsService.populateBucketPermissions(buckets);
        linkService.populateLinks(buckets);
        return buckets;
    }

    @Override
    public Bucket updateBucket(final Bucket bucket) {
        authorizeBucketAccess(RequestAction.WRITE, bucket.getIdentifier());
        validateUpdateOfRevisableEntity(bucket, BUCKET_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        registryService.verifyBucketExists(bucket.getIdentifier());

        final Bucket updatedBucket = updateRevisableEntity(bucket, BUCKET_ENTITY_TYPE, currentUserIdentity(),
                () -> registryService.updateBucket(bucket));
        permissionsService.populateBucketPermissions(updatedBucket);
        linkService.populateLinks(updatedBucket);
        return  updatedBucket;
    }

    @Override
    public Bucket deleteBucket(final String bucketIdentifier, final RevisionInfo revisionInfo) {
        authorizeBucketAccess(RequestAction.DELETE, bucketIdentifier);
        validateDeleteOfRevisableEntity(bucketIdentifier, revisionInfo, BUCKET_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        registryService.verifyBucketExists(bucketIdentifier);

        return deleteRevisableEntity(bucketIdentifier, BUCKET_ENTITY_TYPE, revisionInfo,
                () -> registryService.deleteBucket(bucketIdentifier));
    }

    // ---------------------- BucketItem methods ----------------------------------------------

    @Override
    public List<BucketItem> getBucketItems(final String bucketIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final List<BucketItem> items = registryService.getBucketItems(bucketIdentifier);
        entityService.populateRevisions(items);
        permissionsService.populateItemPermissions(items);
        linkService.populateLinks(items);
        return items;
    }

    @Override
    public List<BucketItem> getBucketItems() {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return new ArrayList<>();
        }

        final List<BucketItem> items = registryService.getBucketItems(authorizedBucketIds);
        entityService.populateRevisions(items);
        permissionsService.populateItemPermissions(items);
        linkService.populateLinks(items);
        return items;
    }

    // ---------------------- Flow methods ----------------------------------------------

    @Override
    public VersionedFlow createFlow(final String bucketIdentifier, final VersionedFlow versionedFlow) {
        authorizeBucketAccess(RequestAction.WRITE, bucketIdentifier);
        validateCreationOfRevisableEntity(versionedFlow, VERSIONED_FLOW_ENTITY_TYPE);

        // NOTE: Don't validate that identifier is null...
        // NiFi has been sending an identifier, so we must maintain backwards compatibility
        if (versionedFlow.getIdentifier() == null) {
            versionedFlow.setIdentifier(UUID.randomUUID().toString());
        }

        final VersionedFlow createdFlow = createRevisableEntity(versionedFlow, VERSIONED_FLOW_ENTITY_TYPE, currentUserIdentity(),
                () -> registryService.createFlow(bucketIdentifier, versionedFlow));
        permissionsService.populateItemPermissions(createdFlow);
        linkService.populateLinks(createdFlow);
        return createdFlow;
    }

    @Override
    public VersionedFlow getFlow(final String bucketIdentifier, final String flowIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final VersionedFlow flow = entityService.get(
                () -> registryService.getFlow(bucketIdentifier, flowIdentifier));
        permissionsService.populateItemPermissions(flow);
        linkService.populateLinks(flow);
        return flow;
    }

    @Override
    public VersionedFlow getFlow(final String flowIdentifier) {
        final VersionedFlow flow =  entityService.get(() -> registryService.getFlow(flowIdentifier));
        authorizeBucketAccess(RequestAction.READ, flow);

        permissionsService.populateItemPermissions(flow);
        linkService.populateLinks(flow);
        return flow;
    }

    @Override
    public List<VersionedFlow> getFlows(final String bucketIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final List<VersionedFlow> flows = entityService.getEntities(() -> registryService.getFlows(bucketIdentifier));
        permissionsService.populateItemPermissions(flows);
        linkService.populateLinks(flows);
        return flows;
    }

    @Override
    public VersionedFlow updateFlow(final VersionedFlow versionedFlow) {
        authorizeBucketAccess(RequestAction.WRITE, versionedFlow);
        validateUpdateOfRevisableEntity(versionedFlow, VERSIONED_FLOW_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        registryService.verifyFlowExists(versionedFlow.getIdentifier());

        final VersionedFlow updatedFlow =  updateRevisableEntity(versionedFlow, VERSIONED_FLOW_ENTITY_TYPE, currentUserIdentity(),
                () -> registryService.updateFlow(versionedFlow));
        permissionsService.populateItemPermissions(updatedFlow);
        linkService.populateLinks(updatedFlow);
        return updatedFlow;
    }

    @Override
    public VersionedFlow deleteFlow(final String bucketIdentifier, final String flowIdentifier, final RevisionInfo revisionInfo) {
        authorizeBucketAccess(RequestAction.DELETE, bucketIdentifier);
        validateDeleteOfRevisableEntity(flowIdentifier, revisionInfo, VERSIONED_FLOW_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        registryService.verifyFlowExists(flowIdentifier);

        return deleteRevisableEntity(flowIdentifier, VERSIONED_FLOW_ENTITY_TYPE, revisionInfo,
                () -> registryService.deleteFlow(bucketIdentifier, flowIdentifier));
    }

    // ---------------------- Flow Snapshot methods ----------------------------------------------

    @Override
    public VersionedFlowSnapshot createFlowSnapshot(final VersionedFlowSnapshot flowSnapshot) {
        authorizeBucketAccess(RequestAction.WRITE, flowSnapshot);

        final VersionedFlowSnapshot createdSnapshot = registryService.createFlowSnapshot(flowSnapshot);
        populateLinksAndPermissions(createdSnapshot);
        return createdSnapshot;
    }

    @Override
    public VersionedFlowSnapshot getFlowSnapshot(final String bucketIdentifier, final String flowIdentifier, final Integer version) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final VersionedFlowSnapshot snapshot = registryService.getFlowSnapshot(bucketIdentifier, flowIdentifier, version);
        populateLinksAndPermissions(snapshot);
        return snapshot;
    }

    @Override
    public VersionedFlowSnapshot getFlowSnapshot(final String flowIdentifier, final Integer version) {
        final VersionedFlowSnapshotMetadata latestMetadata = registryService.getLatestFlowSnapshotMetadata(flowIdentifier);
        authorizeBucketAccess(RequestAction.READ, latestMetadata);

        final String bucketIdentifier = latestMetadata.getBucketIdentifier();
        final VersionedFlowSnapshot snapshot = registryService.getFlowSnapshot(bucketIdentifier, flowIdentifier, version);
        populateLinksAndPermissions(snapshot);
        return snapshot;
    }

    @Override
    public VersionedFlowSnapshot getLatestFlowSnapshot(final String bucketIdentifier, final String flowIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final VersionedFlowSnapshotMetadata latestMetadata = getLatestFlowSnapshotMetadata(bucketIdentifier, flowIdentifier);
        final VersionedFlowSnapshot lastSnapshot = getFlowSnapshot(bucketIdentifier, flowIdentifier, latestMetadata.getVersion());
        populateLinksAndPermissions(lastSnapshot);
        return lastSnapshot;
    }

    @Override
    public VersionedFlowSnapshot getLatestFlowSnapshot(final String flowIdentifier) {
        final VersionedFlowSnapshotMetadata latestMetadata = registryService.getLatestFlowSnapshotMetadata(flowIdentifier);
        authorizeBucketAccess(RequestAction.READ, latestMetadata);

        final String bucketIdentifier = latestMetadata.getBucketIdentifier();
        final Integer latestVersion = latestMetadata.getVersion();

        final VersionedFlowSnapshot lastSnapshot = registryService.getFlowSnapshot(bucketIdentifier, flowIdentifier, latestVersion);
        populateLinksAndPermissions(lastSnapshot);
        return lastSnapshot;
    }

    @Override
    public VersionedFlowSnapshot importVersionedFlowSnapshot(VersionedFlowSnapshot versionedFlowSnapshot, String bucketIdentifier,
                                                             String flowIdentifier, String comments) {
        // set new snapshotMetadata
        final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setBucketIdentifier(bucketIdentifier);
        metadata.setFlowIdentifier(flowIdentifier);
        metadata.setVersion(LATEST_VERSION);

        // if there are new comments, then set it
        // otherwise, keep the original comments
        if (StringUtils.isNotBlank(comments)) {
            metadata.setComments(comments);
        } else if (versionedFlowSnapshot.getSnapshotMetadata() != null && StringUtils.isNotBlank(versionedFlowSnapshot.getSnapshotMetadata().getComments())) {
            metadata.setComments(versionedFlowSnapshot.getSnapshotMetadata().getComments());
        }

        versionedFlowSnapshot.setSnapshotMetadata(metadata);

        final String userIdentity = NiFiUserUtils.getNiFiUserIdentity();
        versionedFlowSnapshot.getSnapshotMetadata().setAuthor(userIdentity);

        return createFlowSnapshot(versionedFlowSnapshot);
    }

    @Override
    public ExportedVersionedFlowSnapshot exportFlowSnapshot(String bucketIdentifier, String flowIdentifier, Integer versionNumber) {
        final VersionedFlowSnapshot versionedFlowSnapshot = getFlowSnapshot(bucketIdentifier, flowIdentifier, versionNumber);

        String flowName = versionedFlowSnapshot.getFlow().getName();
        final String dashFlowName = flowName.replaceAll("\\s", "-");
        final String filename = String.format("%s-version-%d.json", dashFlowName, versionedFlowSnapshot.getSnapshotMetadata().getVersion());

        versionedFlowSnapshot.setFlow(null);
        versionedFlowSnapshot.setBucket(null);
        versionedFlowSnapshot.getSnapshotMetadata().setBucketIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setFlowIdentifier(null);
        versionedFlowSnapshot.getSnapshotMetadata().setLink(null);

        return new ExportedVersionedFlowSnapshot(versionedFlowSnapshot, filename);
    }

    @Override
    public SortedSet<VersionedFlowSnapshotMetadata> getFlowSnapshots(final String bucketIdentifier, final String flowIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final SortedSet<VersionedFlowSnapshotMetadata> snapshots = registryService.getFlowSnapshots(bucketIdentifier, flowIdentifier);
        linkService.populateLinks(snapshots);
        return snapshots;
    }

    @Override
    public SortedSet<VersionedFlowSnapshotMetadata> getFlowSnapshots(final String flowIdentifier) {
        final VersionedFlow flow = registryService.getFlow(flowIdentifier);
        authorizeBucketAccess(RequestAction.READ, flow);

        final String bucketIdentifier = flow.getBucketIdentifier();
        final SortedSet<VersionedFlowSnapshotMetadata> snapshots = registryService.getFlowSnapshots(bucketIdentifier, flowIdentifier);
        linkService.populateLinks(snapshots);
        return snapshots;
    }

    @Override
    public VersionedFlowSnapshotMetadata getLatestFlowSnapshotMetadata(final String bucketIdentifier, final String flowIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final VersionedFlowSnapshotMetadata latest = registryService.getLatestFlowSnapshotMetadata(bucketIdentifier, flowIdentifier);
        linkService.populateLinks(latest);
        return latest;
    }

    @Override
    public VersionedFlowSnapshotMetadata getLatestFlowSnapshotMetadata(final String flowIdentifier) {
        final VersionedFlowSnapshotMetadata latest = registryService.getLatestFlowSnapshotMetadata(flowIdentifier);
        authorizeBucketAccess(RequestAction.READ, latest);

        linkService.populateLinks(latest);
        return latest;
    }

    @Override
    public VersionedFlowDifference getFlowDiff(final String bucketIdentifier, final String flowIdentifier, final Integer versionA, final Integer versionB) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);
        return registryService.getFlowDiff(bucketIdentifier, flowIdentifier, versionA, versionB);
    }

    private void populateLinksAndPermissions(final VersionedFlowSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }

        if (snapshot.getSnapshotMetadata() != null) {
            linkService.populateLinks(snapshot.getSnapshotMetadata());
        }

        if (snapshot.getFlow() != null) {
            linkService.populateLinks(snapshot.getFlow());
        }

        if (snapshot.getBucket() != null) {
            permissionsService.populateBucketPermissions(snapshot.getBucket());
            linkService.populateLinks(snapshot.getBucket());
        }
    }

    // ---------------------- Bundle methods ----------------------------------------------

    @Override
    public List<Bundle> getBundles(final BundleFilterParams filterParams) {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return new ArrayList<>();
        }

        final List<Bundle> bundles = extensionService.getBundles(authorizedBucketIds, filterParams);
        permissionsService.populateItemPermissions(bundles);
        linkService.populateLinks(bundles);
        return bundles;
    }

    @Override
    public List<Bundle> getBundlesByBucket(final String bucketIdentifier) {
        authorizeBucketAccess(RequestAction.READ, bucketIdentifier);

        final List<Bundle> bundles = extensionService.getBundlesByBucket(bucketIdentifier);
        permissionsService.populateItemPermissions(bundles);
        linkService.populateLinks(bundles);
        return bundles;
    }

    @Override
    public Bundle getBundle(final String bundleIdentifier) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        permissionsService.populateItemPermissions(bundle);
        linkService.populateLinks(bundle);
        return bundle;
    }

    @Override
    public Bundle deleteBundle(final String bundleIdentifier) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);
        authorizeBucketAccess(RequestAction.DELETE, bundle);

        final Bundle deletedBundle = extensionService.deleteBundle(bundle);
        permissionsService.populateItemPermissions(deletedBundle);
        linkService.populateLinks(deletedBundle);
        return deletedBundle;
    }

    // ---------------------- Bundle Version methods ----------------------------------------------

    @Override
    public BundleVersion createBundleVersion(final String bucketIdentifier, final BundleType bundleType,
                                             final InputStream inputStream, final String clientSha256) throws IOException {

        authorizeBucketAccess(RequestAction.WRITE, bucketIdentifier);

        final BundleVersion createdBundleVersion = extensionService.createBundleVersion(
                bucketIdentifier, bundleType, inputStream, clientSha256);

        linkService.populateLinks(createdBundleVersion.getVersionMetadata());
        linkService.populateLinks(createdBundleVersion.getBundle());
        linkService.populateLinks(createdBundleVersion.getBucket());

        permissionsService.populateItemPermissions(createdBundleVersion.getBundle());

        return createdBundleVersion;
    }

    @Override
    public SortedSet<BundleVersionMetadata> getBundleVersions(final BundleVersionFilterParams filterParams) {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return Collections.emptySortedSet();
        }

        final SortedSet<BundleVersionMetadata> bundleVersions = extensionService.getBundleVersions(authorizedBucketIds, filterParams);
        linkService.populateLinks(bundleVersions);
        return bundleVersions;
    }

    @Override
    public SortedSet<BundleVersionMetadata> getBundleVersions(final String bundleIdentifier) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final SortedSet<BundleVersionMetadata> bundleVersions = extensionService.getBundleVersions(bundleIdentifier);
        linkService.populateLinks(bundleVersions);
        return bundleVersions;
    }

    @Override
    public BundleVersion getBundleVersion(final String bundleIdentifier, final String version) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);
        linkService.populateLinks(bundleVersion);
        return bundleVersion;
    }

    @Override
    public StreamingContent getBundleVersionContent(final String bundleIdentifier, final String version) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);

        final StreamingOutput streamingOutput = (output) -> extensionService.writeBundleVersionContent(bundleVersion, output);
        return new StreamingContent(streamingOutput, bundleVersion.getFilename());
    }

    @Override
    public BundleVersion deleteBundleVersion(final String bundleIdentifier, final String version) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);
        authorizeBucketAccess(RequestAction.DELETE, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);

        final BundleVersion deletedBundleVersion = extensionService.deleteBundleVersion(bundleVersion);
        linkService.populateLinks(deletedBundleVersion);
        return deletedBundleVersion;
    }

    // ---------------------- Extension methods ----------------------------------------------

    @Override
    public SortedSet<ExtensionMetadata> getExtensionMetadata(final ExtensionFilterParams filterParams) {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return Collections.emptySortedSet();
        }

        final SortedSet<ExtensionMetadata> metadata = extensionService.getExtensionMetadata(authorizedBucketIds, filterParams);
        linkService.populateLinks(metadata);
        return metadata;
    }

    @Override
    public SortedSet<ExtensionMetadata> getExtensionMetadata(final ProvidedServiceAPI serviceAPI) {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return Collections.emptySortedSet();
        }

        final SortedSet<ExtensionMetadata> metadata = extensionService.getExtensionMetadata(authorizedBucketIds, serviceAPI);
        linkService.populateLinks(metadata);
        return metadata;
    }

    @Override
    public SortedSet<ExtensionMetadata> getExtensionMetadata(final String bundleIdentifier, final String version) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);

        final SortedSet<ExtensionMetadata> extensions = extensionService.getExtensionMetadata(bundleVersion);
        linkService.populateLinks(extensions);
        return extensions;
    }

    @Override
    public Extension getExtension(final String bundleIdentifier, final String version, final String name) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);
        return extensionService.getExtension(bundleVersion, name);
    }

    @Override
    public StreamingOutput getExtensionDocs(final String bundleIdentifier, final String version, final String name) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);

        final StreamingOutput streamingOutput = (output) -> extensionService.writeExtensionDocs(bundleVersion, name, output);
        return streamingOutput;
    }

    @Override
    public StreamingOutput getAdditionalDetailsDocs(final String bundleIdentifier, final String version, final String name) {
        final Bundle bundle = extensionService.getBundle(bundleIdentifier);
        authorizeBucketAccess(RequestAction.READ, bundle);

        final String bucketIdentifier = bundle.getBucketIdentifier();
        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucketIdentifier, bundleIdentifier, version);

        final StreamingOutput streamingOutput = (output) -> extensionService.writeAdditionalDetailsDocs(bundleVersion, name, output);
        return streamingOutput;
    }

    @Override
    public SortedSet<TagCount> getExtensionTags() {
        return extensionService.getExtensionTags();
    }

    // ---------------------- Extension Repository methods ----------------------------------------------

    @Override
    public SortedSet<ExtensionRepoBucket> getExtensionRepoBuckets(final URI baseUri) {
        final Set<String> authorizedBucketIds = getAuthorizedBucketIds(RequestAction.READ);
        if (authorizedBucketIds == null || authorizedBucketIds.isEmpty()) {
            // not authorized for any bucket, return empty list of items
            return Collections.emptySortedSet();
        }

        final SortedSet<ExtensionRepoBucket> repoBuckets = extensionService.getExtensionRepoBuckets(authorizedBucketIds);
        linkService.populateFullLinks(repoBuckets, baseUri);
        return repoBuckets;
    }

    @Override
    public SortedSet<ExtensionRepoGroup> getExtensionRepoGroups(final URI baseUri, final String bucketName) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final SortedSet<ExtensionRepoGroup> repoGroups = extensionService.getExtensionRepoGroups(bucket);
        linkService.populateFullLinks(repoGroups, baseUri);
        return extensionService.getExtensionRepoGroups(bucket);
    }

    @Override
    public SortedSet<ExtensionRepoArtifact> getExtensionRepoArtifacts(final URI baseUri, final String bucketName, final String groupId) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final SortedSet<ExtensionRepoArtifact> repoArtifacts = extensionService.getExtensionRepoArtifacts(bucket, groupId);
        linkService.populateFullLinks(repoArtifacts, baseUri);
        return repoArtifacts;
    }

    @Override
    public SortedSet<ExtensionRepoVersionSummary> getExtensionRepoVersions(final URI baseUri, final String bucketName, final String groupId,
                                                                           final String artifactId) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final SortedSet<ExtensionRepoVersionSummary> repoVersions = extensionService.getExtensionRepoVersions(bucket, groupId, artifactId);
        linkService.populateFullLinks(repoVersions, baseUri);
        return repoVersions;
    }

    @Override
    public ExtensionRepoVersion getExtensionRepoVersion(final URI baseUri, final String bucketName, final String groupId,
                                                        final String artifactId, final String version) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);

        final String extensionsUri = generateResourceUri(baseUri,
                "extension-repository",
                bundleVersion.getBucket().getName(),
                bundleVersion.getBundle().getGroupId(),
                bundleVersion.getBundle().getArtifactId(),
                bundleVersion.getVersionMetadata().getVersion(),
                "extensions");

        final String downloadUri = generateResourceUri(baseUri,
                "extension-repository",
                bundleVersion.getBucket().getName(),
                bundleVersion.getBundle().getGroupId(),
                bundleVersion.getBundle().getArtifactId(),
                bundleVersion.getVersionMetadata().getVersion(),
                "content");

        final String sha256Uri = generateResourceUri(baseUri,
                "extension-repository",
                bundleVersion.getBucket().getName(),
                bundleVersion.getBundle().getGroupId(),
                bundleVersion.getBundle().getArtifactId(),
                bundleVersion.getVersionMetadata().getVersion(),
                "sha256");

        final ExtensionRepoVersion repoVersion = new ExtensionRepoVersion();
        repoVersion.setExtensionsLink(Link.fromUri(extensionsUri).rel("extensions").build());
        repoVersion.setDownloadLink(Link.fromUri(downloadUri).rel("content").build());
        repoVersion.setSha256Link(Link.fromUri(sha256Uri).rel("sha256").build());
        repoVersion.setSha256Supplied(bundleVersion.getVersionMetadata().getSha256Supplied());
        return repoVersion;
    }

    @Override
    public StreamingContent getExtensionRepoVersionContent(final String bucketName, final String groupId, final String artifactId,
                                                           final String version) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);
        final StreamingOutput streamingOutput = (output) -> extensionService.writeBundleVersionContent(bundleVersion, output);
        return new StreamingContent(streamingOutput, bundleVersion.getFilename());
    }

    @Override
    public String getExtensionRepoVersionSha256(final String bucketName, final String groupId, final String artifactId,
                                                final String version) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);
        final String sha256Hex = bundleVersion.getVersionMetadata().getSha256();
        return sha256Hex;
    }

    @Override
    public List<ExtensionRepoExtensionMetadata> getExtensionRepoExtensions(final URI baseUri, final String bucketName, final String groupId,
                                                                           final String artifactId, final String version) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);
        final SortedSet<ExtensionMetadata> extensions = extensionService.getExtensionMetadata(bundleVersion);

        final List<ExtensionRepoExtensionMetadata> extensionRepoExtensions = new ArrayList<>(extensions.size());
        extensions.forEach(e -> extensionRepoExtensions.add(new ExtensionRepoExtensionMetadata(e)));
        linkService.populateFullLinks(extensionRepoExtensions, baseUri);
        return extensionRepoExtensions;
    }

    @Override
    public Extension getExtensionRepoExtension(final URI baseUri, final String bucketName, final String groupId,
                                               final String artifactId, final String version, final String extensionName) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);
        final Extension extension = extensionService.getExtension(bundleVersion, extensionName);
        return extension;
    }

    @Override
    public StreamingOutput getExtensionRepoExtensionDocs(final URI baseUri, final String bucketName, final String groupId,
                                                         final String artifactId, final String version, final String extensionName) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);
        final StreamingOutput streamingOutput = (output) -> extensionService.writeExtensionDocs(bundleVersion, extensionName, output);
        return streamingOutput;
    }

    @Override
    public StreamingOutput getExtensionRepoExtensionAdditionalDocs(final URI baseUri, final String bucketName, final String groupId,
                                                                   final String artifactId, final String version, final String extensionName) {
        final Bucket bucket = registryService.getBucketByName(bucketName);
        authorizeBucketAccess(RequestAction.READ, bucket.getIdentifier());

        final BundleVersion bundleVersion = extensionService.getBundleVersion(bucket.getIdentifier(), groupId, artifactId, version);
        final StreamingOutput streamingOutput = (output) -> extensionService.writeAdditionalDetailsDocs(bundleVersion, extensionName, output);
        return streamingOutput;
    }


    // ---------------------- Field methods ----------------------------------------------

    @Override
    public Set<String> getBucketFields() {
        return registryService.getBucketFields();
    }

    @Override
    public Set<String> getBucketItemFields() {
        return registryService.getBucketItemFields();
    }

    @Override
    public Set<String> getFlowFields() {
        return registryService.getFlowFields();
    }

    // ---------------------- User methods ----------------------------------------------

    @Override
    public User createUser(final User user) {
        verifyAuthorizerSupportsConfigurableUserGroups();
        authorizeTenantsAccess(RequestAction.WRITE);
        validateCreationOfRevisableEntity(user, USER_ENTITY_TYPE);
        validateIdentifierNotPresent(user, USER_ENTITY_TYPE);

        user.setIdentifier(UUID.randomUUID().toString());
        return createRevisableEntity(user, USER_ENTITY_TYPE, currentUserIdentity(), () -> authorizationService.createUser(user));
    }

    @Override
    public List<User> getUsers() {
        verifyAuthorizerIsManaged();
        authorizeTenantsAccess(RequestAction.READ);
        return entityService.getEntities(() -> authorizationService.getUsers());
    }

    @Override
    public User getUser(final String identifier) {
        verifyAuthorizerIsManaged();
        authorizeTenantsAccess(RequestAction.READ);
        return entityService.get(() -> authorizationService.getUser(identifier));
    }

    @Override
    public User updateUser(final User user) {
        verifyAuthorizerSupportsConfigurableUserGroups();
        authorizeTenantsAccess(RequestAction.WRITE);
        validateUpdateOfRevisableEntity(user, USER_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        authorizationService.verifyUserExists(user.getIdentifier());

        return updateRevisableEntity(user, USER_ENTITY_TYPE, currentUserIdentity(), () -> authorizationService.updateUser(user));
    }

    @Override
    public User deleteUser(final String identifier, final RevisionInfo revisionInfo) {
        verifyAuthorizerSupportsConfigurableUserGroups();
        authorizeTenantsAccess(RequestAction.DELETE);
        validateDeleteOfRevisableEntity(identifier, revisionInfo, USER_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        authorizationService.verifyUserExists(identifier);

        return deleteRevisableEntity(identifier, USER_ENTITY_TYPE, revisionInfo, () -> authorizationService.deleteUser(identifier));
    }

    // ---------------------- UserGroup methods ----------------------------------------------

    @Override
    public UserGroup createUserGroup(final UserGroup userGroup) {
        verifyAuthorizerSupportsConfigurableUserGroups();
        authorizeTenantsAccess(RequestAction.WRITE);
        validateCreationOfRevisableEntity(userGroup, USER_GROUP_ENTITY_TYPE);
        validateIdentifierNotPresent(userGroup, USER_GROUP_ENTITY_TYPE);

        userGroup.setIdentifier(UUID.randomUUID().toString());
        return createRevisableEntity(userGroup, USER_GROUP_ENTITY_TYPE, currentUserIdentity(),
                () -> authorizationService.createUserGroup(userGroup));
    }

    @Override
    public List<UserGroup> getUserGroups() {
        verifyAuthorizerIsManaged();
        authorizeTenantsAccess(RequestAction.READ);
        return entityService.getEntities(() -> authorizationService.getUserGroups());
    }

    @Override
    public UserGroup getUserGroup(final String identifier) {
        verifyAuthorizerIsManaged();
        authorizeTenantsAccess(RequestAction.READ);
        return entityService.get(() -> authorizationService.getUserGroup(identifier));
    }

    @Override
    public UserGroup updateUserGroup(final UserGroup userGroup) {
        verifyAuthorizerSupportsConfigurableUserGroups();
        authorizeTenantsAccess(RequestAction.WRITE);
        validateUpdateOfRevisableEntity(userGroup, USER_GROUP_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        authorizationService.verifyUserGroupExists(userGroup.getIdentifier());

        return updateRevisableEntity(userGroup, USER_GROUP_ENTITY_TYPE, currentUserIdentity(),
                () -> authorizationService.updateUserGroup(userGroup));
    }

    @Override
    public UserGroup deleteUserGroup(final String identifier, final RevisionInfo revisionInfo) {
        verifyAuthorizerSupportsConfigurableUserGroups();
        authorizeTenantsAccess(RequestAction.DELETE);
        validateDeleteOfRevisableEntity(identifier, revisionInfo, USER_GROUP_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        authorizationService.verifyUserGroupExists(identifier);

        return deleteRevisableEntity(identifier, USER_GROUP_ENTITY_TYPE, revisionInfo,
                () -> authorizationService.deleteUserGroup(identifier));
    }

    // ---------------------- AccessPolicy methods ----------------------------------------------

    @Override
    public AccessPolicy createAccessPolicy(final AccessPolicy accessPolicy) {
        verifyAuthorizerSupportsConfigurablePolicies();
        authorizePoliciesAccess(RequestAction.WRITE);
        validateCreationOfRevisableEntity(accessPolicy, ACCESS_POLICY_ENTITY_TYPE);
        validateIdentifierNotPresent(accessPolicy, ACCESS_POLICY_ENTITY_TYPE);

        accessPolicy.setIdentifier(UUID.randomUUID().toString());
        return createRevisableEntity(accessPolicy, ACCESS_POLICY_ENTITY_TYPE, currentUserIdentity(),
                () -> authorizationService.createAccessPolicy(accessPolicy));
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) {
        verifyAuthorizerIsManaged();
        authorizePoliciesAccess(RequestAction.READ);
        return entityService.get(() -> authorizationService.getAccessPolicy(identifier));
    }

    @Override
    public AccessPolicy getAccessPolicy(final String resource, final RequestAction action) {
        verifyAuthorizerIsManaged();
        authorizePoliciesAccess(RequestAction.READ);
        return entityService.get(() -> authorizationService.getAccessPolicy(resource, action));
    }

    @Override
    public List<AccessPolicy> getAccessPolicies() {
        verifyAuthorizerIsManaged();
        authorizePoliciesAccess(RequestAction.READ);
        return entityService.getEntities(() -> authorizationService.getAccessPolicies());
    }

    @Override
    public AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) {
        verifyAuthorizerSupportsConfigurablePolicies();
        authorizePoliciesAccess(RequestAction.WRITE);
        validateUpdateOfRevisableEntity(accessPolicy, ACCESS_POLICY_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        authorizationService.verifyAccessPolicyExists(accessPolicy.getIdentifier());

        return updateRevisableEntity(accessPolicy, ACCESS_POLICY_ENTITY_TYPE, currentUserIdentity(),
                () -> authorizationService.updateAccessPolicy(accessPolicy));
    }

    @Override
    public AccessPolicy deleteAccessPolicy(final String identifier, final RevisionInfo revisionInfo) {
        verifyAuthorizerSupportsConfigurablePolicies();
        authorizePoliciesAccess(RequestAction.DELETE);
        validateDeleteOfRevisableEntity(identifier, revisionInfo, ACCESS_POLICY_ENTITY_TYPE);

        // verify outside of the revisable update so ResourceNotFoundException will be thrown instead of InvalidRevisionException
        authorizationService.verifyAccessPolicyExists(identifier);

        return deleteRevisableEntity(identifier, ACCESS_POLICY_ENTITY_TYPE, revisionInfo,
                () -> authorizationService.deleteAccessPolicy(identifier));
    }

    @Override
    public List<Resource> getResources() {
        authorizePoliciesAccess(RequestAction.READ);
        return authorizationService.getResources();
    }

    // ---------------------- Permission methods -----------------------------

    @Override
    public CurrentUser getCurrentUser() {
        return authorizationService.getCurrentUser();
    }


    // ---------------------- Authorization methods -----------------------------

    private void verifyAuthorizerIsManaged() {
        authorizationService.verifyAuthorizerIsManaged();
    }

    private void verifyAuthorizerSupportsConfigurablePolicies() {
        authorizationService.verifyAuthorizerSupportsConfigurablePolicies();
    }

    private void verifyAuthorizerSupportsConfigurableUserGroups() {
        authorizationService.verifyAuthorizerSupportsConfigurableUserGroups();
    }

    // ---------------------- Configuration methods -----------------------------

    @Override
    public RegistryConfiguration getRegistryConfiguration() {
        final RegistryConfiguration config = new RegistryConfiguration();

        boolean hasAnyConfigurationAccess = false;
        AccessDeniedException lastAccessDeniedException = null;
        try {
            final Authorizable policyAuthorizer = authorizableLookup.getPoliciesAuthorizable();
            authorizationService.authorize(policyAuthorizer, RequestAction.READ);
            config.setSupportsManagedAuthorizer(authorizationService.isManagedAuthorizer());
            config.setSupportsConfigurableAuthorizer(authorizationService.isConfigurableAccessPolicyProvider());
            hasAnyConfigurationAccess = true;
        } catch (AccessDeniedException e) {
            lastAccessDeniedException = e;
        }

        try {
            authorizationService.authorize(authorizableLookup.getTenantsAuthorizable(), RequestAction.READ);
            config.setSupportsConfigurableUsersAndGroups(authorizationService.isConfigurableUserGroupProvider());
            hasAnyConfigurationAccess = true;
        } catch (AccessDeniedException e) {
            lastAccessDeniedException = e;
        }

        if (!hasAnyConfigurationAccess) {
            // If the user doesn't have access to any configuration, then throw the exception.
            // Otherwise, return what they can access.
            throw lastAccessDeniedException;
        }

        return config;
    }

    // ---------------------- Helper methods -------------------------------------

    private void authorizeBucketsAccess(RequestAction actionType) throws AccessDeniedException {
        final Authorizable bucketsAuthorizable = authorizableLookup.getBucketsAuthorizable();
        authorizationService.authorize(bucketsAuthorizable, actionType);
    }

    private void authorizeBucketAccess(final RequestAction actionType, final String bucketIdentifier) {
        if (StringUtils.isBlank(bucketIdentifier)) {
            throw new IllegalArgumentException("Unable to authorize access because bucket identifier is null or blank");
        }

        final Authorizable bucketAuthorizable = authorizableLookup.getBucketAuthorizable(bucketIdentifier);
        authorizationService.authorize(bucketAuthorizable, actionType);
    }

    private void authorizeBucketAccess(final RequestAction action, final Bundle bundle) {
        // this should never happen, but if somehow the back-end didn't populate the bucket id let's make sure the bundle isn't returned
        if (bundle == null) {
            throw new IllegalStateException("Unable to authorize access because bucket identifier is null or blank");
        }

        authorizeBucketAccess(action, bundle.getBucketIdentifier());
    }

    private void authorizeBucketAccess(final RequestAction action, final VersionedFlow flow) {
        // this should never happen, but if somehow the back-end didn't populate the bucket id let's make sure the flow isn't returned
        if (flow == null) {
            throw new IllegalStateException("Unable to authorize access because bucket identifier is null or blank");
        }

        authorizeBucketAccess(action, flow.getBucketIdentifier());
    }

    private void authorizeBucketAccess(final RequestAction action, final VersionedFlowSnapshot flowSnapshot) {
        // this should never happen, but if somehow the back-end didn't populate the bucket id let's make sure the flow snapshot isn't returned
        if (flowSnapshot == null || flowSnapshot.getSnapshotMetadata() == null) {
            throw new IllegalStateException("Unable to authorize access because bucket identifier is null or blank");
        }

        authorizeBucketAccess(action, flowSnapshot.getSnapshotMetadata().getBucketIdentifier());
    }

    private void authorizeBucketAccess(final RequestAction action, final VersionedFlowSnapshotMetadata flowSnapshotMetadata) {
        // this should never happen, but if somehow the back-end didn't populate the bucket id let's make sure the flow snapshot isn't returned
        if (flowSnapshotMetadata == null) {
            throw new IllegalStateException("Unable to authorize access because bucket identifier is null or blank");
        }

        authorizeBucketAccess(action, flowSnapshotMetadata.getBucketIdentifier());
    }

    private Set<String> getAuthorizedBucketIds(final RequestAction actionType) {
        return authorizationService.getAuthorizedResources(actionType, ResourceType.Bucket)
                .stream()
                .map(StandardServiceFacade::extractBucketIdFromResource)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toSet());
    }

    private static String extractBucketIdFromResource(Resource resource) {
        if (resource == null || resource.getIdentifier() == null || !resource.getIdentifier().startsWith("/buckets/")) {
            return null;
        }

        final String[] pathComponents = resource.getIdentifier().split("/");
        if (pathComponents.length < 3) {
            return null;
        }
        return pathComponents[2];
    }

    private String generateResourceUri(final URI baseUri, final String... path) {
        final URI fullUri = UriBuilder.fromUri(baseUri).segment(path).build();
        return fullUri.toString();
    }

    private void authorizePoliciesAccess(final RequestAction actionType) {
        final Authorizable policiesAuthorizable = authorizableLookup.getPoliciesAuthorizable();
        authorizationService.authorize(policiesAuthorizable, actionType);
    }

    private void authorizeTenantsAccess(RequestAction actionType) {
        final Authorizable tenantsAuthorizable = authorizableLookup.getTenantsAuthorizable();
        authorizationService.authorize(tenantsAuthorizable, actionType);
    }

    // ---------------------- Revision Helper Methods -------------------------------------

    private void validateCreationOfRevisableEntity(final RevisableEntity entity, final String entityTypeName) {
        if (entity == null) {
            throw new IllegalArgumentException(entityTypeName + " cannot be null");
        }

        // skip checking revision if feature is disabled
        if (!revisionFeature.isEnabled()) {
            return;
        }

        // NOT: restore identifier check here when we no longer needs backwards compatibility

        if (entity.getRevision() == null
                || entity.getRevision().getVersion() == null
                || entity.getRevision().getVersion().longValue() != 0) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new " + entityTypeName + ".");
        }
    }

    /**
     * NOTE: This logic should be moved back to validateCreationOfRevisableEntity once we no longer need to maintain
     * backwards compatibility (i.e. on a major release like 1.0.0).
     *
     * Currently NiFi has been sending an identifier when creating a flow, so we need to continue to allow that.
     */
    private void validateIdentifierNotPresent(final RevisableEntity entity, final String entityTypeName) {
        if (entity.getIdentifier() != null) {
            throw new IllegalArgumentException(entityTypeName + " identifier cannot be specified when creating a new "
                    + entityTypeName.toLowerCase() + ".");
        }
    }

    private void validateUpdateOfRevisableEntity(final RevisableEntity entity, final String entityTypeName) {
        if (entity == null) {
            throw new IllegalArgumentException(entityTypeName + " cannot be null");
        }

        // skip checking revision if feature is disabled
        if (!revisionFeature.isEnabled()) {
            return;
        }

        if (entity.getRevision() == null || entity.getRevision().getVersion() == null) {
            throw new IllegalArgumentException("Revision info must be specified.");
        }
    }

    private void validateDeleteOfRevisableEntity(final String identifier, final RevisionInfo revision, final String entityTypeName) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException(entityTypeName + " identifier is required");
        }

        // skip checking revision if feature is disabled
        if (!revisionFeature.isEnabled()) {
            return;
        }

        if (revision == null || revision.getVersion() == null) {
            throw new IllegalArgumentException("Revision info must be specified.");
        }
    }

    private <T extends RevisableEntity> T createRevisableEntity(final T requestEntity, final String entityTypeName,
                                                                final String creatorIdentity, final Supplier<T> createEntity) {

        // skip using the entity service if revision feature is disabled
        if (!revisionFeature.isEnabled()) {
            final T entity = createEntity.get();
            if (entity.getRevision() == null) {
                entity.setRevision(new RevisionInfo(null, 0L));
            }
            return entity;
        } else {
            try {
                return entityService.create(requestEntity, creatorIdentity, createEntity);
            } catch (InvalidRevisionException e) {
                final String msg = String.format(INVALID_REVISION_MSG, entityTypeName, "create", requestEntity.getIdentifier());
                throw new InvalidRevisionException(msg, e);
            }
        }
    }

    private <T extends RevisableEntity> T updateRevisableEntity(final T requestEntity, final String entityTypeName,
                                                                final String updaterIdentity, final Supplier<T> updateEntity) {

        // skip using the entity service if revision feature is disabled
        if (!revisionFeature.isEnabled()) {
            final T entity = updateEntity.get();
            if (entity.getRevision() == null) {
                entity.setRevision(new RevisionInfo(null, 0L));
            }
            return entity;
        } else {
            try {
                return entityService.update(requestEntity, updaterIdentity, updateEntity);
            } catch (InvalidRevisionException e) {
                final String msg = String.format(INVALID_REVISION_MSG, entityTypeName, "update", requestEntity.getIdentifier());
                throw new InvalidRevisionException(msg, e);
            }
        }
    }

    private <T extends RevisableEntity> T deleteRevisableEntity(final String entityIdentifier, final String entityTypeName,
                                                 final RevisionInfo revisionInfo, final Supplier<T> deleteEntity) {
        // skip using the entity service if revision feature is disabled
        if (!revisionFeature.isEnabled()) {
            final T entity = deleteEntity.get();
            if (entity.getRevision() == null) {
                entity.setRevision(new RevisionInfo(null, 0L));
            }
            return entity;
        } else {
            try {
                return entityService.delete(entityIdentifier, revisionInfo, deleteEntity);
            } catch (InvalidRevisionException e) {
                final String msg = String.format(INVALID_REVISION_MSG, entityTypeName, "delete", entityIdentifier);
                throw new InvalidRevisionException(msg, e);
            }
        }
    }

}
