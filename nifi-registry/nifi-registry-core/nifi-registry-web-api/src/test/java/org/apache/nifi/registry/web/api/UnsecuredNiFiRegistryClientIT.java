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
package org.apache.nifi.registry.web.api;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.bucket.BucketItemType;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.BundleClient;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.ExtensionClient;
import org.apache.nifi.registry.client.ExtensionRepoClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.ItemsClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.extension.bundle.BuildInfo;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionDependency;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionMetadataContainer;
import org.apache.nifi.registry.extension.component.TagCount;
import org.apache.nifi.registry.extension.component.manifest.Extension;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.registry.extension.repo.ExtensionRepoBucket;
import org.apache.nifi.registry.extension.repo.ExtensionRepoExtensionMetadata;
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersion;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.flow.ExternalControllerServiceReference;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedParameter;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedPropertyDescriptor;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.util.FileUtils;
import org.bouncycastle.util.encoders.Hex;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test all basic functionality of JerseyNiFiRegistryClient.
 */
public class UnsecuredNiFiRegistryClientIT extends UnsecuredITBase {

    static final Logger LOGGER = LoggerFactory.getLogger(UnsecuredNiFiRegistryClientIT.class);

    static final String CLIENT_ID = "UnsecuredNiFiRegistryClientIT";

    private NiFiRegistryClient client;

    @Before
    public void setup() {
        final String baseUrl = createBaseURL();
        LOGGER.info("Using base url = " + baseUrl);

        final NiFiRegistryClientConfig clientConfig = new NiFiRegistryClientConfig.Builder()
                .baseUrl(baseUrl)
                .build();

        assertNotNull(clientConfig);

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder()
                .config(clientConfig)
                .build();

        assertNotNull(client);
        this.client = client;

        // Clear the extension bundles storage directory in case previous tests left data
        final File extensionsStorageDir = new File("./target/test-classes/extension_bundles");
        if (extensionsStorageDir.exists()) {
            try {
                FileUtils.deleteFile(extensionsStorageDir, true);
            } catch (Exception e) {
                LOGGER.warn("Unable to delete extensions storage dir due to: " + e.getMessage(), e);
            }
        }
    }

    @After
    public void teardown() {
        try {
            client.close();
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetAccessStatus() throws IOException, NiFiRegistryException {
        final UserClient userClient = client.getUserClient();
        final CurrentUser currentUser = userClient.getAccessStatus();
        assertEquals("anonymous", currentUser.getIdentity());
        assertTrue(currentUser.isAnonymous());
        assertNotNull(currentUser.getResourcePermissions());
        Permissions fullAccess = new Permissions().withCanRead(true).withCanWrite(true).withCanDelete(true);
        assertEquals(fullAccess, currentUser.getResourcePermissions().getAnyTopLevelResource());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getBuckets());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getTenants());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getPolicies());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getProxy());
    }

    @Test
    public void testNiFiRegistryClient() throws IOException, NiFiRegistryException, NoSuchAlgorithmException {
        // ---------------------- TEST BUCKETS --------------------------//

        final BucketClient bucketClient = client.getBucketClient();

        // create buckets
        final int numBuckets = 10;
        final List<Bucket> createdBuckets = new ArrayList<>();

        for (int i=0; i < numBuckets; i++) {
            final Bucket createdBucket = createBucket(bucketClient, i);
            LOGGER.info("Created bucket # " + i + " with id " + createdBucket.getIdentifier());
            createdBuckets.add(createdBucket);
        }

        // get each bucket
        for (final Bucket bucket : createdBuckets) {
            final Bucket retrievedBucket = bucketClient.get(bucket.getIdentifier());
            assertNotNull(retrievedBucket);
            assertNotNull(retrievedBucket.getRevision());
            assertFalse(retrievedBucket.isAllowBundleRedeploy());
            LOGGER.info("Retrieved bucket " + retrievedBucket.getIdentifier());
        }

        //final Bucket nonExistentBucket = bucketClient.get("does-not-exist");
        //assertNull(nonExistentBucket);

        // get bucket fields
        final Fields bucketFields = bucketClient.getFields();
        assertNotNull(bucketFields);
        LOGGER.info("Retrieved bucket fields, size = " + bucketFields.getFields().size());
        assertTrue(bucketFields.getFields().size() > 0);

        // get all buckets
        final List<Bucket> allBuckets = bucketClient.getAll();
        LOGGER.info("Retrieved buckets, size = " + allBuckets.size());
        assertEquals(numBuckets, allBuckets.size());
        allBuckets.stream().forEach(b -> System.out.println("Retrieve bucket " + b.getIdentifier()));

        // update each bucket
        for (final Bucket bucket : createdBuckets) {
            final Bucket bucketUpdate = new Bucket();
            bucketUpdate.setIdentifier(bucket.getIdentifier());
            bucketUpdate.setDescription(bucket.getDescription() + " UPDATE");
            bucketUpdate.setRevision(bucket.getRevision());

            final Bucket updatedBucket = bucketClient.update(bucketUpdate);
            assertNotNull(updatedBucket);
            LOGGER.info("Updated bucket " + updatedBucket.getIdentifier());
        }

        // ---------------------- TEST FLOWS --------------------------//

        final FlowClient flowClient = client.getFlowClient();

        // create flows
        final Bucket flowsBucket = createdBuckets.get(0);

        final VersionedFlow flow1 = createFlow(flowClient, flowsBucket, 1);
        LOGGER.info("Created flow # 1 with id " + flow1.getIdentifier());

        final VersionedFlow flow2 = createFlow(flowClient, flowsBucket, 2);
        LOGGER.info("Created flow # 2 with id " + flow2.getIdentifier());

        // get flow
        final VersionedFlow retrievedFlow1 = flowClient.get(flowsBucket.getIdentifier(), flow1.getIdentifier());
        assertNotNull(retrievedFlow1);
        LOGGER.info("Retrieved flow # 1 with id " + retrievedFlow1.getIdentifier());

        final VersionedFlow retrievedFlow2 = flowClient.get(flowsBucket.getIdentifier(), flow2.getIdentifier());
        assertNotNull(retrievedFlow2);
        LOGGER.info("Retrieved flow # 2 with id " + retrievedFlow2.getIdentifier());

        // get flow without bucket
        final VersionedFlow retrievedFlow1WithoutBucket = flowClient.get(flow1.getIdentifier());
        assertNotNull(retrievedFlow1WithoutBucket);
        assertEquals(flow1.getIdentifier(), retrievedFlow1WithoutBucket.getIdentifier());
        LOGGER.info("Retrieved flow # 1 without bucket id, with id " + retrievedFlow1WithoutBucket.getIdentifier());

        // update flows
        final VersionedFlow flow1Update = new VersionedFlow();
        flow1Update.setIdentifier(flow1.getIdentifier());
        flow1Update.setName(flow1.getName() + " UPDATED");
        flow1Update.setRevision(retrievedFlow1.getRevision());

        final VersionedFlow updatedFlow1 = flowClient.update(flowsBucket.getIdentifier(), flow1Update);
        assertNotNull(updatedFlow1);
        LOGGER.info("Updated flow # 1 with id " + updatedFlow1.getIdentifier());

        // get flow fields
        final Fields flowFields = flowClient.getFields();
        assertNotNull(flowFields);
        LOGGER.info("Retrieved flow fields, size = " + flowFields.getFields().size());
        assertTrue(flowFields.getFields().size() > 0);

        // get flows in bucket
        final List<VersionedFlow> flowsInBucket = flowClient.getByBucket(flowsBucket.getIdentifier());
        assertNotNull(flowsInBucket);
        assertEquals(2, flowsInBucket.size());
        flowsInBucket.stream().forEach(f -> LOGGER.info("Flow in bucket, flow id " + f.getIdentifier()));

        // ---------------------- TEST SNAPSHOTS --------------------------//

        final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();

        // create snapshots
        final VersionedFlow snapshotFlow = flow1;

        final VersionedFlowSnapshot snapshot1 = createSnapshot(snapshotClient, snapshotFlow, 1);
        assertNotNull(snapshot1);
        assertNotNull(snapshot1.getSnapshotMetadata());
        assertEquals(snapshotFlow.getIdentifier(), snapshot1.getSnapshotMetadata().getFlowIdentifier());
        LOGGER.info("Created snapshot # 1 with version " + snapshot1.getSnapshotMetadata().getVersion());

        final VersionedFlowSnapshot snapshot2 = createSnapshot(snapshotClient, snapshotFlow, 2);
        LOGGER.info("Created snapshot # 2 with version " + snapshot2.getSnapshotMetadata().getVersion());

        // get snapshot
        final VersionedFlowSnapshot retrievedSnapshot1 = snapshotClient.get(snapshotFlow.getBucketIdentifier(), snapshotFlow.getIdentifier(), 1);
        assertNotNull(retrievedSnapshot1);
        assertFalse(retrievedSnapshot1.isLatest());
        LOGGER.info("Retrieved snapshot # 1 with version " + retrievedSnapshot1.getSnapshotMetadata().getVersion());

        final VersionedFlowSnapshot retrievedSnapshot2 = snapshotClient.get(snapshotFlow.getBucketIdentifier(), snapshotFlow.getIdentifier(), 2);
        assertNotNull(retrievedSnapshot2);
        assertTrue(retrievedSnapshot2.isLatest());
        LOGGER.info("Retrieved snapshot # 2 with version " + retrievedSnapshot2.getSnapshotMetadata().getVersion());

        // get snapshot without bucket
        final VersionedFlowSnapshot retrievedSnapshot1WithoutBucket = snapshotClient.get(snapshotFlow.getIdentifier(), 1);
        assertNotNull(retrievedSnapshot1WithoutBucket);
        assertFalse(retrievedSnapshot1WithoutBucket.isLatest());
        assertEquals(snapshotFlow.getIdentifier(), retrievedSnapshot1WithoutBucket.getSnapshotMetadata().getFlowIdentifier());
        assertEquals(1, retrievedSnapshot1WithoutBucket.getSnapshotMetadata().getVersion());
        LOGGER.info("Retrieved snapshot # 1 without using bucket id, with version " + retrievedSnapshot1WithoutBucket.getSnapshotMetadata().getVersion());

        // get latest
        final VersionedFlowSnapshot retrievedSnapshotLatest = snapshotClient.getLatest(snapshotFlow.getBucketIdentifier(), snapshotFlow.getIdentifier());
        assertNotNull(retrievedSnapshotLatest);
        assertEquals(snapshot2.getSnapshotMetadata().getVersion(), retrievedSnapshotLatest.getSnapshotMetadata().getVersion());
        assertTrue(retrievedSnapshotLatest.isLatest());
        LOGGER.info("Retrieved latest snapshot with version " + retrievedSnapshotLatest.getSnapshotMetadata().getVersion());

        // get latest without bucket
        final VersionedFlowSnapshot retrievedSnapshotLatestWithoutBucket = snapshotClient.getLatest(snapshotFlow.getIdentifier());
        assertNotNull(retrievedSnapshotLatestWithoutBucket);
        assertEquals(snapshot2.getSnapshotMetadata().getVersion(), retrievedSnapshotLatestWithoutBucket.getSnapshotMetadata().getVersion());
        assertTrue(retrievedSnapshotLatestWithoutBucket.isLatest());
        LOGGER.info("Retrieved latest snapshot without bucket, with version " + retrievedSnapshotLatestWithoutBucket.getSnapshotMetadata().getVersion());

        // get metadata
        final List<VersionedFlowSnapshotMetadata> retrievedMetadata = snapshotClient.getSnapshotMetadata(snapshotFlow.getBucketIdentifier(), snapshotFlow.getIdentifier());
        assertNotNull(retrievedMetadata);
        assertEquals(2, retrievedMetadata.size());
        assertEquals(2, retrievedMetadata.get(0).getVersion());
        assertEquals(1, retrievedMetadata.get(1).getVersion());
        retrievedMetadata.stream().forEach(s -> LOGGER.info("Retrieved snapshot metadata " + s.getVersion()));

        // get metadata without bucket
        final List<VersionedFlowSnapshotMetadata> retrievedMetadataWithoutBucket = snapshotClient.getSnapshotMetadata(snapshotFlow.getIdentifier());
        assertNotNull(retrievedMetadataWithoutBucket);
        assertEquals(2, retrievedMetadataWithoutBucket.size());
        assertEquals(2, retrievedMetadataWithoutBucket.get(0).getVersion());
        assertEquals(1, retrievedMetadataWithoutBucket.get(1).getVersion());
        retrievedMetadataWithoutBucket.stream().forEach(s -> LOGGER.info("Retrieved snapshot metadata " + s.getVersion()));

        // get latest metadata
        final VersionedFlowSnapshotMetadata latestMetadata = snapshotClient.getLatestMetadata(snapshotFlow.getBucketIdentifier(), snapshotFlow.getIdentifier());
        assertNotNull(latestMetadata);
        assertEquals(2, latestMetadata.getVersion());

        // get latest metadata that doesn't exist
        try {
            snapshotClient.getLatestMetadata(snapshotFlow.getBucketIdentifier(), "DOES-NOT-EXIST");
            fail("Should have thrown exception");
        } catch (NiFiRegistryException nfe) {
            assertEquals("Error retrieving latest snapshot metadata: The specified flow ID does not exist in this bucket.", nfe.getMessage());
        }

        // get latest metadata without bucket
        final VersionedFlowSnapshotMetadata latestMetadataWithoutBucket = snapshotClient.getLatestMetadata(snapshotFlow.getIdentifier());
        assertNotNull(latestMetadataWithoutBucket);
        assertEquals(snapshotFlow.getIdentifier(), latestMetadataWithoutBucket.getFlowIdentifier());
        assertEquals(2, latestMetadataWithoutBucket.getVersion());

        // ---------------------- TEST EXTENSIONS ----------------------//

        // verify we have no bundles yet
        final BundleClient bundleClient = client.getBundleClient();
        final List<Bundle> allBundles = bundleClient.getAll();
        assertEquals(0, allBundles.size());

        final Bucket bundlesBucket = createdBuckets.get(1);
        final Bucket bundlesBucket2 = createdBuckets.get(2);
        final BundleVersionClient bundleVersionClient = client.getBundleVersionClient();

        // create version 1.0.0 of nifi-test-nar
        final String testNar1 = "src/test/resources/extensions/nars/nifi-test-nar-1.0.0.nar";
        final BundleVersion createdTestNarV1 = createExtensionBundleVersionWithStream(bundlesBucket, bundleVersionClient, testNar1, null);

        final Bundle testNarV1Bundle = createdTestNarV1.getBundle();
        LOGGER.info("Created bundle with id {}", new Object[]{testNarV1Bundle.getIdentifier()});

        assertEquals("org.apache.nifi", testNarV1Bundle.getGroupId());
        assertEquals("nifi-test-nar", testNarV1Bundle.getArtifactId());
        assertEquals(BundleType.NIFI_NAR, testNarV1Bundle.getBundleType());
        assertEquals(1, testNarV1Bundle.getVersionCount());

        assertEquals("org.apache.nifi:nifi-test-nar", testNarV1Bundle.getName());
        assertEquals(bundlesBucket.getIdentifier(), testNarV1Bundle.getBucketIdentifier());
        assertEquals(bundlesBucket.getName(), testNarV1Bundle.getBucketName());
        assertNotNull(testNarV1Bundle.getPermissions());
        assertTrue(testNarV1Bundle.getCreatedTimestamp() > 0);
        assertTrue(testNarV1Bundle.getModifiedTimestamp() > 0);

        final BundleVersionMetadata testNarV1Metadata = createdTestNarV1.getVersionMetadata();
        assertEquals("1.0.0", testNarV1Metadata.getVersion());
        assertNotNull(testNarV1Metadata.getId());
        assertNotNull(testNarV1Metadata.getSha256());
        assertNotNull(testNarV1Metadata.getAuthor());
        assertEquals(testNarV1Bundle.getIdentifier(), testNarV1Metadata.getBundleId());
        assertEquals(bundlesBucket.getIdentifier(), testNarV1Metadata.getBucketId());
        assertTrue(testNarV1Metadata.getTimestamp() > 0);
        assertFalse(testNarV1Metadata.getSha256Supplied());
        assertTrue(testNarV1Metadata.getContentSize() > 1);

        final BuildInfo testNarV1BuildInfo = testNarV1Metadata.getBuildInfo();
        assertNotNull(testNarV1BuildInfo);
        assertTrue(testNarV1BuildInfo.getBuilt() > 0);
        assertNotNull(testNarV1BuildInfo.getBuildTool());
        assertNotNull(testNarV1BuildInfo.getBuildRevision());

        final Set<BundleVersionDependency> dependencies = createdTestNarV1.getDependencies();
        assertNotNull(dependencies);
        assertEquals(1, dependencies.size());

        final BundleVersionDependency testNarV1Dependency = dependencies.stream().findFirst().get();
        assertEquals("org.apache.nifi", testNarV1Dependency.getGroupId());
        assertEquals("nifi-test-api-nar", testNarV1Dependency.getArtifactId());
        assertEquals("1.0.0", testNarV1Dependency.getVersion());

        final String testNar2 = "src/test/resources/extensions/nars/nifi-test-nar-2.0.0.nar";

        // try to create version 2.0.0 of nifi-test-nar when the supplied SHA-256 does not match server's
        final String madeUpSha256 = "MADE-UP-SHA-256";
        try {
            createExtensionBundleVersionWithStream(bundlesBucket, bundleVersionClient, testNar2, madeUpSha256);
            fail("Should have thrown exception");
        } catch (Exception e) {
            // should have thrown exception from mismatched SHA-256
        }

        // create version 2.0.0 of nifi-test-nar using correct supplied SHA-256
        final String testNar2Sha256 = calculateSha256Hex(testNar2);
        final BundleVersion createdTestNarV2 = createExtensionBundleVersionWithStream(bundlesBucket, bundleVersionClient, testNar2, testNar2Sha256);
        assertTrue(createdTestNarV2.getVersionMetadata().getSha256Supplied());

        final Bundle testNarV2Bundle = createdTestNarV2.getBundle();
        LOGGER.info("Created bundle with id {}", new Object[]{testNarV2Bundle.getIdentifier()});

        // create version 1.0.0 of nifi-foo-nar, use the file variant
        final String fooNar = "src/test/resources/extensions/nars/nifi-foo-nar-1.0.0.nar";
        final BundleVersion createdFooNarV1 = createExtensionBundleVersionWithFile(bundlesBucket, bundleVersionClient, fooNar, null);
        assertFalse(createdFooNarV1.getVersionMetadata().getSha256Supplied());

        final Bundle fooNarV1Bundle = createdFooNarV1.getBundle();
        LOGGER.info("Created bundle with id {}", new Object[]{fooNarV1Bundle.getIdentifier()});

        // verify that bucket 1 currently does not allow redeploying non-snapshot artifacts
        assertFalse(bundlesBucket.isAllowBundleRedeploy());

        // try to re-deploy version 1.0.0 of nifi-foo-nar, should fail
        try {
            createExtensionBundleVersionWithFile(bundlesBucket, bundleVersionClient, fooNar, null);
            fail("Should have thrown exception when re-deploying foo nar");
        } catch (Exception e) {
            // Should throw exception
        }

        // now update bucket 1 to allow redeploy
        bundlesBucket.setAllowBundleRedeploy(true);
        final Bucket updatedBundlesBucket = bucketClient.update(bundlesBucket);
        assertTrue(updatedBundlesBucket.isAllowBundleRedeploy());

        // try to re-deploy version 1.0.0 of nifi-foo-nar again, this time should work
        assertNotNull(createExtensionBundleVersionWithFile(bundlesBucket, bundleVersionClient, fooNar, null));

        // verify there are 2 bundles now
        final List<Bundle> allBundlesAfterCreate = bundleClient.getAll();
        assertEquals(2, allBundlesAfterCreate.size());

        // create version 2.0.0-SNAPSHOT (build 1 content) of nifi-foor-nar in the first bucket
        final String fooNarV2SnapshotB1 = "src/test/resources/extensions/nars/nifi-foo-nar-2.0.0-SNAPSHOT-BUILD1.nar";
        final BundleVersion createdFooNarV2SnapshotB1 = createExtensionBundleVersionWithFile(bundlesBucket, bundleVersionClient, fooNarV2SnapshotB1, null);
        assertFalse(createdFooNarV2SnapshotB1.getVersionMetadata().getSha256Supplied());

        // create version 2.0.0-SNAPSHOT (build 2 content) of nifi-foor-nar in the second bucket
        // proves that snapshots can have different checksums across buckets, non-snapshots can't
        final String fooNarV2SnapshotB2 = "src/test/resources/extensions/nars/nifi-foo-nar-2.0.0-SNAPSHOT-BUILD2.nar";
        final BundleVersion createdFooNarV2SnapshotB2 = createExtensionBundleVersionWithFile(bundlesBucket2, bundleVersionClient, fooNarV2SnapshotB2, null);
        assertFalse(createdFooNarV2SnapshotB2.getVersionMetadata().getSha256Supplied());

        // create version 2.0.0-SNAPSHOT (build 2 content) of nifi-foor-nar in the second bucket
        // proves that we can overwrite a snapshot in a given bucket
        final String fooNarV2SnapshotB3 = "src/test/resources/extensions/nars/nifi-foo-nar-2.0.0-SNAPSHOT-BUILD3.nar";
        final BundleVersion createdFooNarV2SnapshotB3 = createExtensionBundleVersionWithFile(bundlesBucket2, bundleVersionClient, fooNarV2SnapshotB3, null);
        assertFalse(createdFooNarV2SnapshotB3.getVersionMetadata().getSha256Supplied());

        // verify retrieving nifi-foo-nar 2.0.0-SNAPSHOT from second bucket returns the build 3 content
        final BundleVersion retrievedFooNarV2SnapshotB3 = bundleVersionClient.getBundleVersion(
                createdFooNarV2SnapshotB3.getVersionMetadata().getBundleId(),
                createdFooNarV2SnapshotB3.getVersionMetadata().getVersion());
        assertEquals(calculateSha256Hex(fooNarV2SnapshotB3), retrievedFooNarV2SnapshotB3.getVersionMetadata().getSha256());

        final List<ExtensionMetadata> fooNarV2SnapshotB3Extensions = bundleVersionClient.getExtensions(
                createdFooNarV2SnapshotB3.getVersionMetadata().getBundleId(),
                createdFooNarV2SnapshotB3.getVersionMetadata().getVersion());
        assertNotNull(fooNarV2SnapshotB3Extensions);
        assertEquals(1, fooNarV2SnapshotB3Extensions.size());
        checkExtensionMetadata(fooNarV2SnapshotB3Extensions);

        // verify getting an extension for a specific bundle version
        final String fooNarV2SnapshotB3ExtensionName = fooNarV2SnapshotB3Extensions.get(0).getName();
        final Extension fooNarV2SnapshotB3Extension = bundleVersionClient.getExtension(
                createdFooNarV2SnapshotB3.getVersionMetadata().getBundleId(),
                createdFooNarV2SnapshotB3.getVersionMetadata().getVersion(),
                fooNarV2SnapshotB3ExtensionName
        );
        assertNotNull(fooNarV2SnapshotB3Extension);
        assertEquals(fooNarV2SnapshotB3ExtensionName, fooNarV2SnapshotB3Extension.getName());

        // verify getting the docs for an extension for a specific bundle version
        try (final InputStream docsInput = bundleVersionClient.getExtensionDocs(
                createdFooNarV2SnapshotB3.getVersionMetadata().getBundleId(),
                createdFooNarV2SnapshotB3.getVersionMetadata().getVersion(),
                fooNarV2SnapshotB3ExtensionName
        )) {
            final String docsContent = IOUtils.toString(docsInput, StandardCharsets.UTF_8);
            assertNotNull(docsContent);
            assertTrue(docsContent.startsWith("<!DOCTYPE html>"));
        }

        // verify getting bundles by bucket
        assertEquals(2, bundleClient.getByBucket(bundlesBucket.getIdentifier()).size());
        assertEquals(0, bundleClient.getByBucket(flowsBucket.getIdentifier()).size());

        // verify getting bundles by id
        final Bundle retrievedBundle = bundleClient.get(testNarV1Bundle.getIdentifier());
        assertNotNull(retrievedBundle);
        assertEquals(testNarV1Bundle.getIdentifier(), retrievedBundle.getIdentifier());
        assertEquals(testNarV1Bundle.getGroupId(), retrievedBundle.getGroupId());
        assertEquals(testNarV1Bundle.getArtifactId(), retrievedBundle.getArtifactId());

        // verify getting list of version metadata for a bundle
        final List<BundleVersionMetadata> bundleVersions = bundleVersionClient.getBundleVersions(testNarV1Bundle.getIdentifier());
        assertNotNull(bundleVersions);
        assertEquals(2, bundleVersions.size());

        // verify getting a bundle version by the bundle id + version string
        final BundleVersion bundleVersion1 = bundleVersionClient.getBundleVersion(testNarV1Bundle.getIdentifier(), "1.0.0");
        assertNotNull(bundleVersion1);
        assertEquals("1.0.0", bundleVersion1.getVersionMetadata().getVersion());
        assertNotNull(bundleVersion1.getDependencies());
        assertEquals(1, bundleVersion1.getDependencies().size());

        final BundleVersion bundleVersion2 = bundleVersionClient.getBundleVersion(testNarV1Bundle.getIdentifier(), "2.0.0");
        assertNotNull(bundleVersion2);
        assertEquals("2.0.0", bundleVersion2.getVersionMetadata().getVersion());

        // verify getting the input stream for a bundle version
        try (final InputStream bundleVersion1InputStream = bundleVersionClient.getBundleVersionContent(testNarV1Bundle.getIdentifier(), "1.0.0")) {
            final String sha256Hex = DigestUtils.sha256Hex(bundleVersion1InputStream);
            assertEquals(testNarV1Metadata.getSha256(), sha256Hex);
        }

        // verify writing a bundle version to an output stream
        final File targetDir = new File("./target");
        final File bundleFile = bundleVersionClient.writeBundleVersionContent(testNarV1Bundle.getIdentifier(), "1.0.0", targetDir);
        assertNotNull(bundleFile);

        try (final InputStream bundleInputStream = new FileInputStream(bundleFile)) {
            final String sha256Hex = DigestUtils.sha256Hex(bundleInputStream);
            assertEquals(testNarV1Metadata.getSha256(), sha256Hex);
        }

        // Verify deleting a bundle version
        final BundleVersion deletedBundleVersion2 = bundleVersionClient.delete(testNarV1Bundle.getIdentifier(), "2.0.0");
        assertNotNull(deletedBundleVersion2);
        assertEquals(testNarV1Bundle.getIdentifier(), deletedBundleVersion2.getBundle().getIdentifier());
        assertEquals("2.0.0", deletedBundleVersion2.getVersionMetadata().getVersion());

        try {
            bundleVersionClient.getBundleVersion(testNarV1Bundle.getIdentifier(), "2.0.0");
            fail("Should have thrown exception");
        } catch (Exception e) {
            // should catch exception
        }

        // Verify getting bundles with filter params
        assertEquals(3, bundleClient.getAll(BundleFilterParams.empty()).size());

        final List<Bundle> filteredBundles = bundleClient.getAll(BundleFilterParams.of("org.apache.nifi", "nifi-test-nar"));
        assertEquals(1, filteredBundles.size());

        // Verify getting bundle versions with filter params
        assertEquals(4, bundleVersionClient.getBundleVersions(BundleVersionFilterParams.empty()).size());

        final List<BundleVersionMetadata> filteredVersions = bundleVersionClient.getBundleVersions(
                BundleVersionFilterParams.of("org.apache.nifi", "nifi-foo-nar", "1.0.0"));
        assertEquals(1, filteredVersions.size());

        final List<BundleVersionMetadata> filteredVersions2 = bundleVersionClient.getBundleVersions(
                BundleVersionFilterParams.of("org.apache.nifi", null, null));
        assertEquals(4, filteredVersions2.size());

        filteredVersions2.forEach(bvm -> {
            assertNotNull(bvm.getGroupId());
            assertNotNull(bvm.getArtifactId());
            assertNotNull(bvm.getVersion());
        });

        // ---------------------- TEST EXTENSION REPO ----------------------//

        final ExtensionRepoClient extensionRepoClient = client.getExtensionRepoClient();

        final List<ExtensionRepoBucket> repoBuckets = extensionRepoClient.getBuckets();
        assertEquals(createdBuckets.size(), repoBuckets.size());

        final String bundlesBucketName = bundlesBucket.getName();
        final List<ExtensionRepoGroup> repoGroups = extensionRepoClient.getGroups(bundlesBucketName);
        assertEquals(1, repoGroups.size());

        final String repoGroupId = "org.apache.nifi";
        final ExtensionRepoGroup repoGroup = repoGroups.get(0);
        assertEquals(repoGroupId, repoGroup.getGroupId());

        final List<ExtensionRepoArtifact> repoArtifacts = extensionRepoClient.getArtifacts(bundlesBucketName, repoGroupId);
        assertEquals(2, repoArtifacts.size());

        final String repoArtifactId = "nifi-test-nar";
        final List<ExtensionRepoVersionSummary> repoVersions = extensionRepoClient.getVersions(bundlesBucketName, repoGroupId, repoArtifactId);
        assertEquals(1, repoVersions.size());

        final String repoVersionString = "1.0.0";
        final ExtensionRepoVersion repoVersion = extensionRepoClient.getVersion(bundlesBucketName, repoGroupId, repoArtifactId, repoVersionString);
        assertNotNull(repoVersion);
        assertNotNull(repoVersion.getDownloadLink());
        assertNotNull(repoVersion.getSha256Link());
        assertNotNull(repoVersion.getExtensionsLink());

        // verify the version links for content and sha256
        final Client jerseyClient = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();

        final WebTarget downloadLinkTarget = jerseyClient.target(repoVersion.getDownloadLink().getUri());
        try (final InputStream downloadLinkInputStream = downloadLinkTarget.request()
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get().readEntity(InputStream.class)) {
            final String sha256DownloadResult = DigestUtils.sha256Hex(downloadLinkInputStream);

            final WebTarget sha256LinkTarget = jerseyClient.target(repoVersion.getSha256Link().getUri());
            final String sha256LinkResult = sha256LinkTarget.request().get(String.class);
            assertEquals(sha256DownloadResult, sha256LinkResult);
        }

        // verify the version link for extension metadata works
        final WebTarget extensionsLinkTarget = jerseyClient.target(repoVersion.getExtensionsLink().getUri());
        final ExtensionRepoExtensionMetadata[] extensions = extensionsLinkTarget.request()
                .accept(MediaType.APPLICATION_JSON)
                .get(ExtensionRepoExtensionMetadata[].class);
        assertNotNull(extensions);
        assertTrue(extensions.length > 0);
        checkExtensionMetadata(Stream.of(extensions).map(e -> e.getExtensionMetadata()).collect(Collectors.toSet()));
        Stream.of(extensions).forEach(e -> {
            assertNotNull(e.getLink());
            assertNotNull(e.getLinkDocs());
        });

        // verify the client methods for content input stream, content sha256, and extensions
        try (final InputStream repoVersionInputStream = extensionRepoClient.getVersionContent(bundlesBucketName, repoGroupId, repoArtifactId, repoVersionString)) {
            final String sha256Hex = DigestUtils.sha256Hex(repoVersionInputStream);

            final String repoSha256Hex = extensionRepoClient.getVersionSha256(bundlesBucketName, repoGroupId, repoArtifactId, repoVersionString);
            assertEquals(sha256Hex, repoSha256Hex);

            final Optional<String> repoSha256HexOptional = extensionRepoClient.getVersionSha256(repoGroupId, repoArtifactId, repoVersionString);
            assertTrue(repoSha256HexOptional.isPresent());
            assertEquals(sha256Hex, repoSha256HexOptional.get());

            final List<ExtensionRepoExtensionMetadata> extensionList = extensionRepoClient.getVersionExtensions(bundlesBucketName, repoGroupId, repoArtifactId, repoVersionString);
            assertNotNull(extensionList);
            assertTrue(extensionList.size() > 0);
            extensionList.forEach(em -> {
                assertNotNull(em.getExtensionMetadata());
                assertNotNull(em.getLink());
                assertNotNull(em.getLinkDocs());
            });

            final String extensionName = extensionList.get(0).getExtensionMetadata().getName();
            final Extension extension = extensionRepoClient.getVersionExtension(
                    bundlesBucketName, repoGroupId, repoArtifactId, repoVersionString, extensionName);
            assertNotNull(extension);
            assertEquals(extensionName, extension.getName());

            // verify getting the docs for an extension from extension repo
            try (final InputStream docsInput = extensionRepoClient.getVersionExtensionDocs(
                    bundlesBucketName, repoGroupId, repoArtifactId, repoVersionString, extensionName)
            ) {
                final String docsContent = IOUtils.toString(docsInput, StandardCharsets.UTF_8);
                assertNotNull(docsContent);
                assertTrue(docsContent.startsWith("<!DOCTYPE html>"));
            }
        }

        final Optional<String> repoSha256HexDoesNotExist = extensionRepoClient.getVersionSha256(repoGroupId, repoArtifactId, "DOES-NOT-EXIST");
        assertFalse(repoSha256HexDoesNotExist.isPresent());

        // since we uploaded two snapshot versions, make sure when we retrieve the sha that it's for the second snapshot that replaced the first
        final Optional<String> fooNarV2SnapshotLatestSha = extensionRepoClient.getVersionSha256(
                createdFooNarV2SnapshotB3.getBundle().getGroupId(),
                createdFooNarV2SnapshotB3.getBundle().getArtifactId(),
                createdFooNarV2SnapshotB2.getVersionMetadata().getVersion());
        assertTrue(fooNarV2SnapshotLatestSha.isPresent());
        assertEquals(calculateSha256Hex(fooNarV2SnapshotB3), fooNarV2SnapshotLatestSha.get());

        // ---------------------- TEST EXTENSIONS -------------------------- //

        final ExtensionClient extensionClient = client.getExtensionClient();

        final List<TagCount> tagCounts = extensionClient.getTagCounts();
        assertNotNull(tagCounts);
        assertTrue(tagCounts.size() > 0);
        tagCounts.forEach(tc -> {
            assertNotNull(tc.getTag());
        });

        final ExtensionMetadataContainer allExtensions = extensionClient.findExtensions((ExtensionFilterParams)null);
        assertNotNull(allExtensions);
        assertNotNull(allExtensions.getExtensions());
        assertEquals(4, allExtensions.getNumResults());
        assertEquals(4, allExtensions.getExtensions().size());

        allExtensions.getExtensions().forEach(e -> {
            assertNotNull(e.getName());
            assertNotNull(e.getDisplayName());
            assertNotNull(e.getLink());
            assertNotNull(e.getLinkDocs());
        });

        final ExtensionMetadataContainer processorExtensions = extensionClient.findExtensions(
                new ExtensionFilterParams.Builder().extensionType(ExtensionType.PROCESSOR).build());
        assertNotNull(processorExtensions);
        assertNotNull(processorExtensions.getExtensions());
        assertEquals(3, processorExtensions.getNumResults());
        assertEquals(3, processorExtensions.getExtensions().size());

        final ExtensionMetadataContainer serviceExtensions = extensionClient.findExtensions(
                new ExtensionFilterParams.Builder().extensionType(ExtensionType.CONTROLLER_SERVICE).build());
        assertNotNull(serviceExtensions);
        assertNotNull(serviceExtensions.getExtensions());
        assertEquals(1, serviceExtensions.getNumResults());
        assertEquals(1, serviceExtensions.getExtensions().size());

        final ExtensionMetadataContainer narExtensions = extensionClient.findExtensions(
                new ExtensionFilterParams.Builder().bundleType(BundleType.NIFI_NAR).build());
        assertNotNull(narExtensions);
        assertNotNull(narExtensions.getExtensions());
        assertEquals(4, narExtensions.getNumResults());
        assertEquals(4, narExtensions.getExtensions().size());

        final ProvidedServiceAPI serviceAPI = new ProvidedServiceAPI();
        serviceAPI.setClassName("org.apache.nifi.service.TestService");
        serviceAPI.setGroupId("org.apache.nifi");
        serviceAPI.setArtifactId("nifi-test-service-api-nar");
        serviceAPI.setVersion("1.0.0");

        final ExtensionMetadataContainer providedTestServiceApi = extensionClient.findExtensions(serviceAPI);
        assertNotNull(providedTestServiceApi);
        assertNotNull(providedTestServiceApi.getExtensions());
        assertEquals(1, providedTestServiceApi.getNumResults());
        assertEquals(1, providedTestServiceApi.getExtensions().size());
        assertEquals("org.apache.nifi.service.TestServiceImpl", providedTestServiceApi.getExtensions().first().getName());

        // ---------------------- TEST ITEMS -------------------------- //

        final ItemsClient itemsClient = client.getItemsClient();

        // get fields
        final Fields itemFields = itemsClient.getFields();
        assertNotNull(itemFields.getFields());
        assertTrue(itemFields.getFields().size() > 0);

        // get all items
        final List<BucketItem> allItems = itemsClient.getAll();
        assertEquals(5, allItems.size());
        allItems.stream().forEach(i -> {
            assertNotNull(i.getBucketName());
            assertNotNull(i.getLink());
        });
        allItems.stream().forEach(i -> LOGGER.info("All items, item " + i.getIdentifier()));

        // verify 2 flow items
        final List<BucketItem> flowItems = allItems.stream()
                .filter(i -> i.getType() == BucketItemType.Flow)
                .collect(Collectors.toList());
        assertEquals(2, flowItems.size());

        // verify 3 bundle items
        final List<BucketItem> extensionBundleItems = allItems.stream()
                .filter(i -> i.getType() == BucketItemType.Bundle)
                .collect(Collectors.toList());
        assertEquals(3, extensionBundleItems.size());

        // get items for bucket
        final List<BucketItem> bucketItems = itemsClient.getByBucket(flowsBucket.getIdentifier());
        assertEquals(2, bucketItems.size());
        allItems.stream().forEach(i -> assertNotNull(i.getBucketName()));
        bucketItems.stream().forEach(i -> LOGGER.info("Items in bucket, item " + i.getIdentifier()));

        // ----------------------- TEST DIFF ---------------------------//

        final VersionedFlowSnapshot snapshot3 = buildSnapshot(snapshotFlow, 3);
        final VersionedProcessGroup newlyAddedPG = new VersionedProcessGroup();
        newlyAddedPG.setIdentifier("new-pg");
        newlyAddedPG.setName("NEW Process Group");
        snapshot3.getFlowContents().getProcessGroups().add(newlyAddedPG);
        snapshotClient.create(snapshot3);

        VersionedFlowDifference diff = flowClient.diff(snapshotFlow.getBucketIdentifier(), snapshotFlow.getIdentifier(), 3, 2);
        assertNotNull(diff);
        assertEquals(1, diff.getComponentDifferenceGroups().size());

        // ---------------------- DELETE DATA --------------------------//

        final VersionedFlow deletedFlow1 = flowClient.delete(flowsBucket.getIdentifier(), updatedFlow1.getIdentifier(), updatedFlow1.getRevision());
        assertNotNull(deletedFlow1);
        LOGGER.info("Deleted flow " + deletedFlow1.getIdentifier());

        final VersionedFlow deletedFlow2 = flowClient.delete(flowsBucket.getIdentifier(), flow2.getIdentifier(), flow2.getRevision());
        assertNotNull(deletedFlow2);
        LOGGER.info("Deleted flow " + deletedFlow2.getIdentifier());

        final Bundle deletedBundle1 = bundleClient.delete(testNarV1Bundle.getIdentifier());
        assertNotNull(deletedBundle1);
        LOGGER.info("Deleted extension bundle " + deletedBundle1.getIdentifier());

        final Bundle deletedBundle2 = bundleClient.delete(fooNarV1Bundle.getIdentifier());
        assertNotNull(deletedBundle2);
        LOGGER.info("Deleted extension bundle " + deletedBundle2.getIdentifier());

        // delete each bucket
        for (final Bucket bucket : createdBuckets) {
            final Bucket deletedBucket = bucketClient.delete(bucket.getIdentifier(), bucket.getRevision());
            assertNotNull(deletedBucket);
            LOGGER.info("Deleted bucket " + deletedBucket.getIdentifier());
        }
        assertEquals(0, bucketClient.getAll().size());

        LOGGER.info("!!! SUCCESS !!!");

    }

    @Test
    public void testFlowSnapshotsWithParameterContextAndEncodingVersion() throws IOException, NiFiRegistryException {
        final RevisionInfo initialRevision = new RevisionInfo(null, 0L);

        // Create a bucket
        final Bucket bucket = new Bucket();
        bucket.setName("Test Bucket");
        bucket.setRevision(initialRevision);

        final Bucket createdBucket = client.getBucketClient().create(bucket);
        assertNotNull(createdBucket);

        // Create the flow
        final VersionedFlow flow = new VersionedFlow();
        flow.setName("My Flow");
        flow.setBucketIdentifier(createdBucket.getIdentifier());
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client.getFlowClient().create(flow);
        assertNotNull(createdFlow);

        // Create a param context
        final VersionedParameter param1 = new VersionedParameter();
        param1.setName("Param 1");
        param1.setValue("Param 1 Value");
        param1.setDescription("Description");

        final VersionedParameter param2 = new VersionedParameter();
        param2.setName("Param 2");
        param2.setValue("Param 2 Value");
        param2.setDescription("Description");
        param2.setSensitive(true);

        final VersionedParameterContext context1 = new VersionedParameterContext();
        context1.setName("Parameter Context 1");
        context1.setParameters(new HashSet<>(Arrays.asList(param1, param2)));

        final Map<String,VersionedParameterContext> contexts = new HashMap<>();
        contexts.put(context1.getName(), context1);

        // Create an external controller service reference
        final ExternalControllerServiceReference serviceReference = new ExternalControllerServiceReference();
        serviceReference.setName("External Service 1");
        serviceReference.setIdentifier(UUID.randomUUID().toString());

        final Map<String,ExternalControllerServiceReference> serviceReferences = new HashMap<>();
        serviceReferences.put(serviceReference.getIdentifier(), serviceReference);

        // Create the snapshot
        final VersionedFlowSnapshot snapshot = buildSnapshot(createdFlow, 1);
        snapshot.setFlowEncodingVersion("2.0.0");
        snapshot.setParameterContexts(contexts);
        snapshot.setExternalControllerServices(serviceReferences);

        final VersionedFlowSnapshot createdSnapshot = client.getFlowSnapshotClient().create(snapshot);
        assertNotNull(createdSnapshot);
        assertNotNull(createdSnapshot.getFlowEncodingVersion());
        assertNotNull(createdSnapshot.getParameterContexts());
        assertNotNull(createdSnapshot.getExternalControllerServices());
        assertEquals(snapshot.getFlowEncodingVersion(), createdSnapshot.getFlowEncodingVersion());
        assertEquals(1, createdSnapshot.getParameterContexts().size());
        assertEquals(1, createdSnapshot.getExternalControllerServices().size());

        // Retrieve the snapshot
        final VersionedFlowSnapshot retrievedSnapshot = client.getFlowSnapshotClient().get(
                createdSnapshot.getSnapshotMetadata().getFlowIdentifier(),
                createdSnapshot.getSnapshotMetadata().getVersion());
        assertNotNull(retrievedSnapshot);
        assertNotNull(retrievedSnapshot.getFlowEncodingVersion());
        assertNotNull(retrievedSnapshot.getParameterContexts());
        assertNotNull(retrievedSnapshot.getExternalControllerServices());
        assertEquals(snapshot.getFlowEncodingVersion(), retrievedSnapshot.getFlowEncodingVersion());
        assertEquals(1, retrievedSnapshot.getParameterContexts().size());
        assertEquals(1, retrievedSnapshot.getExternalControllerServices().size());
    }

    private void checkExtensionMetadata(Collection<ExtensionMetadata> extensions) {
        extensions.forEach(e -> {
            assertNotNull(e.getBundleInfo());
            assertNotNull(e.getBundleInfo().getBucketId());
            assertNotNull(e.getBundleInfo().getBucketName());
            assertNotNull(e.getBundleInfo().getBundleId());
            assertNotNull(e.getBundleInfo().getGroupId());
            assertNotNull(e.getBundleInfo().getArtifactId());
            assertNotNull(e.getBundleInfo().getVersion());
        });
    }

    private BundleVersion createExtensionBundleVersionWithStream(final Bucket bundlesBucket,
                                                                 final BundleVersionClient bundleVersionClient,
                                                                 final String narFile, final String sha256)
            throws IOException, NiFiRegistryException {

        final BundleVersion createdBundleVersion;
        try (final InputStream bundleInputStream = new FileInputStream(narFile)) {
            if (StringUtils.isBlank(sha256)) {
                createdBundleVersion = bundleVersionClient.create(
                        bundlesBucket.getIdentifier(), BundleType.NIFI_NAR, bundleInputStream);
            } else {
                createdBundleVersion = bundleVersionClient.create(
                        bundlesBucket.getIdentifier(), BundleType.NIFI_NAR, bundleInputStream, sha256);
            }
        }

        assertNotNull(createdBundleVersion);
        assertNotNull(createdBundleVersion.getBucket());
        assertNotNull(createdBundleVersion.getBundle());
        assertNotNull(createdBundleVersion.getVersionMetadata());

        return createdBundleVersion;
    }

    private BundleVersion createExtensionBundleVersionWithFile(final Bucket bundlesBucket,
                                                               final BundleVersionClient bundleVersionClient,
                                                               final String narFile, final String sha256)
            throws IOException, NiFiRegistryException {

        final BundleVersion createdBundleVersion;
        if (StringUtils.isBlank(sha256)) {
            createdBundleVersion = bundleVersionClient.create(
                    bundlesBucket.getIdentifier(), BundleType.NIFI_NAR, new File(narFile));
        } else {
            createdBundleVersion = bundleVersionClient.create(
                    bundlesBucket.getIdentifier(), BundleType.NIFI_NAR, new File(narFile), sha256);
        }

        assertNotNull(createdBundleVersion);
        assertNotNull(createdBundleVersion.getBucket());
        assertNotNull(createdBundleVersion.getBundle());
        assertNotNull(createdBundleVersion.getVersionMetadata());

        return createdBundleVersion;
    }

    private String calculateSha256Hex(final String narFile) throws IOException {
        try (final InputStream bundleInputStream = new FileInputStream(narFile)) {
            return Hex.toHexString(DigestUtils.sha256(bundleInputStream));
        }
    }

    private static Bucket createBucket(BucketClient bucketClient, int num) throws IOException, NiFiRegistryException {
        final Bucket bucket = new Bucket();
        bucket.setName("Bucket #" + num);
        bucket.setDescription("This is bucket #" + num);
        bucket.setRevision(new RevisionInfo(CLIENT_ID, 0L));
        return bucketClient.create(bucket);
    }

    private static VersionedFlow createFlow(FlowClient client, Bucket bucket, int num) throws IOException, NiFiRegistryException {
        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setName(bucket.getName() + " Flow #" + num);
        versionedFlow.setDescription("This is " + bucket.getName() + " flow #" + num);
        versionedFlow.setBucketIdentifier(bucket.getIdentifier());
        versionedFlow.setRevision(new RevisionInfo(CLIENT_ID, 0L));
        return client.create(versionedFlow);
    }

    private static VersionedFlowSnapshot buildSnapshot(VersionedFlow flow, int num) {
        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(flow.getBucketIdentifier());
        snapshotMetadata.setFlowIdentifier(flow.getIdentifier());
        snapshotMetadata.setVersion(num);
        snapshotMetadata.setComments("This is snapshot #" + num);

        final VersionedProcessGroup rootProcessGroup = new VersionedProcessGroup();
        rootProcessGroup.setIdentifier("root-pg");
        rootProcessGroup.setName("Root Process Group");

        final VersionedProcessGroup subProcessGroup = new VersionedProcessGroup();
        subProcessGroup.setIdentifier("sub-pg");
        subProcessGroup.setName("Sub Process Group");
        rootProcessGroup.getProcessGroups().add(subProcessGroup);

        final Map<String,String> processorProperties = new HashMap<>();
        processorProperties.put("Prop 1", "Val 1");
        processorProperties.put("Prop 2", "Val 2");

        final Map<String, VersionedPropertyDescriptor> propertyDescriptors = new HashMap<>();

        final VersionedProcessor processor1 = new VersionedProcessor();
        processor1.setIdentifier("p1");
        processor1.setName("Processor 1");
        processor1.setProperties(processorProperties);
        processor1.setPropertyDescriptors(propertyDescriptors);

        final VersionedProcessor processor2 = new VersionedProcessor();
        processor2.setIdentifier("p2");
        processor2.setName("Processor 2");
        processor2.setProperties(processorProperties);
        processor2.setPropertyDescriptors(propertyDescriptors);

        subProcessGroup.getProcessors().add(processor1);
        subProcessGroup.getProcessors().add(processor2);

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setSnapshotMetadata(snapshotMetadata);
        snapshot.setFlowContents(rootProcessGroup);
        return snapshot;
    }

    private static VersionedFlowSnapshot createSnapshot(FlowSnapshotClient client, VersionedFlow flow, int num) throws IOException, NiFiRegistryException {
        final VersionedFlowSnapshot snapshot = buildSnapshot(flow, num);

        return client.create(snapshot);
    }
}
