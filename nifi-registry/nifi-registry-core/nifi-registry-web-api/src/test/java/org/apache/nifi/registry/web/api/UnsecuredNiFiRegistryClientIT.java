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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.extension.ExtensionFilterParams;
import org.apache.nifi.extension.ExtensionMetadata;
import org.apache.nifi.extension.ExtensionMetadataContainer;
import org.apache.nifi.extension.TagCount;
import org.apache.nifi.extension.manifest.Extension;
import org.apache.nifi.extension.manifest.ExtensionType;
import org.apache.nifi.extension.manifest.ProvidedServiceAPI;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
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
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.registry.extension.repo.ExtensionRepoBucket;
import org.apache.nifi.registry.extension.repo.ExtensionRepoExtensionMetadata;
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersion;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.util.FileUtils;
import org.bouncycastle.util.encoders.Hex;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test all basic functionality of JerseyNiFiRegistryClient.
 */
public class UnsecuredNiFiRegistryClientIT extends UnsecuredITBase {

    static final Logger LOGGER = LoggerFactory.getLogger(UnsecuredNiFiRegistryClientIT.class);

    static final String CLIENT_ID = "UnsecuredNiFiRegistryClientIT";
    public static final String REPO_GROUP_ID = "org.apache.nifi";
    public static final String NIFI_TEST_NAR_1_0_0_PATH = "src/test/resources/extensions/nars/nifi-test-nar-1.0.0.nar";
    public static final String NIFI_TEST_NAR_2_0_0_PATH = "src/test/resources/extensions/nars/nifi-test-nar-2.0.0.nar";
    public static final String NIFI_FOO_NAR_1_0_0_PATH = "src/test/resources/extensions/nars/nifi-foo-nar-1.0.0.nar";
    public static final String NIFI_FOO_NAR_2_0_0_BUILD1_PATH = "src/test/resources/extensions/nars/nifi-foo-nar-2.0.0-SNAPSHOT-BUILD1.nar";
    public static final String NIFI_FOO_NAR_2_0_0_BUILD2_PATH = "src/test/resources/extensions/nars/nifi-foo-nar-2.0.0-SNAPSHOT-BUILD2.nar";
    public static final String NIFI_FOO_NAR_2_0_0_BUILD3_PATH = "src/test/resources/extensions/nars/nifi-foo-nar-2.0.0-SNAPSHOT-BUILD3.nar";

    private NiFiRegistryClient client;

    @BeforeEach
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

    @AfterEach
    public void teardown() {
        try {
            client.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to close Nifi Registry client", e);
        }
    }

    @Test
    public void testGetAccessStatus() throws IOException, NiFiRegistryException {
        final UserClient userClient = client.getUserClient();
        final CurrentUser currentUser = userClient.getAccessStatus();
        assertEquals("anonymous", currentUser.getIdentity());
        assertTrue(currentUser.isAnonymous());
        assertNotNull(currentUser.getResourcePermissions());
        final Permissions fullAccess = new Permissions().withCanRead(true).withCanWrite(true).withCanDelete(true);
        assertEquals(fullAccess, currentUser.getResourcePermissions().getAnyTopLevelResource());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getBuckets());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getTenants());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getPolicies());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getProxy());
    }

    @Nested
    public class BucketClientTests {

        BucketClient bucketClient;
        List<Bucket> createdBuckets;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            bucketClient = client.getBucketClient();
            createdBuckets = createBuckets();
        }

        @Test
        public void testGetAllBucket() throws IOException, NiFiRegistryException {
            for (final Bucket bucket : createdBuckets) {
                final Bucket retrievedBucket = bucketClient.get(bucket.getIdentifier());
                assertNotNull(retrievedBucket);
                assertNotNull(retrievedBucket.getRevision());
                assertFalse(retrievedBucket.isAllowBundleRedeploy());
                LOGGER.info("Retrieved bucket " + retrievedBucket.getIdentifier());
            }
        }

        @Test
        public void testGetBucketField() throws IOException, NiFiRegistryException {
            final Fields bucketFields = bucketClient.getFields();
            assertNotNull(bucketFields);
            LOGGER.info("Retrieved bucket fields, size = " + bucketFields.getFields().size());
            assertTrue(bucketFields.getFields().size() > 0);
        }

        @Test
        public void testGetAllBuckets() throws IOException, NiFiRegistryException {
            final List<Bucket> allBuckets = bucketClient.getAll();
            LOGGER.info("Retrieved buckets, size = " + allBuckets.size());
            assertEquals(createdBuckets.size(), allBuckets.size());
            for (final Bucket bucket : allBuckets) {
                assertTrue(createdBuckets.stream().anyMatch(createdBucket -> createdBucket.getIdentifier().equals(bucket.getIdentifier())));
            }
        }

        @Test
        public void testUpdateEachBucket() throws IOException, NiFiRegistryException {
            for (final Bucket bucket : createdBuckets) {
                final Bucket bucketUpdate = new Bucket();
                bucketUpdate.setIdentifier(bucket.getIdentifier());
                final String newDescription = bucket.getDescription() + " UPDATE";
                bucketUpdate.setDescription(newDescription);
                bucketUpdate.setRevision(bucket.getRevision());

                final Bucket updatedBucket = bucketClient.update(bucketUpdate);
                assertNotNull(updatedBucket);
                assertEquals(newDescription, updatedBucket.getDescription());
                LOGGER.info("Updated bucket " + updatedBucket.getIdentifier());
            }
        }

        @Test
        public void testDeleteBucket() throws IOException, NiFiRegistryException {
            final Bucket bucket = createdBuckets.get(0);
            final Bucket deletedBucket = bucketClient.delete(bucket.getIdentifier(), bucket.getRevision());
            assertNotNull(deletedBucket);
        }

        @Test
        public void testDeleteBucketWithoutRevisionInfo() {
            final Bucket bucket = createdBuckets.get(0);
            // NOTE: I do not know if this is the intended behavior but previously this method overload had no coverage
            // If it's not a backward breaking change maybe we should remove this overload or make it deprecated and
            // throw an Exception without actually querying the server.
            final NiFiRegistryException exception = assertThrows(NiFiRegistryException.class,
                    () -> bucketClient.delete(bucket.getIdentifier()));
            assertEquals("Error deleting bucket: Revision info must be specified.", exception.getMessage());
        }
    }

    @Nested
    public class FlowClientTests {

        FlowClient flowClient;
        Bucket bucket;
        VersionedFlow flow1;
        VersionedFlow flow2;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            flowClient = client.getFlowClient();
            bucket = createBucket(0);
            flow1 = createFlow(bucket, 1);
            flow2 = createFlow(bucket, 2);
        }

        @Test
        public void testGetFlow() throws IOException, NiFiRegistryException {
            final VersionedFlow retrievedFlow1 = flowClient.get(bucket.getIdentifier(), flow1.getIdentifier());
            assertNotNull(retrievedFlow1);
            LOGGER.info("Retrieved flow # 1 with id " + retrievedFlow1.getIdentifier());

            final VersionedFlow retrievedFlow2 = flowClient.get(bucket.getIdentifier(), flow2.getIdentifier());
            assertNotNull(retrievedFlow2);
            LOGGER.info("Retrieved flow # 2 with id " + retrievedFlow2.getIdentifier());
        }

        @Test
        public void testGetFlowWithoutBucket() throws IOException, NiFiRegistryException {
            final VersionedFlow retrievedFlow1WithoutBucket = flowClient.get(flow1.getIdentifier());
            assertNotNull(retrievedFlow1WithoutBucket);
            assertEquals(flow1.getIdentifier(), retrievedFlow1WithoutBucket.getIdentifier());
            LOGGER.info("Retrieved flow # 1 without bucket id, with id " + retrievedFlow1WithoutBucket.getIdentifier());
        }

        @Test
        public void testUpdateFlow() throws IOException, NiFiRegistryException {
            final VersionedFlow retrievedFlow1 = flowClient.get(bucket.getIdentifier(), flow1.getIdentifier());
            assertNotNull(retrievedFlow1);
            LOGGER.info("Retrieved flow # 1 with id " + retrievedFlow1.getIdentifier());

            final VersionedFlow flow1Update = new VersionedFlow();
            flow1Update.setIdentifier(flow1.getIdentifier());
            final String newName = flow1.getName() + " UPDATED";
            flow1Update.setName(newName);
            flow1Update.setRevision(retrievedFlow1.getRevision());

            final VersionedFlow updatedFlow1 = flowClient.update(bucket.getIdentifier(), flow1Update);
            assertNotNull(updatedFlow1);
            assertEquals(newName, updatedFlow1.getName());
            LOGGER.info("Updated flow # 1 with id " + updatedFlow1.getIdentifier());
        }

        @Test
        public void testGetFlowFields() throws IOException, NiFiRegistryException {
            final Fields flowFields = flowClient.getFields();
            assertNotNull(flowFields);
            LOGGER.info("Retrieved flow fields, size = " + flowFields.getFields().size());
            assertTrue(flowFields.getFields().size() > 0);
        }

        @Test
        public void testGetBucketsInFlow() throws IOException, NiFiRegistryException {
            final List<VersionedFlow> flowsInBucket = flowClient.getByBucket(bucket.getIdentifier());
            assertNotNull(flowsInBucket);
            assertEquals(2, flowsInBucket.size());
            flowsInBucket.forEach(f -> LOGGER.info("Flow in bucket, flow id " + f.getIdentifier()));
        }

        @Test
        public void testDeleteFlow() {
            final VersionedFlow deletedFlow1 = assertDoesNotThrow(() -> flowClient.delete(bucket.getIdentifier(),
                    flow1.getIdentifier(),
                    flow1.getRevision()));
            assertNotNull(deletedFlow1);
        }

        @Test
        public void testDeleteFlowWithoutRevisionInfo() {
            // NOTE: I do not know if this is the intended behavior but previously this method overload had no coverage
            // If it's not a backward breaking change maybe we should remove this overload or make it deprecated and
            // throw an Exception without actually querying the server.
            final NiFiRegistryException exception = assertThrows(NiFiRegistryException.class,
                    () -> flowClient.delete(bucket.getIdentifier(), flow1.getIdentifier()));
            assertEquals("Error deleting flow: Revision info must be specified.", exception.getMessage());
        }
    }

    @Nested
    public class FlowSnapshotClientTests {

        FlowSnapshotClient snapshotClient;
        Bucket bucket;
        VersionedFlow flow;
        VersionedFlowSnapshot snapshot1;
        VersionedFlowSnapshot snapshot2;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            snapshotClient = client.getFlowSnapshotClient();
            bucket = createBucket(0);
            flow = createFlow(bucket, 0);
            snapshot1 = createSnapshot(flow, 1);
            snapshot2 = createSnapshot(flow, 2);
        }

        @Test
        public void testGetSnapshot() throws IOException, NiFiRegistryException {
            final VersionedFlowSnapshot retrievedSnapshot1 = snapshotClient.get(flow.getBucketIdentifier(), flow.getIdentifier(), 1);
            assertNotNull(retrievedSnapshot1);
            assertFalse(retrievedSnapshot1.isLatest());
            LOGGER.info("Retrieved snapshot # 1 with version " + retrievedSnapshot1.getSnapshotMetadata().getVersion());

            final VersionedFlowSnapshot retrievedSnapshot2 = snapshotClient.get(flow.getBucketIdentifier(), flow.getIdentifier(), 2);
            assertNotNull(retrievedSnapshot2);
            assertTrue(retrievedSnapshot2.isLatest());
            LOGGER.info("Retrieved snapshot # 2 with version " + retrievedSnapshot2.getSnapshotMetadata().getVersion());
        }

        @Test
        public void testGetSnapshotWithoutBucket() throws IOException, NiFiRegistryException {
            final VersionedFlowSnapshot retrievedSnapshot1WithoutBucket = snapshotClient.get(flow.getIdentifier(), 1);
            assertNotNull(retrievedSnapshot1WithoutBucket);
            assertFalse(retrievedSnapshot1WithoutBucket.isLatest());
            assertEquals(flow.getIdentifier(), retrievedSnapshot1WithoutBucket.getSnapshotMetadata().getFlowIdentifier());
            assertEquals(1, retrievedSnapshot1WithoutBucket.getSnapshotMetadata().getVersion());
            LOGGER.info("Retrieved snapshot # 1 without using bucket id, with version " + retrievedSnapshot1WithoutBucket.getSnapshotMetadata().getVersion());
        }

        @Test
        public void testLatestSnapshot() throws IOException, NiFiRegistryException {
            final VersionedFlowSnapshot retrievedSnapshotLatest = snapshotClient.getLatest(flow.getBucketIdentifier(), flow.getIdentifier());
            assertNotNull(retrievedSnapshotLatest);
            assertEquals(snapshot2.getSnapshotMetadata().getVersion(), retrievedSnapshotLatest.getSnapshotMetadata().getVersion());
            assertTrue(retrievedSnapshotLatest.isLatest());
            LOGGER.info("Retrieved latest snapshot with version " + retrievedSnapshotLatest.getSnapshotMetadata().getVersion());

            // get latest without bucket
            final VersionedFlowSnapshot retrievedSnapshotLatestWithoutBucket = snapshotClient.getLatest(flow.getIdentifier());
            assertNotNull(retrievedSnapshotLatestWithoutBucket);
            assertEquals(snapshot2.getSnapshotMetadata().getVersion(), retrievedSnapshotLatestWithoutBucket.getSnapshotMetadata().getVersion());
            assertTrue(retrievedSnapshotLatestWithoutBucket.isLatest());
            LOGGER.info("Retrieved latest snapshot without bucket, with version " + retrievedSnapshotLatestWithoutBucket.getSnapshotMetadata().getVersion());
        }

        @Test
        public void testGetMetadata() throws IOException, NiFiRegistryException {
            final List<VersionedFlowSnapshotMetadata> retrievedMetadata = snapshotClient.getSnapshotMetadata(flow.getBucketIdentifier(), flow.getIdentifier());
            assertNotNull(retrievedMetadata);
            assertEquals(2, retrievedMetadata.size());
            assertEquals(2, retrievedMetadata.get(0).getVersion());
            assertEquals(1, retrievedMetadata.get(1).getVersion());
            retrievedMetadata.forEach(s -> LOGGER.info("Retrieved snapshot metadata " + s.getVersion()));
        }

        @Test
        public void testGetMetadataWithoutBucket() throws IOException, NiFiRegistryException {
            final List<VersionedFlowSnapshotMetadata> retrievedMetadataWithoutBucket = snapshotClient.getSnapshotMetadata(flow.getIdentifier());
            assertNotNull(retrievedMetadataWithoutBucket);
            assertEquals(2, retrievedMetadataWithoutBucket.size());
            assertEquals(2, retrievedMetadataWithoutBucket.get(0).getVersion());
            assertEquals(1, retrievedMetadataWithoutBucket.get(1).getVersion());
            retrievedMetadataWithoutBucket.forEach(s -> LOGGER.info("Retrieved snapshot metadata " + s.getVersion()));
        }

        @Test
        public void testLatestMetadata() throws IOException, NiFiRegistryException {
            final VersionedFlowSnapshotMetadata latestMetadata = snapshotClient.getLatestMetadata(flow.getBucketIdentifier(), flow.getIdentifier());
            assertNotNull(latestMetadata);
            assertEquals(2, latestMetadata.getVersion());
        }

        @Test
        public void testLatestMetadataThatDoesNotExist() {
            final NiFiRegistryException nfe = assertThrows(NiFiRegistryException.class,
                    () -> snapshotClient.getLatestMetadata(flow.getBucketIdentifier(), "DOES-NOT-EXIST"));
            assertEquals("Error retrieving latest snapshot metadata: The specified flow ID does not exist in this bucket.", nfe.getMessage());
        }

        @Test
        public void testLatestMetadataWithoutBucket() throws IOException, NiFiRegistryException {
            final VersionedFlowSnapshotMetadata latestMetadataWithoutBucket = snapshotClient.getLatestMetadata(flow.getIdentifier());
            assertNotNull(latestMetadataWithoutBucket);
            assertEquals(flow.getIdentifier(), latestMetadataWithoutBucket.getFlowIdentifier());
            assertEquals(2, latestMetadataWithoutBucket.getVersion());
        }

        @Test
        public void testDiff() throws IOException, NiFiRegistryException {
            final VersionedFlowSnapshot snapshot = buildSnapshot(flow, 3);
            final VersionedProcessGroup newlyAddedPG = new VersionedProcessGroup();
            newlyAddedPG.setIdentifier("new-pg");
            newlyAddedPG.setName("NEW Process Group");
            snapshot.getFlowContents().getProcessGroups().add(newlyAddedPG);
            client.getFlowSnapshotClient().create(snapshot);

            final VersionedFlowDifference diff = client.getFlowClient().diff(flow.getBucketIdentifier(), flow.getIdentifier(), 3, 2);
            assertNotNull(diff);
            assertEquals(1, diff.getComponentDifferenceGroups().size());
        }
    }

    @Nested
    public class BundleClientTests {

        BundleClient bundleClient;
        BundleVersionClient bundleVersionClient;
        Bucket bucket;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            bundleClient = client.getBundleClient();
            bundleVersionClient = client.getBundleVersionClient();
            bucket = createBucket(0);
        }

        @Test
        public void testCreateNifiTestNarBundleVersion1() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            assertEquals(REPO_GROUP_ID, createdBundle.getGroupId());
            assertEquals("nifi-test-nar", createdBundle.getArtifactId());
            assertEquals(BundleType.NIFI_NAR, createdBundle.getBundleType());
            assertEquals(1, createdBundle.getVersionCount());

            assertEquals("org.apache.nifi:nifi-test-nar", createdBundle.getName());
            assertEquals(bucket.getIdentifier(), createdBundle.getBucketIdentifier());
            assertEquals(bucket.getName(), createdBundle.getBucketName());
            assertNotNull(createdBundle.getPermissions());
            assertTrue(createdBundle.getCreatedTimestamp() > 0);
            assertTrue(createdBundle.getModifiedTimestamp() > 0);

            final BundleVersionMetadata createdBundleVersionVersionMetadata = createdBundleVersion.getVersionMetadata();
            assertEquals("1.0.0", createdBundleVersionVersionMetadata.getVersion());
            assertNotNull(createdBundleVersionVersionMetadata.getId());
            assertNotNull(createdBundleVersionVersionMetadata.getSha256());
            assertNotNull(createdBundleVersionVersionMetadata.getAuthor());
            assertEquals(createdBundle.getIdentifier(), createdBundleVersionVersionMetadata.getBundleId());
            assertEquals(bucket.getIdentifier(), createdBundleVersionVersionMetadata.getBucketId());
            assertTrue(createdBundleVersionVersionMetadata.getTimestamp() > 0);
            assertFalse(createdBundleVersionVersionMetadata.getSha256Supplied());
            assertTrue(createdBundleVersionVersionMetadata.getContentSize() > 1);

            final BuildInfo createdBuildInfo = createdBundleVersionVersionMetadata.getBuildInfo();
            assertNotNull(createdBuildInfo);
            assertTrue(createdBuildInfo.getBuilt() > 0);
            assertNotNull(createdBuildInfo.getBuildTool());
            assertNotNull(createdBuildInfo.getBuildRevision());

            final Set<BundleVersionDependency> bundleDependencies = createdBundleVersion.getDependencies();
            assertNotNull(bundleDependencies);
            assertEquals(1, bundleDependencies.size());

            final BundleVersionDependency bundleDependency = bundleDependencies.stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("No Bundle dependency found"));
            assertEquals(REPO_GROUP_ID, bundleDependency.getGroupId());
            assertEquals("nifi-test-api-nar", bundleDependency.getArtifactId());
            assertEquals("1.0.0", bundleDependency.getVersion());
        }

        @Test
        public void testCreatingBundleWithSha256() throws IOException, NiFiRegistryException {
            final File testNarFile = new File(NIFI_TEST_NAR_1_0_0_PATH);
            try (final InputStream inputStream = new FileInputStream(testNarFile)) {
                final String sha256Hex = DigestUtils.sha256Hex(inputStream);
                final BundleVersion bundleVersion = createExtensionBundleVersionWithFile(bucket,
                        bundleVersionClient,
                        NIFI_TEST_NAR_1_0_0_PATH,
                        sha256Hex);
                assertNotNull(bundleVersion);
            }
        }

        @Test
        public void testBundleUpgradeFailsWithInvalidSha256() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final String madeUpSha256 = "MADE-UP-SHA-256";
            try {
                createNifiTestNarV2ExtensionBundle(bucket, madeUpSha256);
                fail("Should have thrown exception");
            } catch (Exception e) {
                LOGGER.info("Create extension bundle thrown exception as expected");
            }
        }

        @Test
        public void testBundleUpgrade() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final BundleVersion upgradedBundleVersion = createNifiTestNarV2ExtensionBundle(bucket);
            assertTrue(upgradedBundleVersion.getVersionMetadata().getSha256Supplied());

            final Bundle upgradedBundle = upgradedBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", upgradedBundle.getIdentifier());
            assertEquals(2, upgradedBundle.getVersionCount());
        }

        @Test
        public void testNifiFooNarBundleRedeployFails() throws IOException, NiFiRegistryException {
            assertFalse(bucket.isAllowBundleRedeploy());

            final BundleVersion createdBundleVersion = createNifiFooNarV1ExtensionBundle(bucket);
            assertFalse(createdBundleVersion.getVersionMetadata().getSha256Supplied());

            try {
                createNifiFooNarV1ExtensionBundle(bucket);
                fail("Should have thrown exception when re-deploying foo nar");
            } catch (Exception e) {
                LOGGER.info("Redeploy extension bundle fails as expected");
            }
        }

        @Test
        public void testNifiFooNarBundleRedeploy() throws IOException, NiFiRegistryException {
            bucket.setAllowBundleRedeploy(true);
            final BundleVersion createdBundleVersion = createNifiFooNarV1ExtensionBundle(bucket);
            assertFalse(createdBundleVersion.getVersionMetadata().getSha256Supplied());

            final Bucket updatedBundlesBucket = client.getBucketClient().update(bucket);
            assertTrue(updatedBundlesBucket.isAllowBundleRedeploy());

            // try to re-deploy version 1.0.0 of nifi-foo-nar again, this time should work
            final BundleVersion redeployedExtension = createNifiFooNarV1ExtensionBundle(bucket);
            assertNotNull(redeployedExtension);

            // verify there are only one bundle
            final List<Bundle> allBundlesAfterRedeploy = bundleClient.getAll();
            assertEquals(1, allBundlesAfterRedeploy.size());
        }

        @Test
        public void testSnapshotsCanHaveDifferentChecksums() throws IOException, NiFiRegistryException {
            final BundleVersion createdBuild1BundleVersion = createNifiFooNarV2Build1ExtensionBundle(bucket);
            assertFalse(createdBuild1BundleVersion.getVersionMetadata().getSha256Supplied());

            final Bucket newBucket = createBucket(1);
            final BundleVersion createdBuild2BundleVersion = createNifiFooNarV2Build2ExtensionBundle(newBucket);
            assertFalse(createdBuild2BundleVersion.getVersionMetadata().getSha256Supplied());
        }

        @Test
        public void testSnapshotsCanBeOverwritten() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiFooNarV2Build2ExtensionBundle(bucket);
            assertFalse(createdBundleVersion.getVersionMetadata().getSha256Supplied());

            final BundleVersion overwrittenBundleVersion = createNifiFooNarV2Build3ExtensionBundle(bucket);
            assertFalse(overwrittenBundleVersion.getVersionMetadata().getSha256Supplied());

            final BundleVersion retrievedBundleVersion = bundleVersionClient.getBundleVersion(
                    overwrittenBundleVersion.getVersionMetadata().getBundleId(),
                    overwrittenBundleVersion.getVersionMetadata().getVersion());
            assertEquals(calculateSha256Hex(NIFI_FOO_NAR_2_0_0_BUILD3_PATH), retrievedBundleVersion.getVersionMetadata().getSha256());

            final List<ExtensionMetadata> extensions = bundleVersionClient.getExtensions(
                    overwrittenBundleVersion.getVersionMetadata().getBundleId(),
                    overwrittenBundleVersion.getVersionMetadata().getVersion());
            assertNotNull(extensions);
            assertEquals(1, extensions.size());
            checkExtensionMetadata(extensions);

            final String extensionName = extensions.get(0).getName();
            final Extension extension = bundleVersionClient.getExtension(
                    overwrittenBundleVersion.getVersionMetadata().getBundleId(),
                    overwrittenBundleVersion.getVersionMetadata().getVersion(),
                    extensionName
            );
            assertNotNull(extension);
            assertEquals(extensionName, extension.getName());

            try (final InputStream docsInput = bundleVersionClient.getExtensionDocs(
                    overwrittenBundleVersion.getVersionMetadata().getBundleId(),
                    overwrittenBundleVersion.getVersionMetadata().getVersion(),
                    extensionName
            )) {
                final String docsContent = IOUtils.toString(docsInput, StandardCharsets.UTF_8);
                assertNotNull(docsContent);
                assertTrue(docsContent.startsWith("<!DOCTYPE html>"));
            }
        }

        @Test
        public void testGetBundlesByBucket() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            assertEquals(1, bundleClient.getByBucket(bucket.getIdentifier()).size());
        }

        @Test
        public void testGetBundlesById() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final Bundle retrievedBundle = bundleClient.get(createdBundle.getIdentifier());
            assertNotNull(retrievedBundle);
            assertEquals(createdBundle.getIdentifier(), retrievedBundle.getIdentifier());
            assertEquals(createdBundle.getGroupId(), retrievedBundle.getGroupId());
            assertEquals(createdBundle.getArtifactId(), retrievedBundle.getArtifactId());
        }

        @Test
        public void testGetListOfVersionMetadataOfBundle() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final BundleVersion upgradedBundleVersion = createNifiTestNarV2ExtensionBundle(bucket);
            assertTrue(upgradedBundleVersion.getVersionMetadata().getSha256Supplied());

            final List<BundleVersionMetadata> bundleVersions = bundleVersionClient.getBundleVersions(createdBundle.getIdentifier());
            assertNotNull(bundleVersions);
            assertEquals(2, bundleVersions.size());
        }

        @Test
        public void testGetBundleVersionByBundleIdAndVersion() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final BundleVersion upgradedBundleVersion = createNifiTestNarV2ExtensionBundle(bucket);
            assertTrue(upgradedBundleVersion.getVersionMetadata().getSha256Supplied());

            final BundleVersion bundleVersion1 = bundleVersionClient.getBundleVersion(createdBundle.getIdentifier(), "1.0.0");
            assertNotNull(bundleVersion1);
            assertEquals("1.0.0", bundleVersion1.getVersionMetadata().getVersion());
            assertNotNull(bundleVersion1.getDependencies());
            assertEquals(1, bundleVersion1.getDependencies().size());

            final BundleVersion bundleVersion2 = bundleVersionClient.getBundleVersion(createdBundle.getIdentifier(), "2.0.0");
            assertNotNull(bundleVersion2);
            assertEquals("2.0.0", bundleVersion2.getVersionMetadata().getVersion());
        }

        @Test
        public void testGetContentOfBundle() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            try (final InputStream bundleVersion1InputStream = bundleVersionClient.getBundleVersionContent(createdBundle.getIdentifier(), "1.0.0")) {
                final String sha256Hex = DigestUtils.sha256Hex(bundleVersion1InputStream);
                final BundleVersionMetadata testNarV1Metadata = createdBundleVersion.getVersionMetadata();
                assertEquals(testNarV1Metadata.getSha256(), sha256Hex);
            }
        }

        @Test
        public void testWriteBundleVersionToFile() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final File targetDir = new File("./target");
            final File bundleFile = bundleVersionClient.writeBundleVersionContent(createdBundle.getIdentifier(), "1.0.0", targetDir);
            assertNotNull(bundleFile);

            try (final InputStream bundleInputStream = new FileInputStream(bundleFile)) {
                final String sha256Hex = DigestUtils.sha256Hex(bundleInputStream);
                final BundleVersionMetadata testNarV1Metadata = createdBundleVersion.getVersionMetadata();
                assertEquals(testNarV1Metadata.getSha256(), sha256Hex);
            }
        }

        @Test
        public void testDeleteBundleVersion() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle createdBundle = createdBundleVersion.getBundle();
            LOGGER.info("Created bundle with id {}", createdBundle.getIdentifier());

            final BundleVersion upgradedBundleVersion = createNifiTestNarV2ExtensionBundle(bucket);
            assertTrue(upgradedBundleVersion.getVersionMetadata().getSha256Supplied());

            final BundleVersion deletedBundleVersion = bundleVersionClient.delete(createdBundle.getIdentifier(), "2.0.0");
            assertNotNull(deletedBundleVersion);
            assertEquals(createdBundle.getIdentifier(), deletedBundleVersion.getBundle().getIdentifier());
            assertEquals("2.0.0", deletedBundleVersion.getVersionMetadata().getVersion());

            try {
                bundleVersionClient.getBundleVersion(createdBundle.getIdentifier(), "2.0.0");
                fail("Should have thrown exception");
            } catch (Exception e) {
                LOGGER.info("Getting deleted bundle version fails as expected");
            }

            final BundleVersion bundleVersion1 = bundleVersionClient.getBundleVersion(createdBundle.getIdentifier(), "1.0.0");
            assertNotNull(bundleVersion1);
        }

        @Test
        public void testDeleteBundle() throws IOException, NiFiRegistryException {
            final BundleVersion createdBundleVersion = createNifiTestNarV1ExtensionBundle(bucket);
            final Bundle deletedBundle = bundleClient.delete(createdBundleVersion.getBundle().getIdentifier());
            assertNotNull(deletedBundle);

            try {
                bundleClient.get(createdBundleVersion.getBundle().getIdentifier());
                fail("Should have thrown exception");
            } catch (Exception e) {
                LOGGER.info("Getting deleted bundle fails as expected");
            }
        }

        @Test
        public void testGetBundlesWithEmptyFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiFooNarV1ExtensionBundle(bucket);
            assertEquals(2, bundleClient.getAll(BundleFilterParams.empty()).size());
        }

        @Test
        public void testGetBundlesWithFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiFooNarV1ExtensionBundle(bucket);

            final BundleFilterParams filterParams = BundleFilterParams.of(REPO_GROUP_ID, "nifi-test-nar");
            final List<Bundle> filteredBundles = bundleClient.getAll(filterParams);
            assertEquals(1, filteredBundles.size());
        }

        @Test
        public void testGetBundlesWithBucketNameFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiFooNarV1ExtensionBundle(bucket);

            final BundleFilterParams filterParams = BundleFilterParams.of(bucket.getName(), REPO_GROUP_ID, "nifi-test-nar");
            final List<Bundle> filteredBundles = bundleClient.getAll(filterParams);
            assertEquals(1, filteredBundles.size());
        }

        @Test
        public void testGetBundleVersionsWithoutFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiTestNarV2ExtensionBundle(bucket);

            final List<BundleVersionMetadata> bundles = bundleVersionClient.getBundleVersions((BundleVersionFilterParams) null);
            assertEquals(2, bundles.size());
        }

        @Test
        public void testGetBundleVersionsWithEmptyFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiTestNarV2ExtensionBundle(bucket);

            assertEquals(2, bundleVersionClient.getBundleVersions(BundleVersionFilterParams.empty()).size());
        }

        @Test
        public void testGetBundleVersionsWithFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiTestNarV2ExtensionBundle(bucket);

            final BundleVersionFilterParams filterParams = BundleVersionFilterParams.of(REPO_GROUP_ID, "nifi-test-nar", "1.0.0");
            final List<BundleVersionMetadata> filteredVersions = bundleVersionClient.getBundleVersions(filterParams);
            assertEquals(1, filteredVersions.size());
        }

        @Test
        public void testGetBundleVersionsWithOnlyGroupFilter() throws IOException, NiFiRegistryException {
            createNifiTestNarV1ExtensionBundle(bucket);
            createNifiTestNarV2ExtensionBundle(bucket);
            createNifiFooNarV1ExtensionBundle(bucket);

            final BundleVersionFilterParams filterParams = BundleVersionFilterParams.of(REPO_GROUP_ID, null, null);
            final List<BundleVersionMetadata> filteredVersions = bundleVersionClient.getBundleVersions(filterParams);
            assertEquals(3, filteredVersions.size());

            filteredVersions.forEach(bvm -> {
                assertNotNull(bvm.getGroupId());
                assertNotNull(bvm.getArtifactId());
                assertNotNull(bvm.getVersion());
            });
        }
    }

    @Nested
    public class ExtensionRepoClientTests {

        public static final String REPO_ARTIFACT_ID = "nifi-test-nar";
        public static final String REPO_VERSION = "1.0.0";

        ExtensionRepoClient extensionRepoClient;

        Bucket bundleBucket;
        BundleVersion testNarV1BundleVersion;
        BundleVersion createdFooNarV2SnapshotB2;
        BundleVersion createdFooNarV2SnapshotB3;
        Bundle testNarBundle;
        Bundle fooNarBundle;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            extensionRepoClient = client.getExtensionRepoClient();
            bundleBucket = createBucket(0);

            testNarV1BundleVersion = createNifiTestNarV1ExtensionBundle(bundleBucket);
            testNarBundle = testNarV1BundleVersion.getBundle();
            createNifiTestNarV2ExtensionBundle(bundleBucket);

            fooNarBundle = createNifiFooNarV1ExtensionBundle(bundleBucket).getBundle();
            createNifiFooNarV2Build1ExtensionBundle(bundleBucket);
            createdFooNarV2SnapshotB2 = createNifiFooNarV2Build2ExtensionBundle(bundleBucket);
            createdFooNarV2SnapshotB3 = createNifiFooNarV2Build3ExtensionBundle(bundleBucket);
        }

        @Test
        public void testExtensionRepoBucketsAreTheSameAsCreatedBuckets() throws IOException, NiFiRegistryException {
            final List<ExtensionRepoBucket> repoBuckets = extensionRepoClient.getBuckets();
            assertEquals(1, repoBuckets.size());
        }

        @Test
        public void testGetGroups() throws IOException, NiFiRegistryException {
            final String bundlesBucketName = bundleBucket.getName();
            final List<ExtensionRepoGroup> repoGroups = extensionRepoClient.getGroups(bundlesBucketName);
            assertEquals(1, repoGroups.size());

            final ExtensionRepoGroup repoGroup = repoGroups.get(0);
            assertEquals(REPO_GROUP_ID, repoGroup.getGroupId());
        }

        @Test
        public void testGetRepoArtifacts() throws IOException, NiFiRegistryException {
            final List<ExtensionRepoArtifact> repoArtifacts = extensionRepoClient.getArtifacts(bundleBucket.getName(), REPO_GROUP_ID);
            assertEquals(2, repoArtifacts.size());
        }

        @Test
        public void testGetVersions() throws IOException, NiFiRegistryException {
            final List<ExtensionRepoVersionSummary> repoVersions = extensionRepoClient.getVersions(bundleBucket.getName(),
                    REPO_GROUP_ID,
                    REPO_ARTIFACT_ID);
            assertEquals(2, repoVersions.size());
        }

        @Test
        public void testGetVersion() throws IOException, NiFiRegistryException {
            final ExtensionRepoVersion repoVersion = extensionRepoClient.getVersion(bundleBucket.getName(),
                    REPO_GROUP_ID,
                    REPO_ARTIFACT_ID,
                    REPO_VERSION);
            assertNotNull(repoVersion);
            assertNotNull(repoVersion.getDownloadLink());
            assertNotNull(repoVersion.getSha256Link());
            assertNotNull(repoVersion.getExtensionsLink());
        }

        @Test
        public void getVersionLinksForContentAndSha256() throws IOException, NiFiRegistryException {
            final Client jerseyClient = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();

            final ExtensionRepoVersion repoVersion = extensionRepoClient.getVersion(bundleBucket.getName(),
                    REPO_GROUP_ID,
                    REPO_ARTIFACT_ID,
                    REPO_VERSION);

            final WebTarget downloadLinkTarget = jerseyClient.target(repoVersion.getDownloadLink().getUri());
            try (final InputStream downloadLinkInputStream = downloadLinkTarget.request()
                    .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .get()
                    .readEntity(InputStream.class)) {
                final String sha256DownloadResult = DigestUtils.sha256Hex(downloadLinkInputStream);

                final WebTarget sha256LinkTarget = jerseyClient.target(repoVersion.getSha256Link().getUri());
                final String sha256LinkResult = sha256LinkTarget.request().get(String.class);
                assertEquals(sha256DownloadResult, sha256LinkResult);
            }
        }

        @Test
        public void verifyVersionLinkForExtensionMetadataWorks() throws IOException, NiFiRegistryException {
            final Client jerseyClient = ClientBuilder.newBuilder().register(MultiPartFeature.class).build();
            final ExtensionRepoVersion repoVersion = extensionRepoClient.getVersion(bundleBucket.getName(),
                    REPO_GROUP_ID,
                    REPO_ARTIFACT_ID,
                    REPO_VERSION);

            final WebTarget extensionsLinkTarget = jerseyClient.target(repoVersion.getExtensionsLink().getUri());
            final ExtensionRepoExtensionMetadata[] extensions = extensionsLinkTarget.request()
                    .accept(MediaType.APPLICATION_JSON)
                    .get(ExtensionRepoExtensionMetadata[].class);
            assertNotNull(extensions);
            assertTrue(extensions.length > 0);
            checkExtensionMetadata(Stream.of(extensions).map(ExtensionRepoExtensionMetadata::getExtensionMetadata).collect(Collectors.toSet()));
            Stream.of(extensions).forEach(e -> {
                assertNotNull(e.getLink());
                assertNotNull(e.getLinkDocs());
            });
        }

        @Test
        public void verifyClientMethodsForContentInputStream() throws IOException, NiFiRegistryException {
            try (final InputStream repoVersionInputStream = extensionRepoClient.getVersionContent(bundleBucket.getName(),
                    REPO_GROUP_ID,
                    REPO_ARTIFACT_ID,
                    REPO_VERSION)) {
                final String sha256Hex = DigestUtils.sha256Hex(repoVersionInputStream);

                final String repoSha256Hex = extensionRepoClient.getVersionSha256(bundleBucket.getName(),
                        REPO_GROUP_ID,
                        REPO_ARTIFACT_ID,
                        REPO_VERSION);
                assertEquals(sha256Hex, repoSha256Hex);

                final Optional<String> repoSha256HexOptional = extensionRepoClient.getVersionSha256(REPO_GROUP_ID,
                        REPO_ARTIFACT_ID,
                        REPO_VERSION);
                assertTrue(repoSha256HexOptional.isPresent());
                assertEquals(sha256Hex, repoSha256HexOptional.get());

                final List<ExtensionRepoExtensionMetadata> extensionList = extensionRepoClient.getVersionExtensions(bundleBucket.getName(),
                        REPO_GROUP_ID,
                        REPO_ARTIFACT_ID,
                        REPO_VERSION);
                assertNotNull(extensionList);
                assertTrue(extensionList.size() > 0);
                extensionList.forEach(em -> {
                    assertNotNull(em.getExtensionMetadata());
                    assertNotNull(em.getLink());
                    assertNotNull(em.getLinkDocs());
                });

                final String extensionName = extensionList.get(0).getExtensionMetadata().getName();
                final Extension extension = extensionRepoClient.getVersionExtension(
                        bundleBucket.getName(),
                        REPO_GROUP_ID,
                        REPO_ARTIFACT_ID,
                        REPO_VERSION,
                        extensionName);
                assertNotNull(extension);
                assertEquals(extensionName, extension.getName());

                // verify getting the docs for an extension from extension repo
                try (final InputStream docsInput = extensionRepoClient.getVersionExtensionDocs(
                        bundleBucket.getName(),
                        REPO_GROUP_ID,
                        REPO_ARTIFACT_ID,
                        REPO_VERSION,
                        extensionName)
                ) {
                    final String docsContent = IOUtils.toString(docsInput, StandardCharsets.UTF_8);
                    assertNotNull(docsContent);
                    assertTrue(docsContent.startsWith("<!DOCTYPE html>"));
                }
            }
        }

        @Test
        public void testSha256DoesNotExist() throws IOException, NiFiRegistryException {
            final Optional<String> repoSha256HexDoesNotExist = extensionRepoClient.getVersionSha256(REPO_GROUP_ID, REPO_ARTIFACT_ID, "DOES-NOT-EXIST");
            assertFalse(repoSha256HexDoesNotExist.isPresent());
        }

        @Test
        public void testCorrectShaIsRetrieved() throws IOException, NiFiRegistryException {
            final Optional<String> fooNarV2SnapshotLatestSha = extensionRepoClient.getVersionSha256(
                    createdFooNarV2SnapshotB3.getBundle().getGroupId(),
                    createdFooNarV2SnapshotB3.getBundle().getArtifactId(),
                    createdFooNarV2SnapshotB2.getVersionMetadata().getVersion());
            assertTrue(fooNarV2SnapshotLatestSha.isPresent());
            assertEquals(calculateSha256Hex(NIFI_FOO_NAR_2_0_0_BUILD3_PATH), fooNarV2SnapshotLatestSha.get());
        }

        @Test
        public void testWriteBundleVersionContent() throws IOException, NiFiRegistryException {
            final File bundleVersionFile = extensionRepoClient.writeBundleVersionContent(bundleBucket.getName(),
                    REPO_GROUP_ID,
                    REPO_ARTIFACT_ID,
                    REPO_VERSION,
                    new File("./target"));
            assertNotNull(bundleVersionFile);

            try (final InputStream inputStream = new FileInputStream(bundleVersionFile)) {
                final String sha256Hex = DigestUtils.sha256Hex(inputStream);
                assertEquals(testNarV1BundleVersion.getVersionMetadata().getSha256(), sha256Hex);
            }
        }
    }

    @Nested
    public class ExtensionClientTests {

        ExtensionClient extensionClient;
        Bucket bundleBucket;
        Bundle testNarBundle;
        Bundle fooNarBundle;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            extensionClient = client.getExtensionClient();

            bundleBucket = createBucket(1);
            testNarBundle = createNifiTestNarV1ExtensionBundle(bundleBucket).getBundle();
            createNifiTestNarV2ExtensionBundle(bundleBucket);

            fooNarBundle = createNifiFooNarV1ExtensionBundle(bundleBucket).getBundle();
            createNifiFooNarV2Build1ExtensionBundle(bundleBucket);
            createNifiFooNarV2Build2ExtensionBundle(bundleBucket);
            createNifiFooNarV2Build3ExtensionBundle(bundleBucket);
        }

        @Test
        public void testGetTags() throws IOException, NiFiRegistryException {
            final List<TagCount> tagCounts = extensionClient.getTagCounts();
            assertNotNull(tagCounts);
            assertTrue(tagCounts.size() > 0);
            tagCounts.forEach(tc -> {
                assertNotNull(tc.getTag());
            });
        }

        @Test
        public void testGetAllExtension() throws IOException, NiFiRegistryException {
            final ExtensionMetadataContainer allExtensions = extensionClient.findExtensions((ExtensionFilterParams) null);
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
        }

        @Test
        public void testFindProcessorExtensions() throws IOException, NiFiRegistryException {
            final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                    .extensionType(ExtensionType.PROCESSOR)
                    .build();
            final ExtensionMetadataContainer processorExtensions = extensionClient.findExtensions(filterParams);
            assertNotNull(processorExtensions);
            assertNotNull(processorExtensions.getExtensions());
            assertEquals(3, processorExtensions.getNumResults());
            assertEquals(3, processorExtensions.getExtensions().size());
        }

        @Test
        public void testFindControllerServiceExtensions() throws IOException, NiFiRegistryException {
            final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                    .extensionType(ExtensionType.CONTROLLER_SERVICE)
                    .build();
            final ExtensionMetadataContainer serviceExtensions = extensionClient.findExtensions(filterParams);
            assertNotNull(serviceExtensions);
            assertNotNull(serviceExtensions.getExtensions());
            assertEquals(1, serviceExtensions.getNumResults());
            assertEquals(1, serviceExtensions.getExtensions().size());
        }

        @Test
        public void testFindNifiNarBundleExtensions() throws IOException, NiFiRegistryException {
            final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                    .bundleType(BundleType.NIFI_NAR)
                    .build();
            final ExtensionMetadataContainer narExtensions = extensionClient.findExtensions(filterParams);
            assertNotNull(narExtensions);
            assertNotNull(narExtensions.getExtensions());
            assertEquals(4, narExtensions.getNumResults());
            assertEquals(4, narExtensions.getExtensions().size());
        }

        @Test
        public void testFindExtensionsByTag() throws IOException, NiFiRegistryException {
            final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                    .tag("test")
                    .build();
            final ExtensionMetadataContainer extensions = extensionClient.findExtensions(filterParams);
            assertNotNull(extensions);
            assertNotNull(extensions.getExtensions());
            assertEquals(3, extensions.getNumResults());
            assertEquals(3, extensions.getExtensions().size());
        }

        @Test
        public void testFindExtensionsByProvidedServiceApi() throws IOException, NiFiRegistryException {
            final ProvidedServiceAPI serviceAPI = new ProvidedServiceAPI();
            serviceAPI.setClassName("org.apache.nifi.service.TestService");
            serviceAPI.setGroupId(REPO_GROUP_ID);
            serviceAPI.setArtifactId("nifi-test-service-api-nar");
            serviceAPI.setVersion("1.0.0");

            final ExtensionMetadataContainer providedTestServiceApi = extensionClient.findExtensions(serviceAPI);
            assertNotNull(providedTestServiceApi);
            assertNotNull(providedTestServiceApi.getExtensions());
            assertEquals(1, providedTestServiceApi.getNumResults());
            assertEquals(1, providedTestServiceApi.getExtensions().size());
            assertEquals("org.apache.nifi.service.TestServiceImpl", providedTestServiceApi.getExtensions().first().getName());
        }
    }

    @Nested
    public class ItemsClientTests {

        ItemsClient itemsClient;
        Bucket flowBucket;
        VersionedFlow flow1;
        VersionedFlow flow2;

        Bucket bundleBucket;
        Bundle testNarBundle;
        Bundle fooNarBundle;

        @BeforeEach
        public void setup() throws IOException, NiFiRegistryException {
            itemsClient = client.getItemsClient();
            flowBucket = createBucket(0);
            flow1 = createFlow(flowBucket, 1);
            flow2 = createFlow(flowBucket, 2);

            bundleBucket = createBucket(1);
            testNarBundle = createNifiTestNarV1ExtensionBundle(bundleBucket).getBundle();
            createNifiTestNarV2ExtensionBundle(bundleBucket);

            fooNarBundle = createNifiFooNarV1ExtensionBundle(bundleBucket).getBundle();
            createNifiFooNarV2Build1ExtensionBundle(bundleBucket);
            createNifiFooNarV2Build2ExtensionBundle(bundleBucket);
            createNifiFooNarV2Build3ExtensionBundle(bundleBucket);
        }

        @Test
        public void testGetFields() throws IOException, NiFiRegistryException {
            final Fields itemFields = itemsClient.getFields();
            assertNotNull(itemFields.getFields());
            assertTrue(itemFields.getFields().size() > 0);
        }

        @Test
        public void testGetAllItems() throws IOException, NiFiRegistryException {
            final List<BucketItem> allItems = itemsClient.getAll();
            assertEquals(4, allItems.size());
            allItems.forEach(i -> {
                assertNotNull(i.getBucketName());
                assertNotNull(i.getLink());
            });
            allItems.forEach(i -> LOGGER.info("All items, item " + i.getIdentifier()));
        }

        @Test
        public void testGetFlowItems() throws IOException, NiFiRegistryException {
            final List<BucketItem> allItems = itemsClient.getAll();
            final List<BucketItem> flowItems = allItems.stream()
                    .filter(i -> i.getType() == BucketItemType.Flow)
                    .collect(Collectors.toList());
            assertEquals(2, flowItems.size());
        }

        @Test
        public void testGetBundleItems() throws IOException, NiFiRegistryException {
            final List<BucketItem> allItems = itemsClient.getAll();
            final List<BucketItem> extensionBundleItems = allItems.stream()
                    .filter(i -> i.getType() == BucketItemType.Bundle)
                    .collect(Collectors.toList());
            assertEquals(2, extensionBundleItems.size());
        }

        @Test
        public void testGetItemsForFlowBucket() throws IOException, NiFiRegistryException {
            final List<BucketItem> bucketItems = itemsClient.getByBucket(flowBucket.getIdentifier());
            assertEquals(2, bucketItems.size());
            final List<BucketItem> allItems = itemsClient.getAll();
            allItems.forEach(i -> assertNotNull(i.getBucketName()));
            bucketItems.forEach(i -> LOGGER.info("Items in bucket, item " + i.getIdentifier()));
        }
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

        final Map<String, VersionedParameterContext> contexts = new HashMap<>();
        contexts.put(context1.getName(), context1);

        // Create an external controller service reference
        final ExternalControllerServiceReference serviceReference = new ExternalControllerServiceReference();
        serviceReference.setName("External Service 1");
        serviceReference.setIdentifier(UUID.randomUUID().toString());

        final ParameterProviderReference parameterProviderReference = new ParameterProviderReference();
        parameterProviderReference.setIdentifier("parameter-provider");
        parameterProviderReference.setType("com.test.TestParameterProvider");
        parameterProviderReference.setName("provider");
        parameterProviderReference.setBundle(new org.apache.nifi.flow.Bundle("group", "artifact", "version"));

        final Map<String, ParameterProviderReference> parameterProviderReferences = new HashMap<>();
        parameterProviderReferences.put(parameterProviderReference.getIdentifier(), parameterProviderReference);

        final Map<String, ExternalControllerServiceReference> serviceReferences = new HashMap<>();
        serviceReferences.put(serviceReference.getIdentifier(), serviceReference);

        // Create the snapshot
        final VersionedFlowSnapshot snapshot = buildSnapshot(createdFlow, 1);
        snapshot.setFlowEncodingVersion("2.0.0");
        snapshot.setParameterContexts(contexts);
        snapshot.setExternalControllerServices(serviceReferences);
        snapshot.setParameterProviders(parameterProviderReferences);

        final VersionedFlowSnapshot createdSnapshot = client.getFlowSnapshotClient().create(snapshot);
        assertNotNull(createdSnapshot);
        assertNotNull(createdSnapshot.getFlowEncodingVersion());
        assertNotNull(createdSnapshot.getParameterContexts());
        assertNotNull(createdSnapshot.getExternalControllerServices());
        assertEquals(snapshot.getFlowEncodingVersion(), createdSnapshot.getFlowEncodingVersion());
        assertEquals(1, createdSnapshot.getParameterContexts().size());
        assertEquals(1, createdSnapshot.getExternalControllerServices().size());
        assertEquals(1, createdSnapshot.getParameterProviders().size());

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
        assertEquals(1, retrievedSnapshot.getParameterProviders().size());
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
            final String narFile,
            final String sha256)
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
            final String narFile,
            final String sha256)
            throws IOException, NiFiRegistryException {

        final BundleVersion createdBundleVersion;
        if (StringUtils.isBlank(sha256)) {
            createdBundleVersion = bundleVersionClient.create(bundlesBucket.getIdentifier(),
                    BundleType.NIFI_NAR,
                    new File(narFile));
        } else {
            createdBundleVersion = bundleVersionClient.create(
                    bundlesBucket.getIdentifier(),
                    BundleType.NIFI_NAR,
                    new File(narFile),
                    sha256);
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

    private Bucket createBucket(int index) throws IOException, NiFiRegistryException {
        final Bucket bucket = new Bucket();
        bucket.setName("Bucket #" + index);
        bucket.setDescription("This is bucket #" + index);
        bucket.setRevision(new RevisionInfo(CLIENT_ID, 0L));
        return client.getBucketClient().create(bucket);
    }

    private VersionedFlow createFlow(Bucket bucket, int index) throws IOException, NiFiRegistryException {
        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setName(bucket.getName() + " Flow #" + index);
        versionedFlow.setDescription("This is " + bucket.getName() + " flow #" + index);
        versionedFlow.setBucketIdentifier(bucket.getIdentifier());
        versionedFlow.setRevision(new RevisionInfo(CLIENT_ID, 0L));
        return client.getFlowClient().create(versionedFlow);
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

        final Map<String, String> processorProperties = new HashMap<>();
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

    private VersionedFlowSnapshot createSnapshot(VersionedFlow flow, int num) throws IOException, NiFiRegistryException {
        return client.getFlowSnapshotClient().create(buildSnapshot(flow, num));
    }

    private List<Bucket> createBuckets() throws IOException, NiFiRegistryException {
        final int numBuckets = 10;
        final List<Bucket> createdBuckets = new ArrayList<>();

        for (int i = 0; i < numBuckets; i++) {
            final Bucket createdBucket = createBucket(i);
            LOGGER.info("Created bucket # " + i + " with id " + createdBucket.getIdentifier());
            createdBuckets.add(createdBucket);
        }
        return createdBuckets;
    }

    private BundleVersion createNifiTestNarV1ExtensionBundle(final Bucket bucket) throws IOException, NiFiRegistryException {
        return createExtensionBundleVersionWithStream(bucket, client.getBundleVersionClient(), NIFI_TEST_NAR_1_0_0_PATH, null);
    }

    private BundleVersion createNifiTestNarV2ExtensionBundle(final Bucket bucket) throws IOException, NiFiRegistryException {
        final String testNar2Sha256 = calculateSha256Hex(NIFI_TEST_NAR_2_0_0_PATH);
        return createExtensionBundleVersionWithStream(bucket,
                client.getBundleVersionClient(),
                NIFI_TEST_NAR_2_0_0_PATH,
                testNar2Sha256);
    }

    private BundleVersion createNifiTestNarV2ExtensionBundle(final Bucket bucket, String customSha) throws IOException, NiFiRegistryException {
        return createExtensionBundleVersionWithStream(bucket,
                client.getBundleVersionClient(),
                NIFI_TEST_NAR_2_0_0_PATH,
                customSha);
    }

    private BundleVersion createNifiFooNarV1ExtensionBundle(final Bucket bucket) throws IOException, NiFiRegistryException {
        return createExtensionBundleVersionWithFile(bucket,
                client.getBundleVersionClient(),
                NIFI_FOO_NAR_1_0_0_PATH,
                null);
    }

    private BundleVersion createNifiFooNarV2Build1ExtensionBundle(final Bucket bucket) throws IOException, NiFiRegistryException {
        return createExtensionBundleVersionWithFile(bucket,
                client.getBundleVersionClient(),
                NIFI_FOO_NAR_2_0_0_BUILD1_PATH,
                null);
    }

    private BundleVersion createNifiFooNarV2Build2ExtensionBundle(final Bucket bucket) throws IOException, NiFiRegistryException {
        return createExtensionBundleVersionWithFile(bucket,
                client.getBundleVersionClient(),
                NIFI_FOO_NAR_2_0_0_BUILD2_PATH,
                null);
    }

    private BundleVersion createNifiFooNarV2Build3ExtensionBundle(final Bucket bucket) throws IOException, NiFiRegistryException {
        return createExtensionBundleVersionWithFile(bucket,
                client.getBundleVersionClient(),
                NIFI_FOO_NAR_2_0_0_BUILD3_PATH,
                null);
    }
}
