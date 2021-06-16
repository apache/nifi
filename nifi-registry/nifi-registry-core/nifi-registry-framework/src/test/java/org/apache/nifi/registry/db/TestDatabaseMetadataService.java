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
package org.apache.nifi.registry.db;

import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntity;
import org.apache.nifi.registry.db.entity.BucketItemEntityType;
import org.apache.nifi.registry.db.entity.BundleEntity;
import org.apache.nifi.registry.db.entity.BundleVersionDependencyEntity;
import org.apache.nifi.registry.db.entity.BundleVersionEntity;
import org.apache.nifi.registry.db.entity.ExtensionAdditionalDetailsEntity;
import org.apache.nifi.registry.db.entity.ExtensionEntity;
import org.apache.nifi.registry.db.entity.ExtensionProvidedServiceApiEntity;
import org.apache.nifi.registry.db.entity.ExtensionRestrictionEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.db.entity.TagCountEntity;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.component.ExtensionFilterParams;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;
import org.apache.nifi.registry.extension.component.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.service.MetadataService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestDatabaseMetadataService extends DatabaseBaseTest {

    @Autowired
    private MetadataService metadataService;

    //----------------- Buckets ---------------------------------

    @Test
    public void testCreateAndGetBucket() {
        final BucketEntity b = new BucketEntity();
        b.setId("testBucketId");
        b.setName("testBucketName");
        b.setDescription("testBucketDesc");
        b.setCreated(new Date());

        metadataService.createBucket(b);

        final BucketEntity createdBucket = metadataService.getBucketById(b.getId());
        assertNotNull(createdBucket);
        assertEquals(b.getId(), createdBucket.getId());
        assertEquals(b.getName(), createdBucket.getName());
        assertEquals(b.getDescription(), createdBucket.getDescription());
        assertEquals(b.getCreated().getTime(), createdBucket.getCreated().getTime());
        assertFalse(b.isAllowExtensionBundleRedeploy());
    }

    @Test
    public void testGetBucketDoesNotExist() {
        final BucketEntity bucket = metadataService.getBucketById("does-not-exist");
        assertNull(bucket);
    }

    @Test
    public void testGetBucketsByName() {
        final List<BucketEntity> buckets = metadataService.getBucketsByName("Bucket 1");
        assertNotNull(buckets);
        assertEquals(1, buckets.size());
        assertEquals("Bucket 1", buckets.get(0).getName());
    }

    @Test
    public void testGetBucketsByNameNoneFound() {
        final List<BucketEntity> buckets = metadataService.getBucketsByName("Bucket XYZ");
        assertNotNull(buckets);
        assertEquals(0, buckets.size());
    }

    @Test
    public void testUpdateBucket() {
        final BucketEntity bucket = metadataService.getBucketById("1");
        assertNotNull(bucket);
        assertFalse(bucket.isAllowExtensionBundleRedeploy());
        assertFalse(bucket.isAllowPublicRead());

        final String updatedName = bucket.getName() + " UPDATED";
        final String updatedDesc = bucket.getDescription() + "DESC";

        bucket.setName(updatedName);
        bucket.setDescription(updatedDesc);
        bucket.setAllowExtensionBundleRedeploy(true);
        bucket.setAllowPublicRead(true);

        metadataService.updateBucket(bucket);

        final BucketEntity updatedBucket = metadataService.getBucketById(bucket.getId());
        assertNotNull(updatedName);
        assertEquals(updatedName, updatedBucket.getName());
        assertEquals(updatedDesc, updatedBucket.getDescription());
        assertTrue(updatedBucket.isAllowExtensionBundleRedeploy());
        assertTrue(updatedBucket.isAllowPublicRead());
    }

    @Test
    public void testDeleteBucketNoChildren() {
        final BucketEntity bucket = metadataService.getBucketById("6");
        assertNotNull(bucket);

        metadataService.deleteBucket(bucket);

        final BucketEntity deletedBucket = metadataService.getBucketById("6");
        assertNull(deletedBucket);
    }

    @Test
    public void testDeleteBucketWithChildren() {
        final BucketEntity bucket = metadataService.getBucketById("1");
        assertNotNull(bucket);

        metadataService.deleteBucket(bucket);

        final BucketEntity deletedBucket = metadataService.getBucketById("1");
        assertNull(deletedBucket);
    }

    @Test
    public void testGetBucketsForIds() {
        final List<BucketEntity> buckets = metadataService.getBuckets(new HashSet<>(Arrays.asList("1", "2")));
        assertNotNull(buckets);
        assertEquals(2, buckets.size());
        assertEquals("1", buckets.get(0).getId());
        assertEquals("2", buckets.get(1).getId());
    }

    @Test
    public void testGetAllBuckets() {
        final List<BucketEntity> buckets = metadataService.getAllBuckets();
        assertNotNull(buckets);
        assertEquals(6, buckets.size());
    }

    //----------------- BucketItems ---------------------------------

    @Test
    public void testGetBucketItemsForBucket() {
        final BucketEntity bucket = metadataService.getBucketById("1");
        assertNotNull(bucket);

        final List<BucketItemEntity> items = metadataService.getBucketItems(bucket.getId());
        assertNotNull(items);
        assertEquals(2, items.size());

        items.stream().forEach(i -> assertNotNull(i.getBucketName()));
    }

    @Test
    public void testGetBucketItemsForBuckets() {
        final List<BucketItemEntity> items = metadataService.getBucketItems(new HashSet<>(Arrays.asList("1", "2")));
        assertNotNull(items);
        assertEquals(3, items.size());

        items.stream().forEach(i -> assertNotNull(i.getBucketName()));
    }

    @Test
    public void testGetItemsWithCounts() {
        final List<BucketItemEntity> items = metadataService.getBucketItems(new HashSet<>(Arrays.asList("1", "2")));
        assertNotNull(items);

        // 3 items across all buckets
        assertEquals(3, items.size());

        final BucketItemEntity item1 = items.stream().filter(i -> i.getId().equals("1")).findFirst().orElse(null);
        assertNotNull(item1);
        assertEquals(BucketItemEntityType.FLOW, item1.getType());

        final FlowEntity flowEntity = (FlowEntity) item1;
        assertEquals(3, flowEntity.getSnapshotCount());

        items.stream().forEach(i -> assertNotNull(i.getBucketName()));
    }

    @Test
    public void testGetItemsWithCountsFilteredByBuckets() {
        final List<BucketItemEntity> items = metadataService.getBucketItems(Collections.singleton("1"));
        assertNotNull(items);

        // only 2 items in bucket 1
        assertEquals(2, items.size());

        final BucketItemEntity item1 = items.stream().filter(i -> i.getId().equals("1")).findFirst().orElse(null);
        assertNotNull(item1);
        assertEquals(BucketItemEntityType.FLOW, item1.getType());

        final FlowEntity flowEntity = (FlowEntity) item1;
        assertEquals(3, flowEntity.getSnapshotCount());

        items.stream().forEach(i -> assertNotNull(i.getBucketName()));
    }

    //----------------- Flows ---------------------------------

    @Test
    public void testGetFlowByIdWhenExists() {
        final FlowEntity flow = metadataService.getFlowById("1");
        assertNotNull(flow);
        assertEquals("1", flow.getId());
        assertEquals("1", flow.getBucketId());
    }

    @Test
    public void testGetFlowByIdWhenDoesNotExist() {
        final FlowEntity flow = metadataService.getFlowById("does-not-exist");
        assertNull(flow);
    }

    @Test
    public void testCreateFlow() {
        final String bucketId = "1";

        final FlowEntity flow = new FlowEntity();
        flow.setId(UUID.randomUUID().toString());
        flow.setBucketId(bucketId);
        flow.setName("Test Flow 1");
        flow.setDescription("Description for Test Flow 1");
        flow.setCreated(new Date());
        flow.setModified(new Date());
        flow.setType(BucketItemEntityType.FLOW);

        metadataService.createFlow(flow);

        final FlowEntity createdFlow = metadataService.getFlowById(flow.getId());
        assertNotNull(flow);
        assertEquals(flow.getId(), createdFlow.getId());
        assertEquals(flow.getBucketId(), createdFlow.getBucketId());
        assertEquals(flow.getName(), createdFlow.getName());
        assertEquals(flow.getDescription(), createdFlow.getDescription());
        assertEquals(flow.getCreated().getTime(), createdFlow.getCreated().getTime());
        assertEquals(flow.getModified(), createdFlow.getModified());
        assertEquals(flow.getType(), createdFlow.getType());
    }

    @Test
    public void testGetFlowByIdWithSnapshotCount() {
       final FlowEntity flowEntity = metadataService.getFlowByIdWithSnapshotCounts("1");
        assertNotNull(flowEntity);
        assertEquals(3, flowEntity.getSnapshotCount());
    }

    @Test
    public void testGetFlowsByBucket() {
        final BucketEntity bucketEntity = metadataService.getBucketById("1");
        final List<FlowEntity> flows = metadataService.getFlowsByBucket(bucketEntity.getId());
        assertEquals(2, flows.size());

        final FlowEntity flowEntity = flows.stream().filter(f -> f.getId().equals("1")).findFirst().orElse(null);
        assertNotNull(flowEntity);
        assertEquals(3, flowEntity.getSnapshotCount());
    }

    @Test
    public void testGetFlowsByName() {
        final List<FlowEntity> flows = metadataService.getFlowsByName("Flow 1");
        assertNotNull(flows);
        assertEquals(2, flows.size());
        assertEquals("Flow 1", flows.get(0).getName());
        assertEquals("Flow 1", flows.get(1).getName());
    }

    @Test
    public void testGetFlowsByNameByBucket() {
        final List<FlowEntity> flows = metadataService.getFlowsByName("2","Flow 1");
        assertNotNull(flows);
        assertEquals(1, flows.size());
        assertEquals("Flow 1", flows.get(0).getName());
        assertEquals("2", flows.get(0).getBucketId());
    }

    @Test
    public void testUpdateFlow() {
        final FlowEntity flow = metadataService.getFlowById("1");
        assertNotNull(flow);

        final Date originalModified = flow.getModified();

        flow.setName(flow.getName() + " UPDATED");
        flow.setDescription(flow.getDescription() + " UPDATED");

        metadataService.updateFlow(flow);

        final FlowEntity updatedFlow = metadataService.getFlowById( "1");
        assertNotNull(flow);
        assertEquals(flow.getName(), updatedFlow.getName());
        assertEquals(flow.getDescription(), updatedFlow.getDescription());
        assertEquals(flow.getModified().getTime(), updatedFlow.getModified().getTime());
        assertTrue(updatedFlow.getModified().getTime() > originalModified.getTime());
    }

    @Test
    public void testDeleteFlowWithSnapshots() {
        final FlowEntity flow = metadataService.getFlowById( "1");
        assertNotNull(flow);

        metadataService.deleteFlow(flow);

        final FlowEntity deletedFlow = metadataService.getFlowById("1");
        assertNull(deletedFlow);
    }

    //----------------- FlowSnapshots ---------------------------------

    @Test
    public void testGetFlowSnapshot() {
        final FlowSnapshotEntity entity = metadataService.getFlowSnapshot( "1", 1);
        assertNotNull(entity);
        assertEquals("1", entity.getFlowId());
        assertEquals(1, entity.getVersion().intValue());
    }

    @Test
    public void testGetFlowSnapshotDoesNotExist() {
        final FlowSnapshotEntity entity = metadataService.getFlowSnapshot( "DOES-NOT-EXIST", 1);
        assertNull(entity);
    }

    @Test
    public void testCreateFlowSnapshot() {
        final FlowSnapshotEntity flowSnapshot = new FlowSnapshotEntity();
        flowSnapshot.setFlowId("1");
        flowSnapshot.setVersion(4);
        flowSnapshot.setCreated(new Date());
        flowSnapshot.setCreatedBy("test-user");
        flowSnapshot.setComments("Comments");

        metadataService.createFlowSnapshot(flowSnapshot);

        final FlowSnapshotEntity createdFlowSnapshot = metadataService.getFlowSnapshot(flowSnapshot.getFlowId(), flowSnapshot.getVersion());
        assertNotNull(createdFlowSnapshot);
        assertEquals(flowSnapshot.getFlowId(), createdFlowSnapshot.getFlowId());
        assertEquals(flowSnapshot.getVersion(), createdFlowSnapshot.getVersion());
        assertEquals(flowSnapshot.getComments(), createdFlowSnapshot.getComments());
        assertEquals(flowSnapshot.getCreated().getTime(), createdFlowSnapshot.getCreated().getTime());
        assertEquals(flowSnapshot.getCreatedBy(), createdFlowSnapshot.getCreatedBy());
    }

    @Test
    public void testGetLatestSnapshot() {
        final FlowSnapshotEntity latest = metadataService.getLatestSnapshot("1");
        assertNotNull(latest);
        assertEquals("1", latest.getFlowId());
        assertEquals(3, latest.getVersion().intValue());
    }

    @Test
    public void testGetLatestSnapshotDoesNotExist() {
        final FlowSnapshotEntity latest = metadataService.getLatestSnapshot("DOES-NOT-EXIST");
        assertNull(latest);
    }

    @Test
    public void testGetFlowSnapshots() {
        final List<FlowSnapshotEntity> flowSnapshots = metadataService.getSnapshots( "1");
        assertNotNull(flowSnapshots);
        assertEquals(3, flowSnapshots.size());
    }

    @Test
    public void testGetFlowSnapshotsNoneFound() {
        final List<FlowSnapshotEntity> flowSnapshots = metadataService.getSnapshots( "2");
        assertNotNull(flowSnapshots);
        assertEquals(0, flowSnapshots.size());
    }

    @Test
    public void testDeleteFlowSnapshot() {
        final FlowSnapshotEntity entity = metadataService.getFlowSnapshot( "1", 1);
        assertNotNull(entity);

        metadataService.deleteFlowSnapshot(entity);

        final FlowSnapshotEntity deletedEntity = metadataService.getFlowSnapshot( "1", 1);
        assertNull(deletedEntity);
    }

    //----------------- Extension Bundles ---------------------------------

    @Test
    public void testGetExtensionBundleById() {
        final BundleEntity entity = metadataService.getBundle("eb1");
        assertNotNull(entity);

        assertEquals("eb1", entity.getId());
        assertEquals("nifi-example-processors-nar", entity.getName());
        assertEquals("Example processors bundle", entity.getDescription());
        assertNotNull(entity.getCreated());
        assertNotNull(entity.getModified());
        assertEquals(BucketItemEntityType.BUNDLE, entity.getType());
        assertEquals("3", entity.getBucketId());

        assertEquals(BundleType.NIFI_NAR, entity.getBundleType());

        assertEquals("org.apache.nifi", entity.getGroupId());
        assertEquals("nifi-example-processors-nar", entity.getArtifactId());
    }

    @Test
    public void testGetExtensionBundleDoesNotExist() {
        final BundleEntity entity = metadataService.getBundle("does-not-exist");
        assertNull(entity);
    }

    @Test
    public void testGetExtensionBundleByGroupArtifact() {
        final String bucketId = "3";
        final String group = "org.apache.nifi";
        final String artifact = "nifi-example-service-api-nar";

        final BundleEntity entity = metadataService.getBundle(bucketId, group, artifact);
        assertNotNull(entity);
        assertEquals(bucketId, entity.getBucketId());

        assertEquals(group, entity.getGroupId());
        assertEquals(artifact, entity.getArtifactId());
    }

    @Test
    public void testGetExtensionBundleByGroupArtifactDoesNotExist() {
        final String bucketId = "3";
        final String group = "org.apache.nifi";
        final String artifact = "does-not-exist";

        final BundleEntity entity = metadataService.getBundle(bucketId, group, artifact);
        assertNull(entity);
    }

    @Test
    public void testGetExtensionBundlesWithEmptyFilterParams() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final List<BundleEntity> bundles = metadataService.getBundles(bucketIds, BundleFilterParams.empty());
        assertNotNull(bundles);
        assertEquals(3, bundles.size());

        bundles.forEach(b -> {
            assertTrue(b.getVersionCount() > 0);
            assertNotNull(b.getBucketName());
        });
    }

    @Test
    public void testGetExtensionBundlesWithFilterParams() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final List<BundleEntity> bundles = metadataService.getBundles(bucketIds,
                BundleFilterParams.empty());
        assertNotNull(bundles);
        assertEquals(3, bundles.size());

        final List<BundleEntity> bundles2 = metadataService.getBundles(bucketIds,
                BundleFilterParams.of("org.apache.nifi", null));
        assertNotNull(bundles2);
        assertEquals(2, bundles2.size());

        final List<BundleEntity> bundles3 = metadataService.getBundles(bucketIds,
                BundleFilterParams.of("org.apache.%", null));
        assertNotNull(bundles3);
        assertEquals(2, bundles3.size());

        final List<BundleEntity> bundles4 = metadataService.getBundles(bucketIds,
                BundleFilterParams.of("org.apache.nifi", "nifi-example-processors-nar"));
        assertNotNull(bundles4);
        assertEquals(1, bundles4.size());

        final List<BundleEntity> bundles5 = metadataService.getBundles(bucketIds,
                BundleFilterParams.of("org.apache.nifi", "nifi-example-processors-%"));
        assertNotNull(bundles5);
        assertEquals(1, bundles5.size());

        final List<BundleEntity> bundles6 = metadataService.getBundles(bucketIds,
                BundleFilterParams.of(null, "nifi-example-processors-%"));
        assertNotNull(bundles6);
        assertEquals(1, bundles6.size());

        final List<BundleEntity> bundles7 = metadataService.getBundles(bucketIds,
                BundleFilterParams.of("Bucket %", null, "nifi-example-processors-%"));
        assertNotNull(bundles7);
        assertEquals(1, bundles7.size());
    }

    @Test
    public void testGetExtensionBundlesByBucket() {
        final List<BundleEntity> bundles = metadataService.getBundlesByBucket("3");
        assertNotNull(bundles);
        assertEquals(3, bundles.size());

        final List<BundleEntity> bundles2 = metadataService.getBundlesByBucket("6");
        assertNotNull(bundles2);
        assertEquals(0, bundles2.size());
    }

    @Test
    public void testGetExtensionBundlesByBucketAndGroup() {
        final List<BundleEntity> bundles = metadataService.getBundlesByBucketAndGroup("3", "org.apache.nifi");
        assertNotNull(bundles);
        assertEquals(2, bundles.size());

        final List<BundleEntity> bundles2 = metadataService.getBundlesByBucketAndGroup("3", "does-not-exist");
        assertNotNull(bundles2);
        assertEquals(0, bundles2.size());
    }

    @Test
    public void testCreateExtensionBundle() {
        final BundleEntity entity = new BundleEntity();
        entity.setId(UUID.randomUUID().toString());
        entity.setBucketId("3");
        entity.setName("nifi-foo-nar");
        entity.setDescription("This is foo nar");
        entity.setCreated(new Date());
        entity.setModified(new Date());
        entity.setGroupId("org.apache.nifi");
        entity.setArtifactId("nifi-foo-nar");
        entity.setBundleType(BundleType.NIFI_NAR);

        final BundleEntity createdEntity = metadataService.createBundle(entity);
        assertNotNull(createdEntity);

        final List<BundleEntity> bundles = metadataService.getBundlesByBucket("3");
        assertNotNull(bundles);
        assertEquals(4, bundles.size());
    }

    @Test
    public void testDeleteExtensionBundle() {
        final List<BundleEntity> bundles = metadataService.getBundlesByBucket("3");
        assertNotNull(bundles);
        assertEquals(3, bundles.size());

        final BundleEntity existingBundle = bundles.get(0);
        metadataService.deleteBundle(existingBundle);

        final BundleEntity deletedBundle = metadataService.getBundle(existingBundle.getId());
        assertNull(deletedBundle);

        final List<BundleEntity> bundlesAfterDelete = metadataService.getBundlesByBucket("3");
        assertNotNull(bundlesAfterDelete);
        assertEquals(2, bundlesAfterDelete.size());
    }

    @Test
    public void testDeleteBucketWithExtensionBundles() {
        final List<BundleEntity> bundles = metadataService.getBundlesByBucket("3");
        assertNotNull(bundles);
        assertEquals(3, bundles.size());

        final BucketEntity bucket = metadataService.getBucketById("3");
        assertNotNull(bucket);
        metadataService.deleteBucket(bucket);

        final List<BundleEntity> bundlesAfterDelete = metadataService.getBundlesByBucket("3");
        assertNotNull(bundlesAfterDelete);
        assertEquals(0, bundlesAfterDelete.size());
    }

    //----------------- Extension Bundle Versions ---------------------------------

    @Test
    public void testCreateExtensionBundleVersion() {
        final BundleVersionEntity bundleVersion = new BundleVersionEntity();
        bundleVersion.setId(UUID.randomUUID().toString());
        bundleVersion.setBundleId("eb1");
        bundleVersion.setVersion("1.1.0");
        bundleVersion.setCreated(new Date());
        bundleVersion.setCreatedBy("user2");
        bundleVersion.setDescription("This is v1.1.0");
        bundleVersion.setSha256Hex("123456789");
        bundleVersion.setSha256Supplied(false);
        bundleVersion.setContentSize(2048);
        bundleVersion.setSystemApiVersion("2.0.0");

        bundleVersion.setBuildTool("JDK");
        bundleVersion.setBuildFlags("N/A");
        bundleVersion.setBuildBranch("master");
        bundleVersion.setBuildTag("HEAD");
        bundleVersion.setBuildRevision("123456");
        bundleVersion.setBuiltBy("jsmith");
        bundleVersion.setBuilt(new Date());

        metadataService.createBundleVersion(bundleVersion);

        final BundleVersionEntity createdBundleVersion = metadataService.getBundleVersion("eb1", "1.1.0");
        assertNotNull(createdBundleVersion);
        assertEquals(bundleVersion.getId(), createdBundleVersion.getId());
        assertFalse(bundleVersion.getSha256Supplied());

        assertEquals(bundleVersion.getSystemApiVersion(), createdBundleVersion.getSystemApiVersion());
        assertEquals(bundleVersion.getBuildTool(), createdBundleVersion.getBuildTool());
        assertEquals(bundleVersion.getBuildFlags(), createdBundleVersion.getBuildFlags());
        assertEquals(bundleVersion.getBuildBranch(), createdBundleVersion.getBuildBranch());
        assertEquals(bundleVersion.getBuildTag(), createdBundleVersion.getBuildTag());
        assertEquals(bundleVersion.getBuildRevision(), createdBundleVersion.getBuildRevision());
        assertEquals(bundleVersion.getBuiltBy(), createdBundleVersion.getBuiltBy());
        assertEquals(bundleVersion.getBuilt().getTime(), createdBundleVersion.getBuilt().getTime());
    }

    @Test
    public void testGetExtensionBundleVersionsWithEmptyBucketIdsAndEmptyFilterParams() {
        final List<BundleVersionEntity> versionEntities = metadataService.getBundleVersions(
                Collections.emptySet(), BundleVersionFilterParams.empty());
        assertEquals(0, versionEntities.size());
    }

    @Test
    public void testGetExtensionBundleVersionsWithEmptyFilterParams() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final List<BundleVersionEntity> versionEntities = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.empty());
        assertEquals(3, versionEntities.size());
    }

    @Test
    public void testGetExtensionBundleVersionsWithFilterParams() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final List<BundleVersionEntity> versionEntities = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.nifi", null, null));
        assertEquals(2, versionEntities.size());

        versionEntities.forEach(bve -> {
            assertNotNull(bve.getGroupId());
            assertNotNull(bve.getArtifactId());
        });

        final List<BundleVersionEntity> versionEntities2 = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.%", null, null));
        assertEquals(2, versionEntities2.size());

        final List<BundleVersionEntity> versionEntities3 = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.nifi", "nifi-example-processors-nar", null));
        assertEquals(1, versionEntities3.size());

        final List<BundleVersionEntity> versionEntities4 = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.nifi", "nifi-example-processors-%", null));
        assertEquals(1, versionEntities4.size());

        final List<BundleVersionEntity> versionEntities5 = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.nifi", "nifi-example-processors-nar", "1.0.0"));
        assertEquals(1, versionEntities5.size());

        final List<BundleVersionEntity> versionEntities6 = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.nifi", "nifi-example-processors-nar", "1.0.%"));
        assertEquals(1, versionEntities6.size());

        final List<BundleVersionEntity> versionEntities7 = metadataService.getBundleVersions(
                bucketIds, BundleVersionFilterParams.of("org.apache.nifi", "nifi-example-processors-nar", "NOT-FOUND"));
        assertEquals(0, versionEntities7.size());
    }

    @Test
    public void testGetExtensionBundleVersionByBundleIdAndVersion() {
        final BundleVersionEntity bundleVersion = metadataService.getBundleVersion("eb1", "1.0.0");
        assertNotNull(bundleVersion);
        assertEquals("eb1-v1", bundleVersion.getId());
        assertEquals("eb1", bundleVersion.getBundleId());
        assertEquals("1.0.0", bundleVersion.getVersion());
        assertNotNull(bundleVersion.getCreated());
        assertEquals("user1", bundleVersion.getCreatedBy());
        assertEquals("First version of eb1", bundleVersion.getDescription());
        assertTrue(bundleVersion.getSha256Supplied());
        assertEquals(1024, bundleVersion.getContentSize());
    }

    @Test
    public void testGetExtensionBundleVersionByBundleIdAndVersionDoesNotExist() {
        final BundleVersionEntity bundleVersion = metadataService.getBundleVersion("does-not-exist", "1.0.0");
        assertNull(bundleVersion);
    }

    @Test
    public void testGetExtensionBundleVersionByBucketGroupArtifactVersion() {
        final String bucketId = "3";
        final String groupId = "org.apache.nifi";
        final String artifactId = "nifi-example-processors-nar";
        final String version = "1.0.0";

        final BundleVersionEntity bundleVersion = metadataService.getBundleVersion(bucketId, groupId, artifactId, version);
        assertNotNull(bundleVersion);
        assertEquals("eb1-v1", bundleVersion.getId());
        assertTrue(bundleVersion.getSha256Supplied());
    }

    @Test
    public void testGetExtensionBundleVersionByBucketGroupArtifactVersionWhenDoesNotExist() {
        final String bucketId = "3";
        final String groupId = "org.apache.nifi";
        final String artifactId = "nifi-example-processors-nar";
        final String version = "FOO";

        final BundleVersionEntity bundleVersion = metadataService.getBundleVersion(bucketId, groupId, artifactId, version);
        assertNull(bundleVersion);
    }

    @Test
    public void testGetExtensionBundleVersionsByBundleId() {
        final List<BundleVersionEntity> bundleVersions = metadataService.getBundleVersions("eb1");
        assertNotNull(bundleVersions);
        assertEquals(1, bundleVersions.size());

        final BundleVersionEntity bundleVersion = bundleVersions.get(0);
        assertEquals("eb1", bundleVersion.getBundleId());
    }

    @Test
    public void testGetExtensionBundleVersionsByBundleIdWhenDoesNotExist() {
        final List<BundleVersionEntity> bundleVersions = metadataService.getBundleVersions("does-not-exist");
        assertNotNull(bundleVersions);
        assertEquals(0, bundleVersions.size());
    }

    @Test
    public void testGetExtensionBundleVersionsByBucketGroupArtifact() {
        final String bucketId = "3";
        final String groupId = "org.apache.nifi";
        final String artifactId = "nifi-example-processors-nar";

        final List<BundleVersionEntity> bundleVersions = metadataService.getBundleVersions(bucketId, groupId, artifactId);
        assertNotNull(bundleVersions);
        assertEquals(1, bundleVersions.size());

        final BundleVersionEntity bundleVersion = bundleVersions.get(0);
        assertEquals("eb1-v1", bundleVersion.getId());
    }

    @Test
    public void testGetExtensionBundleVersionsByBucketGroupArtifactWhenDoesNotExist() {
        final String bucketId = "3";
        final String groupId = "org.apache.nifi";
        final String artifactId = "does-not-exist";

        final List<BundleVersionEntity> bundleVersions = metadataService.getBundleVersions(bucketId, groupId, artifactId);
        assertNotNull(bundleVersions);
        assertEquals(0, bundleVersions.size());
    }

    @Test
    public void testGetExtensionBundleVersionsGlobal() {
        final String groupId = "org.apache.nifi";
        final String artifactId = "nifi-example-processors-nar";
        final String version = "1.0.0";

        final List<BundleVersionEntity> bundleVersions = metadataService.getBundleVersionsGlobal(groupId, artifactId, version);
        assertNotNull(bundleVersions);
        assertEquals(1, bundleVersions.size());

        final BundleVersionEntity bundleVersion = bundleVersions.get(0);
        assertEquals("eb1-v1", bundleVersion.getId());
    }

    @Test
    public void testDeleteExtensionBundleVersion() {
        final BundleVersionEntity bundleVersion = metadataService.getBundleVersion("eb1", "1.0.0");
        assertNotNull(bundleVersion);

        metadataService.deleteBundleVersion(bundleVersion);

        final BundleVersionEntity deletedBundleVersion = metadataService.getBundleVersion("eb1", "1.0.0");
        assertNull(deletedBundleVersion);
    }

    // ---------- Extension Bundle Version Dependencies ------------

    @Test
    public void testCreateExtensionBundleVersionDependency() {
        final BundleVersionEntity versionEntity = metadataService.getBundleVersion("eb1", "1.0.0");
        assertNotNull(versionEntity);

        final List<BundleVersionDependencyEntity> dependencies = metadataService.getDependenciesForBundleVersion(versionEntity.getId());
        assertNotNull(dependencies);
        assertEquals(1, dependencies.size());

        final BundleVersionDependencyEntity dependencyEntity = new BundleVersionDependencyEntity();
        dependencyEntity.setId(UUID.randomUUID().toString());
        dependencyEntity.setExtensionBundleVersionId(versionEntity.getId());
        dependencyEntity.setGroupId("com.foo");
        dependencyEntity.setArtifactId("foo-nar");
        dependencyEntity.setVersion("1.1.1");

        metadataService.createDependency(dependencyEntity);

        final List<BundleVersionDependencyEntity> dependencies2 = metadataService.getDependenciesForBundleVersion(versionEntity.getId());
        assertNotNull(dependencies2);
        assertEquals(2, dependencies2.size());
    }

    @Test
    public void testGetExtensionBundleVersionDependencies() {
        final List<BundleVersionDependencyEntity> dependencies = metadataService.getDependenciesForBundleVersion("eb1-v1");
        assertNotNull(dependencies);
        assertEquals(1, dependencies.size());

        final BundleVersionDependencyEntity dependency = dependencies.get(0);
        assertEquals("eb1-v1-dep1", dependency.getId());
        assertEquals("eb1-v1", dependency.getExtensionBundleVersionId());
        assertEquals("org.apache.nifi", dependency.getGroupId());
        assertEquals("nifi-example-service-api-nar", dependency.getArtifactId());
        assertEquals("2.0.0", dependency.getVersion());
    }

    @Test
    public void testGetExtensionBundleVersionDependenciesWhenNoneExist() {
        final List<BundleVersionDependencyEntity> dependencies = metadataService.getDependenciesForBundleVersion("DOES-NOT-EXIST");
        assertNotNull(dependencies);
        assertEquals(0, dependencies.size());
    }

    //----------------- Extensions ---------------------------------

    @Test
    public void testCreateExtension() {
        final String extensionId = "4";

        final ExtensionRestrictionEntity restrictionEntity = new ExtensionRestrictionEntity();
        restrictionEntity.setId(UUID.randomUUID().toString());
        restrictionEntity.setExtensionId(extensionId);
        restrictionEntity.setRequiredPermission("read filesystem");
        restrictionEntity.setExplanation("Reads filesystem");

        final ExtensionProvidedServiceApiEntity serviceApiEntity = new ExtensionProvidedServiceApiEntity();
        serviceApiEntity.setId(UUID.randomUUID().toString());
        serviceApiEntity.setExtensionId(extensionId);
        serviceApiEntity.setClassName("com.foo.FooService");
        serviceApiEntity.setGroupId("com.foo");
        serviceApiEntity.setArtifactId("foo-nar");
        serviceApiEntity.setVersion("1.0.0");

        final ExtensionEntity extension = new ExtensionEntity();
        extension.setId(extensionId);
        extension.setBundleVersionId("eb1-v1");
        extension.setName("com.example.FooBarProcessor");
        extension.setDisplayName("FooBarProcessor");
        extension.setExtensionType(ExtensionType.PROCESSOR);
        extension.setTags(new HashSet<>(Arrays.asList("tag1", "tag2")));
        extension.setProvidedServiceApis(Collections.singleton(serviceApiEntity));
        extension.setRestrictions(Collections.singleton(restrictionEntity));
        extension.setContent("{ \"name\" : \"org.apache.nifi.ExampleProcessor\", \"type\" : \"PROCESSOR\" }");
        extension.setAdditionalDetails("DETAILS");

        metadataService.createExtension(extension);

        final ExtensionEntity retrievedExtension = metadataService.getExtensionById(extension.getId());
        assertEquals(extension.getId(), retrievedExtension.getId());
        assertEquals(extension.getBundleVersionId(), retrievedExtension.getBundleVersionId());
        assertEquals(extension.getName(), retrievedExtension.getName());
        assertEquals(extension.getDisplayName(), retrievedExtension.getDisplayName());
        assertEquals(extension.getExtensionType(), retrievedExtension.getExtensionType());
        assertEquals(extension.getContent(), retrievedExtension.getContent());
    }

    @Test
    public void testGetExtensionById() {
        final ExtensionEntity extension = metadataService.getExtensionById("e1");
        assertNotNull(extension);
        assertEquals("e1", extension.getId());
        assertEquals("org.apache.nifi.ExampleProcessor", extension.getName());
        assertEquals("{ \"name\" : \"org.apache.nifi.ExampleProcessor\", \"type\" : \"PROCESSOR\" }", extension.getContent());
        assertFalse(extension.getHasAdditionalDetails());
    }

    @Test
    public void testGetExtensionByIdWhenHasAdditionalDetails() {
        final ExtensionEntity extension = metadataService.getExtensionById("e3");
        assertNotNull(extension);
        assertEquals("e3", extension.getId());
        assertTrue(extension.getHasAdditionalDetails());
    }

    @Test
    public void testGetExtensionByIdWithServiceApi() {
        final ExtensionEntity extension = metadataService.getExtensionById("e3");
        assertNotNull(extension);
        assertEquals("e3", extension.getId());
        assertEquals("org.apache.nifi.ExampleService", extension.getName());
        assertEquals("{ \"name\" : \"org.apache.nifi.ExampleService\", \"type\" : \"CONTROLLER_SERVICE\" }", extension.getContent());
    }

    @Test
    public void testGetExtensionByIdWithRestriction() {
        final ExtensionEntity extension = metadataService.getExtensionById("e2");
        assertNotNull(extension);
        assertEquals("e2", extension.getId());
        assertEquals("org.apache.nifi.ExampleProcessorRestricted", extension.getName());
        assertEquals("{ \"name\" : \"org.apache.nifi.ExampleProcessorRestricted\", \"type\" : \"PROCESSOR\" }", extension.getContent());
    }

    @Test
    public void testGetExtensionByIdDoesNotExist() {
        final ExtensionEntity extension = metadataService.getExtensionById("does-not-exist");
        assertNull(extension);
    }

    @Test
    public void testGetExtensionByName() {
        final ExtensionEntity entity = metadataService.getExtensionByName("eb1-v1", "org.apache.nifi.ExampleProcessor");
        assertNotNull(entity);
        assertEquals("org.apache.nifi.ExampleProcessor", entity.getName());
        assertEquals("eb1-v1", entity.getBundleVersionId());
    }

    @Test
    public void testGetExtensionByNameDoesNotExist() {
        final ExtensionEntity entity = metadataService.getExtensionByName("eb1-v1", "org.apache.nifi.DOESNOTEXIST");
        assertNull(entity);
    }

    @Test
    public void testGetExtensionAdditionalDetailsWhenPresent() {
        final ExtensionAdditionalDetailsEntity entity = metadataService.getExtensionAdditionalDetails("eb2-v1", "org.apache.nifi.ExampleService");
        assertNotNull(entity);
        assertEquals("e3", entity.getExtensionId());
        assertTrue(entity.getAdditionalDetails().isPresent());
    }

    @Test
    public void testGetExtensionAdditionalDetailsWhenNotPresent() {
        final ExtensionAdditionalDetailsEntity entity = metadataService.getExtensionAdditionalDetails("eb1-v1", "org.apache.nifi.ExampleProcessor");
        assertNotNull(entity);
        assertEquals("e1", entity.getExtensionId());
        assertFalse(entity.getAdditionalDetails().isPresent());
    }

    @Test
    public void testGetExtensionAdditionalDetailsWhenExtensionDoesNotExist() {
        final ExtensionAdditionalDetailsEntity entity = metadataService.getExtensionAdditionalDetails("eb1-v1", "org.apache.nifi.DOESNOTEXIST");
        assertNull(entity);
    }

    @Test
    public void testGetAllExtensions() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final List<ExtensionEntity> extensions = metadataService.getExtensions(bucketIds, null);
        assertNotNull(extensions);
        assertEquals(3, extensions.size());
    }

    @Test
    public void testGetAllExtensionsFilteredWithResult() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                .extensionType(ExtensionType.CONTROLLER_SERVICE)
                .bundleType(BundleType.NIFI_NAR)
                .tag("SERVICE")
                .tag("example")
                .build();

        final List<ExtensionEntity> extensions = metadataService.getExtensions(bucketIds, filterParams);
        assertNotNull(extensions);
        assertEquals(1, extensions.size());

        final ExtensionEntity entity = extensions.get(0);
        assertEquals(ExtensionType.CONTROLLER_SERVICE.toString(), entity.getExtensionType().toString());
        assertEquals(BundleType.NIFI_NAR, entity.getBundleType());
    }

    @Test
    public void testGetAllExtensionsFilteredWithNoResult() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                .tag("DOES NOT EXIST")
                .build();

        final List<ExtensionEntity> extensions = metadataService.getExtensions(bucketIds, filterParams);
        assertNotNull(extensions);
        assertEquals(0, extensions.size());
    }

    @Test
    public void testGetAllExtensionsFilteredWithNoBuckets() {
        final Set<String> bucketIds = new HashSet<>();

        final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                .extensionType(ExtensionType.CONTROLLER_SERVICE)
                .bundleType(BundleType.NIFI_NAR)
                .tag("SERVICE")
                .tag("example")
                .build();

        final List<ExtensionEntity> extensions = metadataService.getExtensions(bucketIds, filterParams);
        assertNotNull(extensions);
        assertEquals(0, extensions.size());
    }

    @Test
    public void testGetExtensionsByBundleVersionId() {
        final List<ExtensionEntity> extensions = metadataService.getExtensionsByBundleVersionId("eb1-v1");
        assertNotNull(extensions);
        assertEquals(2, extensions.size());
    }

    @Test
    public void testGetExtensionsByBundleVersionIdDoesNotExist() {
        final List<ExtensionEntity> extensions = metadataService.getExtensionsByBundleVersionId("does-not-exist");
        assertNotNull(extensions);
        assertEquals(0, extensions.size());
    }

    @Test
    public void testGetExtensionTags() {
        final List<TagCountEntity> tags = metadataService.getAllExtensionTags();
        assertNotNull(tags);
        assertEquals(4, tags.size());

        tags.forEach(t -> {
            assertNotNull(t.getTag());
            assertTrue(t.getCount() > 0);
        });
    }

    @Test
    public void testDeleteExtension() {
        final ExtensionEntity extension = metadataService.getExtensionById("e1");
        assertNotNull(extension);

        metadataService.deleteExtension(extension);

        final ExtensionEntity deletedExtension = metadataService.getExtensionById("e1");
        assertNull(deletedExtension);
    }

    @Test
    public void testGetExtensionsByProvidedServiceApi() {
        final Set<String> bucketIds = new HashSet<>();
        bucketIds.add("1");
        bucketIds.add("2");
        bucketIds.add("3");

        final ProvidedServiceAPI serviceAPI = new ProvidedServiceAPI();
        serviceAPI.setClassName("org.apache.nifi.ExampleServiceAPI");
        serviceAPI.setGroupId("org.apache.nifi");
        serviceAPI.setArtifactId("nifi-example-service-api-nar");
        serviceAPI.setVersion("2.0.0");

        final List<ExtensionEntity> extensions = metadataService.getExtensionsByProvidedServiceApi(bucketIds, serviceAPI);
        assertEquals(1, extensions.size());
        assertEquals("e3", extensions.get(0).getId());
    }

    @Test
    public void testGetExtensionsByProvidedServiceApiWithNoBuckets() {
        final Set<String> bucketIds = new HashSet<>();

        final ProvidedServiceAPI serviceAPI = new ProvidedServiceAPI();
        serviceAPI.setClassName("org.apache.nifi.ExampleServiceAPI");
        serviceAPI.setGroupId("org.apache.nifi");
        serviceAPI.setArtifactId("nifi-example-service-api-nar");
        serviceAPI.setVersion("2.0.0");

        final List<ExtensionEntity> extensions = metadataService.getExtensionsByProvidedServiceApi(bucketIds, serviceAPI);
        assertEquals(0, extensions.size());
    }

}
