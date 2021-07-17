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
package org.apache.nifi.registry.web.link;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.bucket.BucketItemType;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleInfo;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.extension.component.ExtensionMetadata;
import org.apache.nifi.registry.extension.repo.ExtensionRepoArtifact;
import org.apache.nifi.registry.extension.repo.ExtensionRepoBucket;
import org.apache.nifi.registry.extension.repo.ExtensionRepoExtensionMetadata;
import org.apache.nifi.registry.extension.repo.ExtensionRepoGroup;
import org.apache.nifi.registry.extension.repo.ExtensionRepoVersionSummary;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestLinkService {

    private static final String BASE_URI = "http://localhost:18080/nifi-registry-api";
    private URI baseUri = UriBuilder.fromUri(BASE_URI).build();

    private LinkService linkService;

    private List<Bucket> buckets;
    private List<VersionedFlow> flows;
    private List<VersionedFlowSnapshotMetadata> snapshots;
    private List<BucketItem> items;

    private List<Bundle> bundles;
    private List<BundleVersionMetadata> bundleVersionMetadata;
    private List<ExtensionMetadata> extensionMetadata;

    private List<ExtensionRepoBucket> extensionRepoBuckets;
    private List<ExtensionRepoGroup> extensionRepoGroups;
    private List<ExtensionRepoArtifact> extensionRepoArtifacts;
    private List<ExtensionRepoVersionSummary> extensionRepoVersions;
    private List<ExtensionRepoExtensionMetadata> extensionRepoExtensionMetadata;

    @Before
    public void setup() {
        linkService = new LinkService();

        // setup buckets
        final Bucket bucket1 = new Bucket();
        bucket1.setIdentifier("b1");
        bucket1.setName("Bucket_1");

        final Bucket bucket2 = new Bucket();
        bucket2.setIdentifier("b2");
        bucket2.setName("Bucket_2");

        buckets = new ArrayList<>();
        buckets.add(bucket1);
        buckets.add(bucket2);

        // setup flows
        final VersionedFlow flow1 = new VersionedFlow();
        flow1.setIdentifier("f1");
        flow1.setName("Flow_1");
        flow1.setBucketIdentifier(bucket1.getIdentifier());

        final VersionedFlow flow2 = new VersionedFlow();
        flow2.setIdentifier("f2");
        flow2.setName("Flow_2");
        flow2.setBucketIdentifier(bucket1.getIdentifier());

        flows = new ArrayList<>();
        flows.add(flow1);
        flows.add(flow2);

        //setup snapshots
        final VersionedFlowSnapshotMetadata snapshotMetadata1 = new VersionedFlowSnapshotMetadata();
        snapshotMetadata1.setFlowIdentifier(flow1.getIdentifier());
        snapshotMetadata1.setVersion(1);
        snapshotMetadata1.setBucketIdentifier(bucket1.getIdentifier());

        final VersionedFlowSnapshotMetadata snapshotMetadata2 = new VersionedFlowSnapshotMetadata();
        snapshotMetadata2.setFlowIdentifier(flow1.getIdentifier());
        snapshotMetadata2.setVersion(2);
        snapshotMetadata2.setBucketIdentifier(bucket1.getIdentifier());

        snapshots = new ArrayList<>();
        snapshots.add(snapshotMetadata1);
        snapshots.add(snapshotMetadata2);

        // setup extension bundles
        final Bundle bundle1 = new Bundle();
        bundle1.setIdentifier("eb1");

        final Bundle bundle2 = new Bundle();
        bundle2.setIdentifier("eb2");

        bundles = new ArrayList<>();
        bundles.add(bundle1);
        bundles.add(bundle2);

        // setup extension bundle versions
        final BundleVersionMetadata bundleVersion1 = new BundleVersionMetadata();
        bundleVersion1.setBundleId(bundle1.getIdentifier());
        bundleVersion1.setVersion("1.0.0");

        final BundleVersionMetadata bundleVersion2 = new BundleVersionMetadata();
        bundleVersion2.setBundleId(bundle1.getIdentifier());
        bundleVersion2.setVersion("2.0.0");

        bundleVersionMetadata = new ArrayList<>();
        bundleVersionMetadata.add(bundleVersion1);
        bundleVersionMetadata.add(bundleVersion2);

        // setup extension metadata
        final BundleInfo bundleInfo1 = new BundleInfo();
        bundleInfo1.setBucketName("Bucket1");
        bundleInfo1.setBucketId("Bucket1");
        bundleInfo1.setBundleId("bundle1");
        bundleInfo1.setGroupId("Group1");
        bundleInfo1.setArtifactId("Artifact1");
        bundleInfo1.setVersion("1");

        final ExtensionMetadata extensionMetadata1 = new ExtensionMetadata();
        extensionMetadata1.setName("Extension1");
        extensionMetadata1.setBundleInfo(bundleInfo1);

        final ExtensionMetadata extensionMetadata2 = new ExtensionMetadata();
        extensionMetadata2.setName("Extension2");
        extensionMetadata2.setBundleInfo(bundleInfo1);

        extensionMetadata = new ArrayList<>();
        extensionMetadata.add(extensionMetadata1);
        extensionMetadata.add(extensionMetadata2);

        // setup extension repo buckets
        final ExtensionRepoBucket rb1 = new ExtensionRepoBucket();
        rb1.setBucketName(bucket1.getName());

        final ExtensionRepoBucket rb2 = new ExtensionRepoBucket();
        rb2.setBucketName(bucket2.getName());

        extensionRepoBuckets = new ArrayList<>();
        extensionRepoBuckets.add(rb1);
        extensionRepoBuckets.add(rb2);

        // setup extension repo groups
        final ExtensionRepoGroup rg1 = new ExtensionRepoGroup();
        rg1.setBucketName(rb1.getBucketName());
        rg1.setGroupId("g1");

        final ExtensionRepoGroup rg2 = new ExtensionRepoGroup();
        rg2.setBucketName(rb1.getBucketName());
        rg2.setGroupId("g2");

        extensionRepoGroups = new ArrayList<>();
        extensionRepoGroups.add(rg1);
        extensionRepoGroups.add(rg2);

        // setup extension repo artifacts
        final ExtensionRepoArtifact ra1 = new ExtensionRepoArtifact();
        ra1.setBucketName(rb1.getBucketName());
        ra1.setGroupId(rg1.getGroupId());
        ra1.setArtifactId("a1");

        final ExtensionRepoArtifact ra2 = new ExtensionRepoArtifact();
        ra2.setBucketName(rb1.getBucketName());
        ra2.setGroupId(rg1.getGroupId());
        ra2.setArtifactId("a2");

        extensionRepoArtifacts = new ArrayList<>();
        extensionRepoArtifacts.add(ra1);
        extensionRepoArtifacts.add(ra2);

        // setup extension repo versions
        final ExtensionRepoVersionSummary rv1 = new ExtensionRepoVersionSummary();
        rv1.setBucketName(rb1.getBucketName());
        rv1.setGroupId(rg1.getGroupId());
        rv1.setArtifactId(ra1.getArtifactId());
        rv1.setVersion("1.0.0");

        final ExtensionRepoVersionSummary rv2 = new ExtensionRepoVersionSummary();
        rv2.setBucketName(rb1.getBucketName());
        rv2.setGroupId(rg1.getGroupId());
        rv2.setArtifactId(ra1.getArtifactId());
        rv2.setVersion("2.0.0");

        extensionRepoVersions = new ArrayList<>();
        extensionRepoVersions.add(rv1);
        extensionRepoVersions.add(rv2);

        // setup extension repo extension metadata
        extensionRepoExtensionMetadata = new ArrayList<>();
        extensionRepoExtensionMetadata.add(new ExtensionRepoExtensionMetadata(extensionMetadata1));
        extensionRepoExtensionMetadata.add(new ExtensionRepoExtensionMetadata(extensionMetadata2));

        // setup items
        items = new ArrayList<>();
        items.add(flow1);
        items.add(flow2);
        items.add(bundle1);
        items.add(bundle2);
    }

    @Test
    public void testPopulateBucketLinks() {
        buckets.forEach(b -> Assert.assertNull(b.getLink()));
        linkService.populateLinks(buckets);
        buckets.forEach(b -> Assert.assertEquals(
                "buckets/" + b.getIdentifier(), b.getLink().getUri().toString()));
    }

    @Test
    public void testPopulateFlowLinks() {
        flows.forEach(f -> Assert.assertNull(f.getLink()));
        linkService.populateLinks(flows);
        flows.forEach(f -> Assert.assertEquals(
                "buckets/" + f.getBucketIdentifier() + "/flows/" + f.getIdentifier(), f.getLink().getUri().toString()));
    }

    @Test
    public void testPopulateSnapshotLinks() {
        snapshots.forEach(s -> Assert.assertNull(s.getLink()));
        linkService.populateLinks(snapshots);
        snapshots.forEach(s -> Assert.assertEquals(
                "buckets/" + s.getBucketIdentifier() + "/flows/" + s.getFlowIdentifier() + "/versions/" + s.getVersion(), s.getLink().getUri().toString()));
    }

    @Test
    public void testPopulateItemLinks() {
        items.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(items);
        items.forEach(i -> {
            if (i.getType() == BucketItemType.Flow) {
                Assert.assertEquals("buckets/" + i.getBucketIdentifier() + "/flows/" + i.getIdentifier(), i.getLink().getUri().toString());
            } else {
                Assert.assertEquals("bundles/" + i.getIdentifier(), i.getLink().getUri().toString());
            }
        });
    }

    @Test
    public void testPopulateExtensionBundleLinks() {
        bundles.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(bundles);
        bundles.forEach(eb -> Assert.assertEquals("bundles/" + eb.getIdentifier(), eb.getLink().getUri().toString()));
    }

    @Test
    public void testPopulateExtensionBundleVersionLinks() {
        bundleVersionMetadata.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(bundleVersionMetadata);
        bundleVersionMetadata.forEach(eb -> Assert.assertEquals(
                "bundles/" + eb.getBundleId() + "/versions/" + eb.getVersion(), eb.getLink().getUri().toString()));
    }

    @Test
    public void testPopulateExtensionBundleVersionExtensionMetadataLinks() {
        extensionMetadata.forEach(i -> Assert.assertNull(i.getLink()));
        extensionMetadata.forEach(i -> Assert.assertNull(i.getLinkDocs()));

        linkService.populateLinks(extensionMetadata);

        extensionMetadata.forEach(e -> {
            final String extensionUri = "bundles/" + e.getBundleInfo().getBundleId()
                    + "/versions/" + e.getBundleInfo().getVersion()
                    + "/extensions/" + e.getName();
            Assert.assertEquals(extensionUri, e.getLink().getUri().toString());
            Assert.assertEquals(extensionUri + "/docs", e.getLinkDocs().getUri().toString());
        });
    }

    @Test
    public void testPopulateExtensionRepoBucketLinks() {
        extensionRepoBuckets.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(extensionRepoBuckets);
        extensionRepoBuckets.forEach(i -> Assert.assertEquals(
                "extension-repository/" + i.getBucketName(),
                i.getLink().getUri().toString())
        );
    }

    @Test
    public void testPopulateExtensionRepoGroupLinks() {
        extensionRepoGroups.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(extensionRepoGroups);
        extensionRepoGroups.forEach(i -> {
            Assert.assertEquals(
                    "extension-repository/" + i.getBucketName() + "/" + i.getGroupId(),
                    i.getLink().getUri().toString()); }
        );
    }

    @Test
    public void testPopulateExtensionRepoArtifactLinks() {
        extensionRepoArtifacts.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(extensionRepoArtifacts);
        extensionRepoArtifacts.forEach(i -> {
            Assert.assertEquals(
                    "extension-repository/" + i.getBucketName() + "/" + i.getGroupId() + "/" + i.getArtifactId(),
                    i.getLink().getUri().toString()); }
        );
    }

    @Test
    public void testPopulateExtensionRepoVersionLinks() {
        extensionRepoVersions.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateLinks(extensionRepoVersions);
        extensionRepoVersions.forEach(i -> {
            Assert.assertEquals(
                    "extension-repository/" + i.getBucketName() + "/" + i.getGroupId() + "/" + i.getArtifactId() + "/" + i.getVersion(),
                    i.getLink().getUri().toString()); }
        );
    }

    @Test
    public void testPopulateExtensionRepoVersionFullLinks() {
        extensionRepoVersions.forEach(i -> Assert.assertNull(i.getLink()));
        linkService.populateFullLinks(extensionRepoVersions, baseUri);
        extensionRepoVersions.forEach(i -> {
            Assert.assertEquals(
                    BASE_URI + "/extension-repository/" + i.getBucketName() + "/" + i.getGroupId() + "/" + i.getArtifactId() + "/" + i.getVersion(),
                    i.getLink().getUri().toString()); }
        );
    }

    @Test
    public void testPopulateExtensionRepoExtensionMetdataFullLinks() {
        extensionRepoExtensionMetadata.forEach(i -> Assert.assertNull(i.getLink()));
        extensionRepoExtensionMetadata.forEach(i -> Assert.assertNull(i.getLinkDocs()));

        linkService.populateFullLinks(extensionRepoExtensionMetadata, baseUri);
        extensionRepoExtensionMetadata.forEach(i -> {
            final BundleInfo bi = i.getExtensionMetadata().getBundleInfo();
            final String extensionUri = BASE_URI + "/extension-repository/" + bi.getBucketName() + "/" + bi.getGroupId() + "/"
                    + bi.getArtifactId() + "/" + bi.getVersion() + "/extensions/" + i.getExtensionMetadata().getName();
            Assert.assertEquals(extensionUri, i.getLink().getUri().toString());
            Assert.assertEquals(extensionUri + "/docs", i.getLinkDocs().getUri().toString());
        });
    }
}
