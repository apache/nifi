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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.extension.ExtensionMetadata;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.BundleVersionClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.util.FileUtils;
import org.bouncycastle.util.encoders.Hex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test all basic functionality of JerseyNiFiRegistryClient.
 */
public class UnsecuredNiFiRegistryClientIT extends UnsecuredITBase {

    static final Logger LOGGER = LoggerFactory.getLogger(UnsecuredNiFiRegistryClientIT.class);

    static final String CLIENT_ID = "UnsecuredNiFiRegistryClientIT";

    private NiFiRegistryClient client;

    @BeforeEach
    public void setup() {
        final String baseUrl = createBaseURL();
        LOGGER.info("Using base url = {}", baseUrl);

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
                LOGGER.warn("Unable to delete extensions storage dir", e);
            }
        }
    }

    @AfterEach
    public void teardown() {
        try {
            client.close();
        } catch (Exception ignored) {

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

    private static VersionedFlowSnapshot createSnapshot(FlowSnapshotClient client, VersionedFlow flow, int num) throws IOException, NiFiRegistryException {
        final VersionedFlowSnapshot snapshot = buildSnapshot(flow, num);

        return client.create(snapshot);
    }
}
