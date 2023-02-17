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
package org.apache.nifi.registry.provider.extension;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.registry.extension.BundleCoordinate;
import org.apache.nifi.registry.extension.BundlePersistenceContext;
import org.apache.nifi.registry.extension.BundlePersistenceException;
import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;
import org.apache.nifi.registry.extension.BundleVersionType;
import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

public class TestFileSystemBundlePersistenceProvider {

    static final String EXTENSION_STORAGE_DIR = "target/extension_storage";

    static final ProviderConfigurationContext CONFIGURATION_CONTEXT = new ProviderConfigurationContext() {
        @Override
        public Map<String, String> getProperties() {
            final Map<String,String> props = new HashMap<>();
            props.put(FileSystemBundlePersistenceProvider.BUNDLE_STORAGE_DIR_PROP, EXTENSION_STORAGE_DIR);
            return props;
        }
    };

    private File bundleStorageDir;
    private BundlePersistenceProvider fileSystemBundleProvider;

    @Before
    public void setup() throws IOException {
        bundleStorageDir = new File(EXTENSION_STORAGE_DIR);
        if (bundleStorageDir.exists()) {
            org.apache.commons.io.FileUtils.cleanDirectory(bundleStorageDir);
            bundleStorageDir.delete();
        }

        Assert.assertFalse(bundleStorageDir.exists());

        fileSystemBundleProvider = new FileSystemBundlePersistenceProvider();
        fileSystemBundleProvider.onConfigured(CONFIGURATION_CONTEXT);
        Assert.assertTrue(bundleStorageDir.exists());
    }

    @Test
    public void testCreateSuccessfully() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        // first version in b1
        final String content1 = "g1-a1-1.0.0";
        final BundleVersionCoordinate versionCoordinate1 = getVersionCoordinate("b1", "g1", "a1", "1.0.0", type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate1 , content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate1, content1);

        // second version in b1
        final String content2 = "g1-a1-1.1.0";
        final BundleVersionCoordinate versionCoordinate2 = getVersionCoordinate("b1", "g1", "a1", "1.1.0", type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate2, content2);
        verifyBundleVersion(bundleStorageDir, versionCoordinate2, content2);

        // same bundle but in b2
        final String content3 = "g1-a1-1.1.0";
        final BundleVersionCoordinate versionCoordinate3 = getVersionCoordinate("b2", "g1", "a1", "1.1.0", type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate3, content3);
        verifyBundleVersion(bundleStorageDir, versionCoordinate3, content2);
    }

    @Test
    public void testCreateWhenBundleVersionAlreadyExists() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        final String content1 = "g1-a1-1.0.0";
        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate("b1", "g1", "a1", "1.0.0", type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);

        // try to save same bundle version that already exists
        try {
            final String newContent = "new content";
            createBundleVersion(fileSystemBundleProvider, versionCoordinate, newContent);
            Assert.fail("Should have thrown exception");
        } catch (BundlePersistenceException e) {
            // expected
        }

        // verify existing content wasn't modified
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);
    }

    @Test
    public void testUpdateWhenBundleVersionAlreadyExists() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        final String content1 = "g1-a1-1.0.0";
        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate("b1", "g1", "a1", "1.0.0", type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);

        // try to save same bundle version that already exists with new content
        final String newContent = "new content";
        updateBundleVersion(fileSystemBundleProvider, versionCoordinate, newContent);
        verifyBundleVersion(bundleStorageDir, versionCoordinate, newContent);

        // retrieved content should be updated
        try (final OutputStream out = new ByteArrayOutputStream()) {
            fileSystemBundleProvider.getBundleVersionContent(versionCoordinate, out);
            final String retrievedContent = new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8);
            Assert.assertEquals(newContent, retrievedContent);
        }
    }

    @Test
    public void testCreateAndGet() throws IOException {
        final String bucketId = "b1";
        final String groupId = "g1";
        final String artifactId = "a1";
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        final String content1 = groupId + "-" + artifactId + "-" + "1.0.0";
        final BundleVersionCoordinate versionCoordinate1 = getVersionCoordinate(bucketId, groupId, artifactId, "1.0.0", type);
        createBundleVersion(fileSystemBundleProvider,versionCoordinate1, content1);

        final String content2 = groupId + "-" + artifactId + "-" + "1.1.0";
        final BundleVersionCoordinate versionCoordinate2 = getVersionCoordinate(bucketId, groupId, artifactId, "1.1.0", type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate2, content2);

        try (final OutputStream out = new ByteArrayOutputStream()) {
            fileSystemBundleProvider.getBundleVersionContent(versionCoordinate1, out);

            final String retrievedContent1 = new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8);
            Assert.assertEquals(content1, retrievedContent1);
        }

        try (final OutputStream out = new ByteArrayOutputStream()) {
            fileSystemBundleProvider.getBundleVersionContent(versionCoordinate2, out);

            final String retrievedContent2 = new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8);
            Assert.assertEquals(content2, retrievedContent2);
        }
    }

    @Test(expected = BundlePersistenceException.class)
    public void testGetWhenDoesNotExist() throws IOException {
        final String bucketId = "b1";
        final String groupId = "g1";
        final String artifactId = "a1";
        final String version = "1.0.0";
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        try (final OutputStream out = new ByteArrayOutputStream()) {
            final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(bucketId, groupId, artifactId, version, type);
            fileSystemBundleProvider.getBundleVersionContent(versionCoordinate, out);
            Assert.fail("Should have thrown exception");
        }
    }

    @Test
    public void testDeleteExtensionBundleVersion() throws IOException {
        final String bucketId = "b1";
        final String groupId = "g1";
        final String artifactId = "a1";
        final String version = "1.0.0";
        final BundleVersionType bundleType = BundleVersionType.NIFI_NAR;

        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(bucketId, groupId, artifactId, version, bundleType);

        // create and verify the bundle version
        final String content1 = groupId + "-" + artifactId + "-" + version;
        createBundleVersion(fileSystemBundleProvider, versionCoordinate, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);

        // delete the bundle version
        fileSystemBundleProvider.deleteBundleVersion(versionCoordinate);

        // verify it was deleted
        final File bundleVersionDir = FileSystemBundlePersistenceProvider.getBundleVersionDirectory(bundleStorageDir, versionCoordinate);
        final File bundleFile = FileSystemBundlePersistenceProvider.getBundleFile(bundleVersionDir, versionCoordinate);
        Assert.assertFalse(bundleFile.exists());
    }

    @Test
    public void testDeleteExtensionBundleVersionWhenDoesNotExist() throws IOException {
        final String bucketId = "b1";
        final String groupId = "g1";
        final String artifactId = "a1";
        final String version = "1.0.0";
        final BundleVersionType bundleType = BundleVersionType.NIFI_NAR;

        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(bucketId, groupId, artifactId, version, bundleType);

        // verify the bundle version does not already exist
        final File bundleVersionDir = FileSystemBundlePersistenceProvider.getBundleVersionDirectory(bundleStorageDir, versionCoordinate);
        final File bundleFile = FileSystemBundlePersistenceProvider.getBundleFile(bundleVersionDir, versionCoordinate);
        Assert.assertFalse(bundleFile.exists());

        // delete the bundle version
        fileSystemBundleProvider.deleteBundleVersion(versionCoordinate);
    }

    @Test
    public void testDeleteAllBundleVersions() throws IOException {
        final String bucketId = "b1";
        final String groupId = "g1";
        final String artifactId = "a1";
        final String version1 = "1.0.0";
        final String version2 = "2.0.0";
        final BundleVersionType bundleType = BundleVersionType.NIFI_NAR;

        // create and verify the bundle version 1
        final String content1 = groupId + "-" + artifactId + "-" + version1;
        final BundleVersionCoordinate versionCoordinate1 = getVersionCoordinate(bucketId, groupId, artifactId, version1, bundleType);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate1, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate1, content1);

        // create and verify the bundle version 2
        final String content2 = groupId + "-" + artifactId + "-" + version2;
        final BundleVersionCoordinate versionCoordinate2 = getVersionCoordinate(bucketId, groupId, artifactId, version2, bundleType);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate2, content2);
        verifyBundleVersion(bundleStorageDir, versionCoordinate2, content2);

        Assert.assertEquals(1, bundleStorageDir.listFiles().length);
        final BundleCoordinate bundleCoordinate = getBundleCoordinate(bucketId, groupId, artifactId);
        fileSystemBundleProvider.deleteAllBundleVersions(bundleCoordinate);
        Assert.assertEquals(0, bundleStorageDir.listFiles().length);
    }

    @Test
    public void testDeleteAllBundleVersionsWhenDoesNotExist() throws IOException {
        final String bucketId = "b1";
        final String groupId = "g1";
        final String artifactId = "a1";

        Assert.assertEquals(0, bundleStorageDir.listFiles().length);
        final BundleCoordinate bundleCoordinate = getBundleCoordinate(bucketId, groupId, artifactId);
        fileSystemBundleProvider.deleteAllBundleVersions(bundleCoordinate);
        Assert.assertEquals(0, bundleStorageDir.listFiles().length);
    }

    private void createBundleVersion(final BundlePersistenceProvider persistenceProvider,
                                     final BundleVersionCoordinate versionCoordinate,
                                     final String content) throws IOException {
        final BundlePersistenceContext context = getPersistenceContext(versionCoordinate);
        try (final InputStream in = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            persistenceProvider.createBundleVersion(context, in);
        }
    }

    private void updateBundleVersion(final BundlePersistenceProvider persistenceProvider,
                                     final BundleVersionCoordinate versionCoordinate,
                                     final String content) throws IOException {
        final BundlePersistenceContext context = getPersistenceContext(versionCoordinate);
        try (final InputStream in = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
            persistenceProvider.updateBundleVersion(context, in);
        }
    }

    private static BundlePersistenceContext getPersistenceContext(final BundleVersionCoordinate versionCoordinate) {
        final BundlePersistenceContext context = Mockito.mock(BundlePersistenceContext.class);
        when(context.getCoordinate()).thenReturn(versionCoordinate);
        return context;
    }

    private static BundleVersionCoordinate getVersionCoordinate(final String bucketId, final String groupId, final String artifactId,
                                                                final String version, final BundleVersionType bundleType) {

        final BundleVersionCoordinate coordinate = Mockito.mock(BundleVersionCoordinate.class);
        when(coordinate.getBucketId()).thenReturn(bucketId);
        when(coordinate.getGroupId()).thenReturn(groupId);
        when(coordinate.getArtifactId()).thenReturn(artifactId);
        when(coordinate.getVersion()).thenReturn(version);
        when(coordinate.getType()).thenReturn(bundleType);
        return coordinate;
    }

    private static BundleCoordinate getBundleCoordinate(final String bucketId, final String groupId, final String artifactId) {
        final BundleCoordinate coordinate = Mockito.mock(BundleCoordinate.class);
        when(coordinate.getBucketId()).thenReturn(bucketId);
        when(coordinate.getGroupId()).thenReturn(groupId);
        when(coordinate.getArtifactId()).thenReturn(artifactId);
        return coordinate;
    }

    private static void verifyBundleVersion(final File storageDir, final BundleVersionCoordinate versionCoordinate,
                                            final String contentString) throws IOException {

        final File bundleVersionDir = FileSystemBundlePersistenceProvider.getBundleVersionDirectory(storageDir, versionCoordinate);
        final File bundleFile = FileSystemBundlePersistenceProvider.getBundleFile(bundleVersionDir, versionCoordinate);
        Assert.assertTrue(bundleFile.exists());

        try (InputStream in = new FileInputStream(bundleFile)) {
            Assert.assertEquals(contentString, IOUtils.toString(in, StandardCharsets.UTF_8));
        }
    }

}
