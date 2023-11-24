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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestFileSystemBundlePersistenceProvider {

    static final String EXTENSION_STORAGE_DIR = "target/extension_storage";

    private static final String BUCKET_ID = "b0000000-0000-0000-0000-000000000000";

    private static final String SECOND_BUCKET_ID = "b2000000-0000-0000-0000-000000000000";

    private static final String GROUP_ID = "c0000000-0000-0000-0000-000000000000";

    private static final String ARTIFACT_ID = "a0000000-0000-0000-0000-000000000000";

    private static final String FIRST_VERSION = "1.0.0";

    private static final String SECOND_VERSION = "1.1.0";

    static final ProviderConfigurationContext CONFIGURATION_CONTEXT = () -> {
        final Map<String,String> props = new HashMap<>();
        props.put(FileSystemBundlePersistenceProvider.BUNDLE_STORAGE_DIR_PROP, EXTENSION_STORAGE_DIR);
        return props;
    };

    private File bundleStorageDir;
    private BundlePersistenceProvider fileSystemBundleProvider;

    @BeforeEach
    public void setup() throws IOException {
        bundleStorageDir = new File(EXTENSION_STORAGE_DIR);
        if (bundleStorageDir.exists()) {
            org.apache.commons.io.FileUtils.cleanDirectory(bundleStorageDir);
            bundleStorageDir.delete();
        }

        assertFalse(bundleStorageDir.exists());

        fileSystemBundleProvider = new FileSystemBundlePersistenceProvider();
        fileSystemBundleProvider.onConfigured(CONFIGURATION_CONTEXT);
        assertTrue(bundleStorageDir.exists());
    }

    @Test
    public void testCreateSuccessfully() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        // first version in b1
        final String content1 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, FIRST_VERSION);
        final BundleVersionCoordinate versionCoordinate1 = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate1 , content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate1, content1);

        // second version in b1
        final String content2 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, SECOND_VERSION);
        final BundleVersionCoordinate versionCoordinate2 = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, SECOND_VERSION, type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate2, content2);
        verifyBundleVersion(bundleStorageDir, versionCoordinate2, content2);

        // same bundle but in b2
        final String content3 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, SECOND_VERSION);
        final BundleVersionCoordinate versionCoordinate3 = getVersionCoordinate(SECOND_BUCKET_ID, GROUP_ID, ARTIFACT_ID, SECOND_VERSION, type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate3, content3);
        verifyBundleVersion(bundleStorageDir, versionCoordinate3, content2);
    }

    @Test
    public void testCreateWhenBundleVersionAlreadyExists() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        final String content1 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, FIRST_VERSION);
        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);

        // try to save same bundle version that already exists
        final String newContent = "new content";
        assertThrows(BundlePersistenceException.class, () -> createBundleVersion(fileSystemBundleProvider, versionCoordinate, newContent));

        // verify existing content wasn't modified
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);
    }

    @Test
    public void testUpdateWhenBundleVersionAlreadyExists() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        final String content1 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, FIRST_VERSION);
        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, type);
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
            assertEquals(newContent, retrievedContent);
        }
    }

    @Test
    public void testCreateAndGet() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        final String content1 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, FIRST_VERSION);
        final BundleVersionCoordinate versionCoordinate1 = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, type);
        createBundleVersion(fileSystemBundleProvider,versionCoordinate1, content1);

        final String content2 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, SECOND_VERSION);
        final BundleVersionCoordinate versionCoordinate2 = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, SECOND_VERSION, type);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate2, content2);

        try (final OutputStream out = new ByteArrayOutputStream()) {
            fileSystemBundleProvider.getBundleVersionContent(versionCoordinate1, out);

            final String retrievedContent1 = new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8);
            assertEquals(content1, retrievedContent1);
        }

        try (final OutputStream out = new ByteArrayOutputStream()) {
            fileSystemBundleProvider.getBundleVersionContent(versionCoordinate2, out);

            final String retrievedContent2 = new String(((ByteArrayOutputStream) out).toByteArray(), StandardCharsets.UTF_8);
            assertEquals(content2, retrievedContent2);
        }
    }

    @Test
    public void testGetWhenDoesNotExist() throws IOException {
        final BundleVersionType type = BundleVersionType.NIFI_NAR;

        try (final OutputStream out = new ByteArrayOutputStream()) {
            final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, type);
            assertThrows(BundlePersistenceException.class, () -> fileSystemBundleProvider.getBundleVersionContent(versionCoordinate, out));
        }
    }

    @Test
    public void testDeleteExtensionBundleVersion() throws IOException {
        final BundleVersionType bundleType = BundleVersionType.NIFI_NAR;

        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, bundleType);

        // create and verify the bundle version
        final String content1 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, FIRST_VERSION);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate, content1);

        // delete the bundle version
        fileSystemBundleProvider.deleteBundleVersion(versionCoordinate);

        // verify it was deleted
        final File bundleVersionDir = FileSystemBundlePersistenceProvider.getBundleVersionDirectory(bundleStorageDir, versionCoordinate);
        final File bundleFile = FileSystemBundlePersistenceProvider.getBundleFile(bundleVersionDir, versionCoordinate);
        assertFalse(bundleFile.exists());
    }

    @Test
    public void testDeleteExtensionBundleVersionWhenDoesNotExist() {
        final BundleVersionType bundleType = BundleVersionType.NIFI_NAR;

        final BundleVersionCoordinate versionCoordinate = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, bundleType);

        // verify the bundle version does not already exist
        final File bundleVersionDir = FileSystemBundlePersistenceProvider.getBundleVersionDirectory(bundleStorageDir, versionCoordinate);
        final File bundleFile = FileSystemBundlePersistenceProvider.getBundleFile(bundleVersionDir, versionCoordinate);
        assertFalse(bundleFile.exists());

        // delete the bundle version
        fileSystemBundleProvider.deleteBundleVersion(versionCoordinate);
    }

    @Test
    public void testDeleteAllBundleVersions() throws IOException {
        final BundleVersionType bundleType = BundleVersionType.NIFI_NAR;

        // create and verify the bundle version 1
        final String content1 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, FIRST_VERSION);
        final BundleVersionCoordinate versionCoordinate1 = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, FIRST_VERSION, bundleType);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate1, content1);
        verifyBundleVersion(bundleStorageDir, versionCoordinate1, content1);

        // create and verify the bundle version 2
        final String content2 = String.format("%s-%s-%s", GROUP_ID, ARTIFACT_ID, SECOND_VERSION);
        final BundleVersionCoordinate versionCoordinate2 = getVersionCoordinate(BUCKET_ID, GROUP_ID, ARTIFACT_ID, SECOND_VERSION, bundleType);
        createBundleVersion(fileSystemBundleProvider, versionCoordinate2, content2);
        verifyBundleVersion(bundleStorageDir, versionCoordinate2, content2);

        assertEquals(1, bundleStorageDir.listFiles().length);
        final BundleCoordinate bundleCoordinate = getBundleCoordinate();
        fileSystemBundleProvider.deleteAllBundleVersions(bundleCoordinate);
        assertEquals(0, bundleStorageDir.listFiles().length);
    }

    @Test
    public void testDeleteAllBundleVersionsWhenDoesNotExist() {
        assertEquals(0, bundleStorageDir.listFiles().length);
        final BundleCoordinate bundleCoordinate = getBundleCoordinate();
        fileSystemBundleProvider.deleteAllBundleVersions(bundleCoordinate);
        assertEquals(0, bundleStorageDir.listFiles().length);
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

    private static BundleCoordinate getBundleCoordinate() {
        final BundleCoordinate coordinate = Mockito.mock(BundleCoordinate.class);
        when(coordinate.getBucketId()).thenReturn(BUCKET_ID);
        when(coordinate.getGroupId()).thenReturn(GROUP_ID);
        when(coordinate.getArtifactId()).thenReturn(ARTIFACT_ID);
        return coordinate;
    }

    private static void verifyBundleVersion(final File storageDir, final BundleVersionCoordinate versionCoordinate,
                                            final String contentString) throws IOException {

        final File bundleVersionDir = FileSystemBundlePersistenceProvider.getBundleVersionDirectory(storageDir, versionCoordinate);
        final File bundleFile = FileSystemBundlePersistenceProvider.getBundleFile(bundleVersionDir, versionCoordinate);
        assertTrue(bundleFile.exists());

        try (InputStream in = new FileInputStream(bundleFile)) {
            assertEquals(contentString, IOUtils.toString(in, StandardCharsets.UTF_8));
        }
    }

}
