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

package org.apache.nifi.minifi.c2.command.syncresource;

import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourceType;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.serializer.C2JacksonSerializer;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.apache.nifi.minifi.c2.command.syncresource.FileResourceRepository.ResourceRepositoryDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.readString;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.UUID.randomUUID;
import static org.apache.commons.codec.digest.DigestUtils.sha512Hex;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.nifi.c2.protocol.api.ResourceType.ASSET;
import static org.apache.nifi.c2.protocol.api.ResourceType.EXTENSION;
import static org.apache.nifi.minifi.c2.command.syncresource.FileResourceRepository.RESOURCE_REPOSITORY_FILE_NAME;
import static org.apache.nifi.util.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
public class FileResourceRepositoryTest {

    private static final String RESOURCE_BINARY_CONTENT = "content";

    @TempDir
    private Path testBaseDirectory;

    private C2Serializer c2Serializer;

    private Path assetRepositoryDirectory;
    private Path extensionDirectory;
    private Path repositoryFile;
    private Path configDirectoryPath;

    @BeforeEach
    public void setup() throws IOException {
        c2Serializer = new C2JacksonSerializer();
        configDirectoryPath = testBaseDirectory.resolve("conf");
        createDirectories(configDirectoryPath);
        assetRepositoryDirectory = testBaseDirectory.resolve("assets").resolve("repository");
        createDirectories(assetRepositoryDirectory);
        extensionDirectory = testBaseDirectory.resolve("extensions");
        createDirectories(extensionDirectory);
        repositoryFile = configDirectoryPath.resolve(RESOURCE_REPOSITORY_FILE_NAME);
    }

    @Test
    public void testRepositoryInitializesWithEmptyContent() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertNull(testRepository.findResourcesGlobalHash().getDigest());
        assertNull(testRepository.findResourcesGlobalHash().getHashType());
        assertEquals(List.of(), testRepository.findAllResourceItems());
    }

    @Test
    public void testRepositoryInitializesWithExistingContent() throws IOException {
        final ResourcesGlobalHash resourcesGlobalHash = resourcesGlobalHash("digest", "hashType");
        final ResourceItem resourceItem = resourceItem("resourceId", null, ASSET);
        final ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(resourcesGlobalHash, List.of(resourceItem));
        saveRepository(initialRepositoryDescriptor);

        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType", testRepository.findResourcesGlobalHash().getHashType());
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resourceId", testRepository.findAllResourceItems().getFirst().getResourceId());
    }

    @Test
    public void testRepositoryInitializationFailure() {
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> createDirectories(any())).thenThrow(new IOException());
            assertThrowsExactly(RuntimeException.class, this::createTestRepository);
        }
    }

    @Test
    public void testSaveGlobalHashSuccess() throws IOException {
        final ResourcesGlobalHash originalGlobalHash = resourcesGlobalHash("digest1", "hashType1");
        final ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(originalGlobalHash, List.of());
        saveRepository(initialRepositoryDescriptor);

        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest1", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType1", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final ResourcesGlobalHash updatedGlobalHash = resourcesGlobalHash("digest2", "hashType2");
        testRepository.saveResourcesGlobalHash(updatedGlobalHash);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest2", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType2", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());
    }

    @Test
    public void testSaveGlobalHashSuccessFailure() throws IOException {
        final ResourcesGlobalHash originalGlobalHash = resourcesGlobalHash("digest1", "hashType1");
        final ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(originalGlobalHash, List.of());
        saveRepository(initialRepositoryDescriptor);

        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest1", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType1", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final Optional<ResourcesGlobalHash> result;
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> writeString(any(), any(), eq(CREATE), eq(TRUNCATE_EXISTING), eq(WRITE), eq(SYNC))).thenThrow(new IOException());

            final ResourcesGlobalHash updatedGlobalHash = resourcesGlobalHash("digest2", "hashType2");
            result = testRepository.saveResourcesGlobalHash(updatedGlobalHash);

        }
        assertTrue(result.isEmpty());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest1", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType1", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());
    }

    @Test
    public void testResourceItemBinaryPresent() throws IOException {
        final String content = "content";
        final String digest = sha512Hex(content);
        final ResourceItem resourceItem = resourceItem("resource1", null, ASSET, "SHA-512", digest);
        createResourceBinary(resourceItem, content);

        final FileResourceRepository testRepository = createTestRepository();

        final boolean result = testRepository.resourceItemBinaryPresent(resourceItem);
        assertTrue(result);
    }

    @Test
    public void testResourceItemBinaryPresentHashMismatch() throws IOException {
        final String content = "content";
        final ResourceItem resourceItem = resourceItem("resource1", null, ASSET, "SHA-512", "not_matching_hash");
        createResourceBinary(resourceItem, content);

        final FileResourceRepository testRepository = createTestRepository();
        final boolean result = testRepository.resourceItemBinaryPresent(resourceItem);
        assertFalse(result);
    }

    @Test
    public void testResourceItemBinaryPresentUnsupportedHashAlgorithm() throws IOException {
        final String content = "content";
        final ResourceItem resourceItem = resourceItem("resource1", null, ASSET, "unsupported_algorithm", "some_hash");
        createResourceBinary(resourceItem, content);

        final FileResourceRepository testRepository = createTestRepository();
        assertThrowsExactly(RuntimeException.class, () -> testRepository.resourceItemBinaryPresent(resourceItem));
    }

    @Test
    public void testAddResourceItemsWithoutContent() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final ResourceItem firstNewItem = resourceItem("resource1", null, ASSET);
        final Optional<ResourceItem> firstResult = testRepository.addResourceItem(firstNewItem);

        assertTrue(firstResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resource1", testRepository.findAllResourceItems().getFirst().getResourceId());

        final ResourceItem secondNewItem = resourceItem("resource2", null, ASSET);
        final Optional<ResourceItem> secondResult = testRepository.addResourceItem(secondNewItem);

        assertTrue(secondResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
    }

    @Test
    public void testAddResourceItemsWithoutContentErrorCase() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final ResourceItem firstNewItem = resourceItem("resource1", null, ASSET);
        final Optional<ResourceItem> result;
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> writeString(any(), any(), eq(CREATE), eq(TRUNCATE_EXISTING), eq(WRITE), eq(SYNC))).thenThrow(new IOException());

            result = testRepository.addResourceItem(firstNewItem);
        }
        assertTrue(result.isEmpty());
    }

    @Test
    public void testAddResourceItemsWithContent() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final ResourceItem firstNewItem = resourceItem("resource1", null, ASSET);
        final Path firstItemTempPath = createTempBinary();
        final Path firstItemExpectedPath = resourcePath(firstNewItem);
        final Optional<ResourceItem> firstResult = testRepository.addResourceItem(firstNewItem, firstItemTempPath);

        assertTrue(firstResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resource1", testRepository.findAllResourceItems().getFirst().getResourceId());
        assertFalse(exists(firstItemTempPath));
        assertTrue(exists(firstItemExpectedPath));

        final ResourceItem secondNewItem = resourceItem("resource2", "subdirectory", ASSET);
        final Path secondItemTempPath = createTempBinary();
        final Path secondItemExpectedPath = resourcePath(secondNewItem);
        final Optional<ResourceItem> secondResult = testRepository.addResourceItem(secondNewItem, secondItemTempPath);

        assertTrue(secondResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(secondItemTempPath));
        assertTrue(exists(firstItemExpectedPath));
        assertTrue(exists(secondItemExpectedPath));

        final ResourceItem thirdNewItem = resourceItem("resource3", null, EXTENSION);
        final Path thirdItemTempPath = createTempBinary();
        final Path thirdItemExpectedPath = resourcePath(thirdNewItem);
        final Optional<ResourceItem> thirdResult = testRepository.addResourceItem(thirdNewItem, thirdItemTempPath);

        assertTrue(thirdResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(3, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(thirdItemTempPath));
        assertTrue(exists(firstItemExpectedPath));
        assertTrue(exists(secondItemExpectedPath));
        assertTrue(exists(thirdItemExpectedPath));
    }

    @Test
    public void testAddResourceItemsWithContentErrorWhenCopying() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final ResourceItem resourceItem = resourceItem("resource1", null, ASSET);
        final Path resourceItemTempPath = Path.of("non_existing_path");
        final Path resourceItemExpectedPath = resourcePath(resourceItem);

        final Optional<ResourceItem> result = testRepository.addResourceItem(resourceItem, resourceItemTempPath);

        assertTrue(result.isEmpty());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(0, testRepository.findAllResourceItems().size());
        assertFalse(exists(resourceItemTempPath));
        assertFalse(exists(resourceItemExpectedPath));
    }

    @Test
    public void testDeleteResourceItem() throws IOException {
        final ResourceItem firstItem = resourceItem("resource1", null, ASSET);
        final Path firstItemPath = createResourceBinary(firstItem, RESOURCE_BINARY_CONTENT);
        final ResourceItem secondItem = resourceItem("resource2", null, ASSET);
        final Path secondItemPath = createResourceBinary(secondItem, RESOURCE_BINARY_CONTENT);
        final ResourceItem thirdItem = resourceItem("resource3", null, ASSET);
        final Path thirdItemPath = createResourceBinary(thirdItem, RESOURCE_BINARY_CONTENT);
        final ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(new ResourcesGlobalHash(), List.of(firstItem, secondItem, thirdItem));
        saveRepository(initialRepositoryDescriptor);

        final FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(3, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertTrue(exists(firstItemPath));
        assertTrue(exists(secondItemPath));
        assertTrue(exists(thirdItemPath));

        final Optional<ResourceItem> firstResult = testRepository.deleteResourceItem(firstItem);
        assertTrue(firstResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(firstItemPath));
        assertTrue(exists(secondItemPath));
        assertTrue(exists(thirdItemPath));

        final Optional<ResourceItem> secondResult = testRepository.deleteResourceItem(thirdItem);
        assertTrue(secondResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(firstItemPath));
        assertTrue(exists(secondItemPath));
        assertFalse(exists(thirdItemPath));

        final Optional<ResourceItem> thirdResult = testRepository.deleteResourceItem(secondItem);
        assertTrue(thirdResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(0, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of(), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(firstItemPath));
        assertFalse(exists(secondItemPath));
        assertFalse(exists(thirdItemPath));
    }

    @Test
    public void testDeleteResourceItemErrorCase() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        final ResourceItem toDeleteItem = resourceItem("resource1", null, ASSET);
        final Optional<ResourceItem> result;
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> writeString(any(), any(), eq(CREATE), eq(TRUNCATE_EXISTING), eq(WRITE), eq(SYNC))).thenThrow(new IOException());

            result = testRepository.deleteResourceItem(toDeleteItem);
        }
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetRelativePathReturnsEmptyInCaseOfResourceNotAvailable() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();

        final Optional<Path> relativePath = testRepository.getAbsolutePath("non_existing_resource_id");

        assertTrue(relativePath.isEmpty());
    }

    @Test
    public void testGetRelativePath() throws IOException {
        final FileResourceRepository testRepository = createTestRepository();
        final ResourceItem resourceItem = resourceItem("resource1", "subfolder", ASSET);
        final Path resourceItemPath = createResourceBinary(resourceItem, RESOURCE_BINARY_CONTENT);
        testRepository.addResourceItem(resourceItem);

        final Optional<Path> relativePath = testRepository.getAbsolutePath("resource1");

        assertTrue(relativePath.isPresent());
        assertEquals(resourceItemPath.toString(), relativePath.get().toString());
    }

    private ResourceRepositoryDescriptor loadRepository() throws IOException {
        return c2Serializer.deserialize(readString(repositoryFile), ResourceRepositoryDescriptor.class).orElse(null);
    }

    private void saveRepository(final ResourceRepositoryDescriptor resourceRepositoryDescriptor) throws IOException {
        writeString(repositoryFile, c2Serializer.serialize(resourceRepositoryDescriptor).orElse(EMPTY), CREATE, TRUNCATE_EXISTING, WRITE, SYNC);
    }

    private FileResourceRepository createTestRepository() {
        return new FileResourceRepository(assetRepositoryDirectory, extensionDirectory, configDirectoryPath, c2Serializer);
    }

    private ResourcesGlobalHash resourcesGlobalHash(final String digest, final String hashType) {
        final ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        resourcesGlobalHash.setDigest(digest);
        resourcesGlobalHash.setHashType(hashType);
        return resourcesGlobalHash;
    }

    private ResourceItem resourceItem(final String id, final String path, final ResourceType resourceType) {
        return resourceItem(id, path, resourceType, null, null);
    }

    private ResourceItem resourceItem(final String id, final String path, final ResourceType resourceType, final String hashType, final String digest) {
        final ResourceItem resourceItem = new ResourceItem();
        resourceItem.setResourceId(id);
        resourceItem.setResourceName(id);
        resourceItem.setResourcePath(path);
        resourceItem.setResourceType(resourceType);
        resourceItem.setHashType(hashType);
        resourceItem.setDigest(digest);
        return resourceItem;
    }

    private void assertRepositoryInMemoryContentEqualsPersistedContent(final FileResourceRepository testRepository) throws IOException {
        assertTrue(exists(repositoryFile));
        final ResourceRepositoryDescriptor loadedRepositoryDescriptor = loadRepository();
        assertNotNull(loadedRepositoryDescriptor);
        assertEquals(loadedRepositoryDescriptor.resourcesGlobalHash(), testRepository.findResourcesGlobalHash());
        assertIterableEquals(loadedRepositoryDescriptor.resourceItems(), testRepository.findAllResourceItems());
    }

    private Path resourcePath(final ResourceItem item) {
        final Path resourcePath = switch (item.getResourceType()) {
            case ASSET -> assetRepositoryDirectory.resolve(isBlank(item.getResourcePath()) ? item.getResourceName() : item.getResourcePath() + "/" + item.getResourceName());
            case EXTENSION -> extensionDirectory.resolve(item.getResourceName());
        };
        return resourcePath.toAbsolutePath();
    }

    private Path createResourceBinary(final ResourceItem resourceItem, final String content) throws IOException {
        final Path resourcePath = resourcePath(resourceItem);
        createFile(resourcePath, content);
        return resourcePath;
    }

    private Path createTempBinary() throws IOException {
        final Path resourcePath = testBaseDirectory.resolve(randomUUID().toString());
        createFile(resourcePath, RESOURCE_BINARY_CONTENT);
        return resourcePath;
    }

    private void createFile(final Path path, final String content) throws IOException {
        createDirectories(path.getParent());
        writeString(path, content);
    }
}
