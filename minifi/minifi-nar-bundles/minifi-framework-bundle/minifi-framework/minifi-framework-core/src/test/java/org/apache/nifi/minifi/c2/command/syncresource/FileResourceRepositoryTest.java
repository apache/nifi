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
import static org.apache.nifi.minifi.c2.command.syncresource.FileResourceRepository.ASSET_REPOSITORY_DIRECTORY;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
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

@ExtendWith(MockitoExtension.class)
public class FileResourceRepositoryTest {

    private static final String RESOURCE_BINARY_CONTENT = "content";

    @TempDir
    private Path testBaseDirectory;

    private C2Serializer c2Serializer;

    private Path assetDirectory;
    private Path assetRepositoryDirectory;
    private Path extensionDirectory;
    private Path repositoryFile;
    private Path configDirectoryPath;

    @BeforeEach
    public void setup() throws IOException {
        c2Serializer = new C2JacksonSerializer();
        configDirectoryPath = testBaseDirectory.resolve("conf");
        createDirectories(configDirectoryPath);
        assetDirectory = testBaseDirectory.resolve("assets");
        assetRepositoryDirectory = assetDirectory.resolve(ASSET_REPOSITORY_DIRECTORY);
        createDirectories(assetRepositoryDirectory);
        extensionDirectory = testBaseDirectory.resolve("extensions");
        createDirectories(extensionDirectory);
        repositoryFile = configDirectoryPath.resolve(RESOURCE_REPOSITORY_FILE_NAME);
    }

    @Test
    public void testRepositoryInitializesWithEmptyContent() throws IOException {
        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertNull(testRepository.findResourcesGlobalHash().getDigest());
        assertNull(testRepository.findResourcesGlobalHash().getHashType());
        assertEquals(List.of(), testRepository.findAllResourceItems());
    }

    @Test
    public void testRepositoryInitializesWithExistingContent() throws IOException {
        ResourcesGlobalHash resourcesGlobalHash = resourcesGlobalHash("digest", "hashType");
        ResourceItem resourceItem = resourceItem("resourceId", null, ASSET);
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(resourcesGlobalHash, List.of(resourceItem));
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = createTestRepository();

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
        ResourcesGlobalHash originalGlobalHash = resourcesGlobalHash("digest1", "hashType1");
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(originalGlobalHash, List.of());
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest1", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType1", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourcesGlobalHash updatedGlobalHash = resourcesGlobalHash("digest2", "hashType2");
        testRepository.saveResourcesGlobalHash(updatedGlobalHash);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest2", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType2", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());
    }

    @Test
    public void testSaveGlobalHashSuccessFailure() throws IOException {
        ResourcesGlobalHash originalGlobalHash = resourcesGlobalHash("digest1", "hashType1");
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(originalGlobalHash, List.of());
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest1", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType1", testRepository.findResourcesGlobalHash().getHashType());
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        Optional<ResourcesGlobalHash> result;
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> writeString(any(), any(), eq(CREATE), eq(TRUNCATE_EXISTING), eq(WRITE), eq(SYNC))).thenThrow(new IOException());

            ResourcesGlobalHash updatedGlobalHash = resourcesGlobalHash("digest2", "hashType2");
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
        String content = "content";
        String digest = sha512Hex(content);
        ResourceItem resourceItem = resourceItem("resource1", null, ASSET, "SHA-512", digest);
        createResourceBinary(resourceItem, content);

        FileResourceRepository testRepository = createTestRepository();

        boolean result = testRepository.resourceItemBinaryPresent(resourceItem);
        assertTrue(result);
    }

    @Test
    public void testResourceItemBinaryPresentHashMismatch() throws IOException {
        String content = "content";
        ResourceItem resourceItem = resourceItem("resource1", null, ASSET, "SHA-512", "not_matching_hash");
        createResourceBinary(resourceItem, content);

        FileResourceRepository testRepository = createTestRepository();
        boolean result = testRepository.resourceItemBinaryPresent(resourceItem);
        assertFalse(result);
    }

    @Test
    public void testResourceItemBinaryPresentUnsupportedHashAlgorithm() throws IOException {
        String content = "content";
        ResourceItem resourceItem = resourceItem("resource1", null, ASSET, "unsupported_algorithm", "some_hash");
        createResourceBinary(resourceItem, content);

        FileResourceRepository testRepository = createTestRepository();
        assertThrowsExactly(RuntimeException.class, () -> testRepository.resourceItemBinaryPresent(resourceItem));
    }

    @Test
    public void testAddResourceItemsWithoutContent() throws IOException {
        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourceItem firstNewItem = resourceItem("resource1", null, ASSET);
        Optional<ResourceItem> firstResult = testRepository.addResourceItem(firstNewItem);

        assertTrue(firstResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resource1", testRepository.findAllResourceItems().getFirst().getResourceId());

        ResourceItem secondNewItem = resourceItem("resource2", null, ASSET);
        Optional<ResourceItem> secondResult = testRepository.addResourceItem(secondNewItem);

        assertTrue(secondResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
    }

    @Test
    public void testAddResourceItemsWithoutContentErrorCase() throws IOException {
        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourceItem firstNewItem = resourceItem("resource1", null, ASSET);
        Optional<ResourceItem> result;
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> writeString(any(), any(), eq(CREATE), eq(TRUNCATE_EXISTING), eq(WRITE), eq(SYNC))).thenThrow(new IOException());

            result = testRepository.addResourceItem(firstNewItem);
        }
        assertTrue(result.isEmpty());
    }

    @Test
    public void testAddResourceItemsWithContent() throws IOException {
        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourceItem firstNewItem = resourceItem("resource1", null, ASSET);
        Path firstItemTempPath = createTempBinary();
        Path firstItemExpectedPath = resourcePath(firstNewItem);
        Optional<ResourceItem> firstResult = testRepository.addResourceItem(firstNewItem, firstItemTempPath);

        assertTrue(firstResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resource1", testRepository.findAllResourceItems().getFirst().getResourceId());
        assertFalse(exists(firstItemTempPath));
        assertTrue(exists(firstItemExpectedPath));

        ResourceItem secondNewItem = resourceItem("resource2", "subdirectory", ASSET);
        Path secondItemTempPath = createTempBinary();
        Path secondItemExpectedPath = resourcePath(secondNewItem);
        Optional<ResourceItem> secondResult = testRepository.addResourceItem(secondNewItem, secondItemTempPath);

        assertTrue(secondResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(secondItemTempPath));
        assertTrue(exists(firstItemExpectedPath));
        assertTrue(exists(secondItemExpectedPath));

        ResourceItem thirdNewItem = resourceItem("resource3", null, EXTENSION);
        Path thirdItemTempPath = createTempBinary();
        Path thirdItemExpectedPath = resourcePath(thirdNewItem);
        Optional<ResourceItem> thirdResult = testRepository.addResourceItem(thirdNewItem, thirdItemTempPath);

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
        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourceItem resourceItem = resourceItem("resource1", null, ASSET);
        Path resourceItemTempPath = Path.of("non_existing_path");
        Path resourceItemExpectedPath = resourcePath(resourceItem);

        Optional<ResourceItem> result = testRepository.addResourceItem(resourceItem, resourceItemTempPath);

        assertTrue(result.isEmpty());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(0, testRepository.findAllResourceItems().size());
        assertFalse(exists(resourceItemTempPath));
        assertFalse(exists(resourceItemExpectedPath));
    }

    @Test
    public void testDeleteResourceItem() throws IOException {
        ResourceItem firstItem = resourceItem("resource1", null, ASSET);
        Path firstItemPath = createResourceBinary(firstItem, RESOURCE_BINARY_CONTENT);
        ResourceItem secondItem = resourceItem("resource2", null, ASSET);
        Path secondItemPath = createResourceBinary(secondItem, RESOURCE_BINARY_CONTENT);
        ResourceItem thirdItem = resourceItem("resource3", null, ASSET);
        Path thirdItemPath = createResourceBinary(thirdItem, RESOURCE_BINARY_CONTENT);
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(new ResourcesGlobalHash(), List.of(firstItem, secondItem, thirdItem));
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = createTestRepository();

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(3, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertTrue(exists(firstItemPath));
        assertTrue(exists(secondItemPath));
        assertTrue(exists(thirdItemPath));

        Optional<ResourceItem> firstResult = testRepository.deleteResourceItem(firstItem);
        assertTrue(firstResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(firstItemPath));
        assertTrue(exists(secondItemPath));
        assertTrue(exists(thirdItemPath));

        Optional<ResourceItem> secondResult = testRepository.deleteResourceItem(thirdItem);
        assertTrue(secondResult.isPresent());
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
        assertFalse(exists(firstItemPath));
        assertTrue(exists(secondItemPath));
        assertFalse(exists(thirdItemPath));

        Optional<ResourceItem> thirdResult = testRepository.deleteResourceItem(secondItem);
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
        FileResourceRepository testRepository = createTestRepository();
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourceItem toDeleteItem = resourceItem("resource1", null, ASSET);
        Optional<ResourceItem> result;
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> writeString(any(), any(), eq(CREATE), eq(TRUNCATE_EXISTING), eq(WRITE), eq(SYNC))).thenThrow(new IOException());

            result = testRepository.deleteResourceItem(toDeleteItem);
        }
        assertTrue(result.isEmpty());
    }

    private ResourceRepositoryDescriptor loadRepository() throws IOException {
        return c2Serializer.deserialize(readString(repositoryFile), ResourceRepositoryDescriptor.class).orElse(null);
    }

    private void saveRepository(ResourceRepositoryDescriptor resourceRepositoryDescriptor) throws IOException {
        writeString(repositoryFile, c2Serializer.serialize(resourceRepositoryDescriptor).orElse(EMPTY), CREATE, TRUNCATE_EXISTING, WRITE, SYNC);
    }

    private FileResourceRepository createTestRepository() {
        return new FileResourceRepository(assetDirectory, extensionDirectory, configDirectoryPath, c2Serializer);
    }

    private ResourcesGlobalHash resourcesGlobalHash(String digest, String hashType) {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        resourcesGlobalHash.setDigest(digest);
        resourcesGlobalHash.setHashType(hashType);
        return resourcesGlobalHash;
    }

    private ResourceItem resourceItem(String id, String path, ResourceType resourceType) {
        return resourceItem(id, path, resourceType, null, null);
    }

    private ResourceItem resourceItem(String id, String path, ResourceType resourceType, String hashType, String digest) {
        ResourceItem resourceItem = new ResourceItem();
        resourceItem.setResourceId(id);
        resourceItem.setResourceName(id);
        resourceItem.setResourcePath(path);
        resourceItem.setResourceType(resourceType);
        resourceItem.setHashType(hashType);
        resourceItem.setDigest(digest);
        return resourceItem;
    }

    private void assertRepositoryInMemoryContentEqualsPersistedContent(FileResourceRepository testRepository) throws IOException {
        assertTrue(exists(repositoryFile));
        ResourceRepositoryDescriptor loadedRepositoryDescriptor = loadRepository();
        assertNotNull(loadedRepositoryDescriptor);
        assertEquals(loadedRepositoryDescriptor.resourcesGlobalHash(), testRepository.findResourcesGlobalHash());
        assertIterableEquals(loadedRepositoryDescriptor.resourceItems(), testRepository.findAllResourceItems());
    }

    private Path resourcePath(ResourceItem item) {
        Path resourcePath = switch (item.getResourceType()) {
            case ASSET -> assetRepositoryDirectory.resolve(isBlank(item.getResourcePath()) ? item.getResourceName() : item.getResourcePath() + "/" + item.getResourceName());
            case EXTENSION -> extensionDirectory.resolve(item.getResourceName());
        };
        return resourcePath.toAbsolutePath();
    }

    private Path createResourceBinary(ResourceItem resourceItem, String content) throws IOException {
        Path resourcePath = resourcePath(resourceItem);
        createFile(resourcePath, content);
        return resourcePath;
    }

    private Path createTempBinary() throws IOException {
        Path resourcePath = testBaseDirectory.resolve(randomUUID().toString());
        createFile(resourcePath, RESOURCE_BINARY_CONTENT);
        return resourcePath;
    }

    private void createFile(Path path, String content) throws IOException {
        createDirectories(path.getParent());
        writeString(path, content);
    }
}
