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

import static java.nio.file.Files.exists;
import static java.nio.file.Files.readString;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.nifi.minifi.c2.command.syncresource.FileResourceRepository.RESOURCE_REPOSITORY_FILE_NAME;
import static org.apache.nifi.util.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.serializer.C2JacksonSerializer;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.apache.nifi.minifi.c2.command.syncresource.FileResourceRepository.ResourceRepositoryDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FileResourceRepositoryTest {

    @TempDir
    private File configDirectory;

    private Path repositoryFile;
    private Path configDirectoryPath;
    private C2Serializer c2Serializer;

    @BeforeEach
    public void setup() {
        c2Serializer = new C2JacksonSerializer();
        configDirectoryPath = configDirectory.toPath();
        repositoryFile = configDirectoryPath.resolve(RESOURCE_REPOSITORY_FILE_NAME);
    }

    @Test
    public void testRepositoryInitializesWithEmptyContent() throws IOException {
        FileResourceRepository testRepository = new FileResourceRepository(configDirectoryPath, c2Serializer);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertNull(testRepository.findResourcesGlobalHash().getDigest());
        assertNull(testRepository.findResourcesGlobalHash().getHashType());
        assertEquals(List.of(), testRepository.findAllResourceItems());
    }

    @Test
    public void testRepositoryInitializesWithExistingContent() throws IOException {
        ResourcesGlobalHash resourcesGlobalHash = resourcesGlobalHash("digest", "hashType");
        ResourceItem resourceItem = resourceItem("resourceId");
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(resourcesGlobalHash, List.of(resourceItem));
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = new FileResourceRepository(configDirectoryPath, c2Serializer);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals("digest", testRepository.findResourcesGlobalHash().getDigest());
        assertEquals("hashType", testRepository.findResourcesGlobalHash().getHashType());
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resourceId", testRepository.findAllResourceItems().get(0).getResourceId());
    }

    @Test
    public void testSaveGlobalHash() throws IOException {
        ResourcesGlobalHash originalGlobalHash = resourcesGlobalHash("digest1", "hashType1");
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(originalGlobalHash, List.of());
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = new FileResourceRepository(configDirectoryPath, c2Serializer);

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
    public void testAddResourceItem() throws IOException {
        FileResourceRepository testRepository = new FileResourceRepository(configDirectoryPath, c2Serializer);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertTrue(testRepository.findAllResourceItems().isEmpty());

        ResourceItem firstNewItem = resourceItem("resource1");
        testRepository.addResourceItem(firstNewItem);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertEquals("resource1", testRepository.findAllResourceItems().get(0).getResourceId());

        ResourceItem secondNewItem = resourceItem("resource2");
        testRepository.addResourceItem(secondNewItem);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
    }

    @Test
    public void testDeleteResourceItem() throws IOException {
        ResourceItem firstItem = resourceItem("resource1");
        ResourceItem secondItem = resourceItem("resource2");
        ResourceItem thirdItem = resourceItem("resource3");
        ResourceRepositoryDescriptor initialRepositoryDescriptor = new ResourceRepositoryDescriptor(new ResourcesGlobalHash(), List.of(firstItem, secondItem, thirdItem));
        saveRepository(initialRepositoryDescriptor);

        FileResourceRepository testRepository = new FileResourceRepository(configDirectoryPath, c2Serializer);

        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(3, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource1", "resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());

        testRepository.deleteResourceItem(firstItem);
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(2, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource2", "resource3"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());

        testRepository.deleteResourceItem(thirdItem);
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(1, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of("resource2"), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());

        testRepository.deleteResourceItem(secondItem);
        assertRepositoryInMemoryContentEqualsPersistedContent(testRepository);
        assertEquals(0, testRepository.findAllResourceItems().size());
        assertIterableEquals(List.of(), testRepository.findAllResourceItems().stream().map(ResourceItem::getResourceId).toList());
    }

    private ResourceRepositoryDescriptor loadRepository() throws IOException {
        return c2Serializer.deserialize(readString(repositoryFile), ResourceRepositoryDescriptor.class).orElse(null);
    }

    private void saveRepository(ResourceRepositoryDescriptor resourceRepositoryDescriptor) throws IOException {
        writeString(repositoryFile, c2Serializer.serialize(resourceRepositoryDescriptor).orElse(EMPTY), CREATE, TRUNCATE_EXISTING, WRITE, SYNC);
    }

    private ResourcesGlobalHash resourcesGlobalHash(String digest, String hashType) {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        resourcesGlobalHash.setDigest(digest);
        resourcesGlobalHash.setHashType(hashType);
        return resourcesGlobalHash;
    }

    private ResourceItem resourceItem(String resource) {
        ResourceItem resourceItem = new ResourceItem();
        resourceItem.setResourceId(resource);
        return resourceItem;
    }

    private void assertRepositoryInMemoryContentEqualsPersistedContent(FileResourceRepository testRepository) throws IOException {
        assertTrue(exists(repositoryFile));
        ResourceRepositoryDescriptor loadedRepositoryDescriptor = loadRepository();
        assertNotNull(loadedRepositoryDescriptor);
        assertEquals(loadedRepositoryDescriptor.resourcesGlobalHash(), testRepository.findResourcesGlobalHash());
        assertIterableEquals(loadedRepositoryDescriptor.resourceItems(), testRepository.findAllResourceItems());
    }
}
