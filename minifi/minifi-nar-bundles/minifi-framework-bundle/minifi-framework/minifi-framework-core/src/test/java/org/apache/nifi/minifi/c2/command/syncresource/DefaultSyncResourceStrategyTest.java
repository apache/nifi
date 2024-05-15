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

import static java.nio.file.Files.createTempFile;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.ResourceType.ASSET;
import static org.apache.nifi.c2.protocol.api.ResourceType.EXTENSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.io.file.PathUtils;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourceType;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultSyncResourceStrategyTest {

    private static final ResourcesGlobalHash C2_GLOBAL_HASH = resourcesGlobalHash("digest1");

    private static final String FAIL_DOWNLOAD_URL = "fail";

    private static final BiFunction<String, Function<InputStream, Optional<Path>>, Optional<Path>> URL_TO_CONTENT_DOWNLOAD_FUNCTION =
        (url, persistFunction) -> url.endsWith(FAIL_DOWNLOAD_URL) ? empty() : persistFunction.apply(new ByteArrayInputStream(url.getBytes()));

    private static String ENRICH_PREFIX = "pre_";
    private static final Function<String, Optional<String>> PREFIXING_ENRICH_FUNCTION = url -> ofNullable(url).map(arg -> ENRICH_PREFIX + arg);

    @TempDir
    private Path assetDirectory;

    @TempDir
    private Path extensionDirectory;

    @Mock
    private ResourceRepository mockResourceRepository;

    private DefaultSyncResourceStrategy testSyncResourceStrategy;

    @BeforeEach
    public void setup() {
        testSyncResourceStrategy = new DefaultSyncResourceStrategy(mockResourceRepository, assetDirectory, extensionDirectory);
    }

    @Test
    public void testAddingNewItems() {
        List<ResourceItem> c2Items = List.of(
            resourceItem("resource1", "url1", null, ASSET),
            resourceItem("resource2", "url2", "", ASSET),
            resourceItem("resource3", "url3", "path3", ASSET),
            resourceItem("resource4", "url4", null, EXTENSION),
            resourceItem("resource5", "url5", "path5", EXTENSION)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());
        c2Items.forEach(resourceItem -> {
            try {
                when(mockResourceRepository.addResourceItem(resourceItem)).thenReturn(resourceItem);
            } catch (Exception e) {
            }
        });

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

        assertEquals(FULLY_APPLIED, resultState);

        List<String> resultAssetPaths = resultFilesOf(assetDirectory);
        List<String> expectedAssetPaths = expectedFilesOf(c2Items, ASSET);
        assertIterableEquals(expectedAssetPaths, resultAssetPaths);

        List<String> resultExtensionPaths = resultFilesOf(extensionDirectory);
        List<String> expectedExtensionsPaths = expectedFilesOf(c2Items, EXTENSION);
        assertEquals(expectedExtensionsPaths, resultExtensionPaths);
        try {
            verify(mockResourceRepository, never()).deleteResourceItem(any());
            verify(mockResourceRepository, times(1)).saveResourcesGlobalHash(C2_GLOBAL_HASH);
        } catch (Exception e) {
        }
    }

    @Test
    public void testAddingNewItemFailureDueToIssueWithUrlEnrichment() {
        List<ResourceItem> c2Items = List.of(
            resourceItem("resource1", null, null, ASSET)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

        assertEquals(NOT_APPLIED, resultState);

        List<String> resultAssetPaths = resultFilesOf(assetDirectory);
        assertTrue(resultAssetPaths.isEmpty());
        try {
            verify(mockResourceRepository, never()).deleteResourceItem(any());
            verify(mockResourceRepository, never()).saveResourcesGlobalHash(C2_GLOBAL_HASH);
        } catch (Exception e) {
        }
    }

    @Test
    public void testAddingNewItemFailureDueToIssueInDownloadFunction() {
        List<ResourceItem> c2Items = List.of(
            resourceItem("resource1", FAIL_DOWNLOAD_URL, null, ASSET)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

        assertEquals(NOT_APPLIED, resultState);

        List<String> resultAssetPaths = resultFilesOf(assetDirectory);
        assertTrue(resultAssetPaths.isEmpty());
        try {
            verify(mockResourceRepository, never()).deleteResourceItem(any());
            verify(mockResourceRepository, never()).saveResourcesGlobalHash(C2_GLOBAL_HASH);
        } catch (Exception e) {
        }
    }

    @Test
    public void testAddingNewItemFailureDueToIssueInPersistFunction() {
        List<ResourceItem> c2Items = List.of(
            resourceItem("resource1", "url1", null, ASSET)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());

        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
            mockedFiles.when(() -> createTempFile(anyString(), eq(null))).thenThrow(IOException.class);

            OperationState resultState =
                testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

            assertEquals(NOT_APPLIED, resultState);

            List<String> resultAssetPaths = resultFilesOf(assetDirectory);
            assertTrue(resultAssetPaths.isEmpty());
            try {
                verify(mockResourceRepository, never()).deleteResourceItem(any());
                verify(mockResourceRepository, never()).saveResourcesGlobalHash(C2_GLOBAL_HASH);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testAddingNewItemFailureDueToIssueWhenMovingFileToFinalLocation() {
        List<ResourceItem> c2Items = List.of(
            resourceItem("resource1", "url1", null, ASSET)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());

        try (MockedStatic<PathUtils> mockedPathUtils = mockStatic(PathUtils.class)) {
            mockedPathUtils.when(() -> PathUtils.createParentDirectories(any(Path.class))).thenThrow(IOException.class);

            OperationState resultState =
                testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

            assertEquals(NOT_APPLIED, resultState);

            List<String> resultAssetPaths = resultFilesOf(assetDirectory);
            assertTrue(resultAssetPaths.isEmpty());
            try {
                verify(mockResourceRepository, never()).deleteResourceItem(any());
                verify(mockResourceRepository, never()).saveResourcesGlobalHash(C2_GLOBAL_HASH);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testAddingNewItemFailureDueToIssueWhenUpdatingRepository() {
        ResourceItem resourceItem = resourceItem("resource1", "url1", null, ASSET);
        List<ResourceItem> c2Items = List.of(resourceItem);
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());
        try {
            when(mockResourceRepository.addResourceItem(resourceItem)).thenThrow(Exception.class);
        } catch (Exception e) {
        }

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);


        assertEquals(NOT_APPLIED, resultState);

        List<String> resultAssetPaths = resultFilesOf(assetDirectory);
        assertTrue(resultAssetPaths.isEmpty());
        try {
            verify(mockResourceRepository, never()).deleteResourceItem(any());
            verify(mockResourceRepository, never()).saveResourcesGlobalHash(C2_GLOBAL_HASH);
        } catch (Exception e) {
        }
    }

    @Test
    public void testDeletingAllItems() {
        List<ResourceItem> c2Items = List.of();
        List<ResourceItem> agentItems = List.of(
            resourceItem("resource1", "url1", null, ASSET),
            resourceItem("resource2", "url2", null, ASSET),
            resourceItem("resource3", "url3", null, EXTENSION)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(agentItems);
        agentItems.forEach(agentItem -> {
            try {
                when(mockResourceRepository.deleteResourceItem(agentItem)).thenReturn(agentItem);
            } catch (Exception e) {
            }
        });

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

        assertEquals(FULLY_APPLIED, resultState);
        try {
            verify(mockResourceRepository, never()).addResourceItem(any());
            verify(mockResourceRepository, times(1)).saveResourcesGlobalHash(C2_GLOBAL_HASH);
        } catch (Exception e) {
        }
    }

    @Test
    public void testDeleteFailureDueToIssueWithUpdatingRepository() {
        List<ResourceItem> c2Items = List.of();
        List<ResourceItem> agentItems = List.of(
            resourceItem("resource1", "url1", null, ASSET)
        );
        when(mockResourceRepository.findAllResourceItems()).thenReturn(agentItems);
        agentItems.forEach(agentItem -> {
            try {
                when(mockResourceRepository.deleteResourceItem(agentItem)).thenThrow(Exception.class);
            } catch (Exception e) {
            }
        });

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, c2Items, URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

        assertEquals(NOT_APPLIED, resultState);
        try {
            verify(mockResourceRepository, never()).addResourceItem(any());
            verify(mockResourceRepository, never()).saveResourcesGlobalHash(C2_GLOBAL_HASH);
        } catch (Exception e) {
        }
    }

    @Test
    public void testAddFileSuccessfulButUpdateGlobalHashFails() {
        ResourceItem c2Item = resourceItem("resource1", "url1", null, ASSET);
        List<ResourceItem> c2Items = List.of(c2Item);
        when(mockResourceRepository.findAllResourceItems()).thenReturn(List.of());
        try {
            when(mockResourceRepository.addResourceItem(c2Item)).thenReturn(c2Item);
            when(mockResourceRepository.saveResourcesGlobalHash(C2_GLOBAL_HASH)).thenThrow(Exception.class);
        } catch (Exception e) {
        }

        OperationState resultState =
            testSyncResourceStrategy.synchronizeResourceRepository(C2_GLOBAL_HASH, List.of(c2Item), URL_TO_CONTENT_DOWNLOAD_FUNCTION, PREFIXING_ENRICH_FUNCTION);

        assertEquals(PARTIALLY_APPLIED, resultState);

        List<String> resultAssetPaths = resultFilesOf(assetDirectory);
        List<String> expectedAssetPaths = expectedFilesOf(c2Items, ASSET);
        assertIterableEquals(expectedAssetPaths, resultAssetPaths);
        try {
            verify(mockResourceRepository, never()).deleteResourceItem(any());
        } catch (Exception e) {
        }
    }


    private static ResourcesGlobalHash resourcesGlobalHash(String digest) {
        ResourcesGlobalHash resourcesGlobalHash = new ResourcesGlobalHash();
        resourcesGlobalHash.setDigest(digest);
        return resourcesGlobalHash;
    }

    private List<String> expectedFilesOf(List<ResourceItem> resourceItems, ResourceType resourceType) {
        return resourceItems.stream()
            .filter(resourceItem -> resourceItem.getResourceType() == resourceType)
            .map(item -> switch (resourceType) {
                case ASSET -> isBlank(item.getResourcePath()) ? item.getResourceName() : item.getResourcePath() + "/" + item.getResourceName();
                case EXTENSION -> item.getResourceName();
            })
            .map(path -> switch (resourceType) {
                case ASSET -> assetDirectory.resolve(path);
                case EXTENSION -> extensionDirectory.resolve(path);
            })
            .map(Path::toAbsolutePath)
            .map(Path::toString)
            .sorted()
            .toList();
    }

    private List<String> resultFilesOf(Path directory) {
        return listFiles(directory.toFile(), null, true).stream()
            .map(File::getAbsolutePath)
            .sorted()
            .toList();
    }

    private ResourceItem resourceItem(String name, String url, String path, ResourceType resourceType) {
        ResourceItem resourceItem = new ResourceItem();
        resourceItem.setResourceName(name);
        resourceItem.setUrl(url);
        resourceItem.setResourceType(resourceType);
        resourceItem.setResourcePath(path);
        return resourceItem;
    }
}
