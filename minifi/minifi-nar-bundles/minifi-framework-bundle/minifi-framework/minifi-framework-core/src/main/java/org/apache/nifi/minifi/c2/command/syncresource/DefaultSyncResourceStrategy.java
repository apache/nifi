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

import static java.nio.file.Files.copy;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Map.entry;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.io.file.PathUtils.createParentDirectories;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.nifi.c2.client.service.operation.SyncResourceStrategy;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSyncResourceStrategy implements SyncResourceStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSyncResourceStrategy.class);

    private static final Set<Entry<OperationState, OperationState>> SUCCESS_RESULT_PAIRS = Set.of(
        entry(NO_OPERATION, NO_OPERATION),
        entry(NO_OPERATION, FULLY_APPLIED),
        entry(FULLY_APPLIED, NO_OPERATION),
        entry(FULLY_APPLIED, FULLY_APPLIED));

    private static final Set<Entry<OperationState, OperationState>> FAILED_RESULT_PAIRS = Set.of(
        entry(NO_OPERATION, NOT_APPLIED),
        entry(NOT_APPLIED, NO_OPERATION),
        entry(NOT_APPLIED, NOT_APPLIED));

    private final ResourceRepository resourceRepository;
    private final Path assetDirectory;
    private final Path extensionDirectory;

    public DefaultSyncResourceStrategy(ResourceRepository resourceRepository, Path assetDirectory, Path extensionDirectory) {
        this.resourceRepository = resourceRepository;
        this.assetDirectory = assetDirectory;
        this.extensionDirectory = extensionDirectory;
    }

    @Override
    public OperationState synchronizeResourceRepository(ResourcesGlobalHash c2GlobalHash, List<ResourceItem> c2ServerItems,
                                                        BiFunction<String, Function<InputStream, Optional<Path>>, Optional<Path>> resourceDownloadFunction,
                                                        Function<String, Optional<String>> urlEnrichFunction) {
        Set<ResourceItem> c2Items = c2ServerItems.stream().collect(toSet());
        Set<ResourceItem> agentItems = resourceRepository.findAllResourceItems().stream().collect(toSet());

        OperationState deleteResult = deleteItems(c2Items, agentItems);
        OperationState saveResult = saveNewItems(c2Items, agentItems, resourceDownloadFunction, urlEnrichFunction);

        Entry<OperationState, OperationState> resultPair = entry(deleteResult, saveResult);

        return SUCCESS_RESULT_PAIRS.contains(resultPair)
            ? saveGlobalHash(c2GlobalHash, deleteResult, saveResult)
            : FAILED_RESULT_PAIRS.contains(resultPair) ? NOT_APPLIED : PARTIALLY_APPLIED;
    }

    private OperationState deleteItems(Set<ResourceItem> c2Items, Set<ResourceItem> agentItems) {
        List<ResourceItem> toDeleteItems = agentItems.stream().filter(not(c2Items::contains)).toList();
        if (toDeleteItems.isEmpty()) {
            return NO_OPERATION;
        }

        List<ResourceItem> deletedItems = toDeleteItems.stream()
            .map(this::deleteItem)
            .flatMap(Optional::stream)
            .toList();

        return deletedItems.isEmpty()
            ? NOT_APPLIED
            : deletedItems.size() == toDeleteItems.size() ? FULLY_APPLIED : PARTIALLY_APPLIED;
    }

    private Optional<ResourceItem> deleteItem(ResourceItem item) {
        try {
            Path resourcePath = resourcePath(item);
            ResourceItem deletedItem = resourceRepository.deleteResourceItem(item);
            deleteSilently(resourcePath, "Unable to delete resource file");
            return Optional.of(deletedItem);
        } catch (Exception e) {
            LOG.error("Unable to delete resources from repository", e);
            return empty();
        }
    }

    private OperationState saveNewItems(Set<ResourceItem> c2Items, Set<ResourceItem> agentItems,
                                        BiFunction<String, Function<InputStream, Optional<Path>>, Optional<Path>> resourceDownloadFunction,
                                        Function<String, Optional<String>> urlEnrichFunction) {
        List<ResourceItem> newItems = c2Items.stream().filter(not(agentItems::contains)).toList();
        if (newItems.isEmpty()) {
            return NO_OPERATION;
        }

        List<ResourceItem> addedItems = newItems.stream()
            .map(newItem -> urlEnrichFunction.apply(newItem.getUrl())
                .flatMap(enrichedUrl -> resourceDownloadFunction.apply(enrichedUrl, this::persistToTemporaryLocation))
                .flatMap(tempResourcePath -> moveToFinalLocation(newItem, tempResourcePath))
                .map(finalPath -> addItemToRepositoryOrCleanupWhenFailure(newItem, finalPath))
            )
            .flatMap(Optional::stream)
            .toList();

        return addedItems.isEmpty()
            ? NOT_APPLIED
            : newItems.size() == addedItems.size() ? FULLY_APPLIED : PARTIALLY_APPLIED;
    }

    private Optional<Path> persistToTemporaryLocation(InputStream inputStream) {
        String tempResourceId = randomUUID().toString();
        try {
            Path tempFile = createTempFile(tempResourceId, null);
            copy(inputStream, tempFile, REPLACE_EXISTING);
            return ofNullable(tempFile);
        } catch (IOException e) {
            LOG.error("Unable to download resource. Will retry in next heartbeat iteration", e);
            return empty();
        }
    }

    private Optional<Path> moveToFinalLocation(ResourceItem resourceItem, Path tempPath) {
        try {
            Path finalPath = resourcePath(resourceItem);

            createParentDirectories(finalPath);
            copy(tempPath, finalPath, REPLACE_EXISTING, COPY_ATTRIBUTES);

            return ofNullable(finalPath);
        } catch (IOException e) {
            LOG.error("Unable to move asset to final location. Syncing this asset will be retried in next heartbeat iteration", e);
            return empty();
        } finally {
            deleteSilently(tempPath, "Unable to cleanup temporary file");
        }
    }

    private ResourceItem addItemToRepositoryOrCleanupWhenFailure(ResourceItem newItem, Path finalPath) {
        try {
            return resourceRepository.addResourceItem(newItem);
        } catch (Exception e) {
            LOG.error("Unable to add resource to repository", e);
            deleteSilently(finalPath, "Unable to cleanup resource file");
            return null;
        }
    }

    private void deleteSilently(Path tempPath, String errorMessage) {
        try {
            deleteIfExists(tempPath);
        } catch (IOException e) {
            LOG.error(errorMessage, e);
        }
    }

    private Path resourcePath(ResourceItem resourceItem) {
        return switch (resourceItem.getResourceType()) {
            case ASSET -> ofNullable(resourceItem.getResourcePath())
                .filter(not(String::isBlank))
                .map(assetDirectory::resolve)
                .orElse(assetDirectory)
                .resolve(resourceItem.getResourceName());
            case EXTENSION -> extensionDirectory.resolve(resourceItem.getResourceName());
        };
    }

    private OperationState saveGlobalHash(ResourcesGlobalHash resourcesGlobalHash, OperationState deleteResult, OperationState saveResult) {
        boolean isGlobalHashRefreshOnly = deleteResult == NO_OPERATION && saveResult == NO_OPERATION;
        try {
            resourceRepository.saveResourcesGlobalHash(resourcesGlobalHash);
            return FULLY_APPLIED;
        } catch (Exception e) {
            LOG.error("Unable to save global hash data", e);
            return isGlobalHashRefreshOnly ? NOT_APPLIED : PARTIALLY_APPLIED;
        }
    }
}
