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
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Map.entry;
import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static java.util.function.Predicate.not;
import static java.util.regex.Pattern.compile;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NO_OPERATION;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.ResourceType.ASSET;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.nifi.c2.client.service.operation.SyncResourceStrategy;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSyncResourceStrategy implements SyncResourceStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSyncResourceStrategy.class);
    private static final Pattern ALLOWED_RESOURCE_PATH_PATTERN = compile("^(?:(?:.[/\\\\])?[^~<>:\\|\\\"\\?\\*\\./\\\\]+(?:(?!(\\.\\.))[^~<>:\\|\\\"\\?\\*])*)?$");

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

    public DefaultSyncResourceStrategy(ResourceRepository resourceRepository) {
        this.resourceRepository = resourceRepository;
    }

    @Override
    public OperationState synchronizeResourceRepository(ResourcesGlobalHash c2GlobalHash, List<ResourceItem> c2ServerItems,
                                                        BiFunction<String, Function<InputStream, Optional<Path>>, Optional<Path>> resourceDownloadFunction,
                                                        Function<String, String> urlEnrichFunction) {
        Set<ResourceItem> c2Items = Set.copyOf(c2ServerItems);
        Set<ResourceItem> agentItems = Set.copyOf(resourceRepository.findAllResourceItems());

        OperationState deleteResult = deleteItems(c2Items, agentItems);
        OperationState saveResult = saveNewItems(c2Items, agentItems, resourceDownloadFunction, urlEnrichFunction);

        Entry<OperationState, OperationState> resultPair = entry(deleteResult, saveResult);

        return SUCCESS_RESULT_PAIRS.contains(resultPair)
            ? saveGlobalHash(c2GlobalHash, deleteResult, saveResult)
            : FAILED_RESULT_PAIRS.contains(resultPair) ? NOT_APPLIED : PARTIALLY_APPLIED;
    }

    private OperationState saveNewItems(Set<ResourceItem> c2Items, Set<ResourceItem> agentItems,
                                        BiFunction<String, Function<InputStream, Optional<Path>>, Optional<Path>> resourceDownloadFunction,
                                        Function<String, String> urlEnrichFunction) {
        List<ResourceItem> newItems = c2Items.stream().filter(not(agentItems::contains)).toList();
        if (newItems.isEmpty()) {
            return NO_OPERATION;
        }

        List<ResourceItem> addedItems = newItems.stream()
            .filter(this::validate)
            .map(downloadIfNotPresentAndAddToRepository(resourceDownloadFunction, urlEnrichFunction))
            .flatMap(Optional::stream)
            .toList();

        return addedItems.isEmpty()
            ? NOT_APPLIED
            : newItems.size() == addedItems.size() ? FULLY_APPLIED : PARTIALLY_APPLIED;
    }

    private Function<ResourceItem, Optional<ResourceItem>> downloadIfNotPresentAndAddToRepository(
        BiFunction<String, Function<InputStream, Optional<Path>>, Optional<Path>> resourceDownloadFunction, Function<String, String> urlEnrichFunction) {
        return resourceItem -> resourceRepository.resourceItemBinaryPresent(resourceItem)
            ? resourceRepository.addResourceItem(resourceItem)
            : Optional.ofNullable(urlEnrichFunction.apply(resourceItem.getUrl()))
            .flatMap(enrichedUrl -> resourceDownloadFunction.apply(enrichedUrl, this::persistToTemporaryLocation))
            .flatMap(tempResourcePath -> resourceRepository.addResourceItem(resourceItem, tempResourcePath));
    }

    private boolean validate(ResourceItem resourceItem) {
        if (resourceItem.getResourcePath() != null && resourceItem.getResourceType() == ASSET) {
            if (!ALLOWED_RESOURCE_PATH_PATTERN.matcher(resourceItem.getResourcePath()).matches()) {
                LOG.error("Invalid resource path {}", resourceItem.getResourcePath());
                return false;
            }
        }
        return true;
    }

    private Optional<Path> persistToTemporaryLocation(InputStream inputStream) {
        String tempResourceId = randomUUID().toString();
        try {
            Path tempFile = createTempFile(tempResourceId, null);
            copy(inputStream, tempFile, REPLACE_EXISTING);
            return Optional.of(tempFile);
        } catch (IOException e) {
            LOG.error("Unable to download resource. Will retry in next heartbeat iteration", e);
            return empty();
        }
    }

    private OperationState deleteItems(Set<ResourceItem> c2Items, Set<ResourceItem> agentItems) {
        List<ResourceItem> toDeleteItems = agentItems.stream().filter(not(c2Items::contains)).toList();
        if (toDeleteItems.isEmpty()) {
            return NO_OPERATION;
        }

        List<ResourceItem> deletedItems = toDeleteItems.stream()
            .map(resourceRepository::deleteResourceItem)
            .flatMap(Optional::stream)
            .toList();

        return deletedItems.isEmpty()
            ? NOT_APPLIED
            : deletedItems.size() == toDeleteItems.size() ? FULLY_APPLIED : PARTIALLY_APPLIED;
    }

    private OperationState saveGlobalHash(ResourcesGlobalHash resourcesGlobalHash, OperationState deleteResult, OperationState saveResult) {
        boolean isGlobalHashRefreshOnly = deleteResult == NO_OPERATION && saveResult == NO_OPERATION;
        return resourceRepository.saveResourcesGlobalHash(resourcesGlobalHash)
            .map(unused -> FULLY_APPLIED)
            .orElse(isGlobalHashRefreshOnly ? NOT_APPLIED : PARTIALLY_APPLIED);
    }
}
