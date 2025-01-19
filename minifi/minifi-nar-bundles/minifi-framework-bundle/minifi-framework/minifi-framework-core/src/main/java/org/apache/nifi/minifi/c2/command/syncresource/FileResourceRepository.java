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
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.readString;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.apache.commons.codec.digest.DigestUtils.sha1Hex;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.apache.commons.codec.digest.DigestUtils.sha512Hex;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_256;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_512;
import static org.apache.commons.io.file.PathUtils.createParentDirectories;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileResourceRepository implements ResourceRepository {

    static final String ASSET_REPOSITORY_DIRECTORY = "repository";
    static final String RESOURCE_REPOSITORY_FILE_NAME = "resources.json";

    private static final Logger LOG = LoggerFactory.getLogger(FileResourceRepository.class);
    private static final ResourceRepositoryDescriptor EMTPY_HOLDER = new ResourceRepositoryDescriptor(new ResourcesGlobalHash(), List.of());

    private final Path assetRepositoryDirectory;
    private final Path extensionDirectory;
    private final Path resourceRepositoryFile;
    private final C2Serializer c2Serializer;

    private ResourceRepositoryDescriptor resourceRepositoryDescriptor;

    public FileResourceRepository(Path assetDirectory, Path extensionDirectory, Path configDirectory, C2Serializer c2Serializer) {
        this.resourceRepositoryFile = configDirectory.resolve(RESOURCE_REPOSITORY_FILE_NAME);
        this.c2Serializer = c2Serializer;
        this.assetRepositoryDirectory = assetDirectory.resolve(ASSET_REPOSITORY_DIRECTORY);
        this.extensionDirectory = extensionDirectory;
        initialize();
    }

    @Override
    public synchronized ResourcesGlobalHash findResourcesGlobalHash() {
        return resourceRepositoryDescriptor.resourcesGlobalHash();
    }

    @Override
    public synchronized Optional<ResourcesGlobalHash> saveResourcesGlobalHash(ResourcesGlobalHash resourcesGlobalHash) {
        ResourceRepositoryDescriptor newRepositoryDescriptor = new ResourceRepositoryDescriptor(resourcesGlobalHash, resourceRepositoryDescriptor.resourceItems());
        try {
            persist(newRepositoryDescriptor);
            return Optional.of(resourcesGlobalHash);
        } catch (IOException e) {
            LOG.error("Unable to save global resource hash data", e);
            return empty();
        }
    }

    @Override
    public synchronized List<ResourceItem> findAllResourceItems() {
        return resourceRepositoryDescriptor.resourceItems();
    }

    @Override
    public synchronized boolean resourceItemBinaryPresent(ResourceItem resourceItem) {
        Path path = resourcePath(resourceItem);
        return exists(path) && calculateDigest(path, resourceItem.getHashType()).equals(resourceItem.getDigest());
    }

    @Override
    public synchronized Optional<ResourceItem> addResourceItem(ResourceItem resourceItem) {
        try {
            List<ResourceItem> newItems = new ArrayList<>(resourceRepositoryDescriptor.resourceItems());
            newItems.add(resourceItem);
            ResourceRepositoryDescriptor newRepositoryDescriptor = new ResourceRepositoryDescriptor(resourceRepositoryDescriptor.resourcesGlobalHash(), newItems);
            persist(newRepositoryDescriptor);
            return Optional.of(resourceItem);
        } catch (IOException e) {
            LOG.error("Unable to persist repository metadata", e);
            return empty();
        }
    }

    @Override
    public synchronized Optional<ResourceItem> addResourceItem(ResourceItem resourceItem, Path source) {
        Path resourcePath = resourcePath(resourceItem);
        try {
            createParentDirectories(resourcePath);
            copy(source, resourcePath, REPLACE_EXISTING, COPY_ATTRIBUTES);
        } catch (IOException e) {
            LOG.error("Unable to move resource to final location. Syncing this asset will be retried in next heartbeat iteration", e);
            return empty();
        } finally {
            deleteSilently(source, "Unable to clear temporary file");
        }

        Optional<ResourceItem> addedItem = addResourceItem(resourceItem);
        if (addedItem.isEmpty()) {
            deleteSilently(resourcePath, "Unable to cleanup resource file");
        }
        return addedItem;
    }

    @Override
    public synchronized Optional<ResourceItem> deleteResourceItem(ResourceItem resourceItem) {
        List<ResourceItem> truncatedItems = resourceRepositoryDescriptor.resourceItems()
            .stream()
            .filter(syncAssetItem -> !syncAssetItem.getResourceId().equals(resourceItem.getResourceId()))
            .collect(toList());

        ResourceRepositoryDescriptor newRepositoryDescriptor = new ResourceRepositoryDescriptor(resourceRepositoryDescriptor.resourcesGlobalHash(), truncatedItems);
        try {
            persist(newRepositoryDescriptor);
        } catch (Exception e) {
            LOG.error("Unable to persist repository metadata", e);
            return empty();
        }

        Path resourcePath = resourcePath(resourceItem);
        deleteSilently(resourcePath, "Unable to delete resource file");

        return Optional.of(resourceItem);
    }

    private void initialize() {
        try {
            createDirectories(assetRepositoryDirectory);
            createDirectories(extensionDirectory);

            if (!exists(resourceRepositoryFile)) {
                persist(EMTPY_HOLDER);
            } else {
                load();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize resource repository", e);
        }
    }

    private void persist(ResourceRepositoryDescriptor descriptor) throws IOException {
        String serializedDescriptor =
            c2Serializer.serialize(descriptor).orElseThrow(() -> new IllegalStateException("Unable to serialize repository descriptor object"));
        writeString(resourceRepositoryFile, serializedDescriptor, CREATE, TRUNCATE_EXISTING, WRITE, SYNC);
        resourceRepositoryDescriptor = descriptor;
    }

    private void load() throws IOException {
        String rawResourceRepository = readString(resourceRepositoryFile);
        resourceRepositoryDescriptor = c2Serializer.deserialize(rawResourceRepository, ResourceRepositoryDescriptor.class)
            .orElseThrow(() -> new IllegalStateException("Unable to deserialize repository descriptor object"));
    }

    private Path resourcePath(ResourceItem resourceItem) {
        return switch (resourceItem.getResourceType()) {
            case ASSET -> ofNullable(resourceItem.getResourcePath())
                .filter(not(String::isBlank))
                .map(assetRepositoryDirectory::resolve)
                .orElse(assetRepositoryDirectory)
                .resolve(resourceItem.getResourceName());
            case EXTENSION -> extensionDirectory.resolve(resourceItem.getResourceName());
        };
    }

    private void deleteSilently(Path path, String errorMessage) {
        try {
            deleteIfExists(path);
        } catch (IOException e) {
            LOG.error(errorMessage, e);
        }
    }

    private String calculateDigest(Path path, String digestAlgorithm) {
        try (InputStream inputStream = newInputStream(path)) {
            return switch (digestAlgorithm) {
                case MD5 -> md5Hex(inputStream);
                case SHA_1 -> sha1Hex(inputStream);
                case SHA_256 -> sha256Hex(inputStream);
                case SHA_512 -> sha512Hex(inputStream);
                default -> throw new Exception("Unsupported digest algorithm: " + digestAlgorithm);
            };
        } catch (Exception e) {
            throw new RuntimeException("Unable to calculate digest for resource", e);
        }
    }

    record ResourceRepositoryDescriptor(ResourcesGlobalHash resourcesGlobalHash, List<ResourceItem> resourceItems) {
    }
}
