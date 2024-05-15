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
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.c2.protocol.api.ResourceItem;
import org.apache.nifi.c2.protocol.api.ResourcesGlobalHash;
import org.apache.nifi.c2.serializer.C2Serializer;

public class FileResourceRepository implements ResourceRepository {

    static final String RESOURCE_REPOSITORY_FILE_NAME = "resources.json";

    private static final ResourceRepositoryDescriptor EMTPY_HOLDER = new ResourceRepositoryDescriptor(new ResourcesGlobalHash(), List.of());

    private final Path resourceRepositoryFile;
    private final C2Serializer c2Serializer;

    private ResourceRepositoryDescriptor resourceRepositoryDescriptor;

    public FileResourceRepository(Path configDirectory, C2Serializer c2Serializer) {
        this.resourceRepositoryFile = configDirectory.resolve(RESOURCE_REPOSITORY_FILE_NAME);
        this.c2Serializer = c2Serializer;

        try {
            initialize();
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize resource repository", e);
        }
    }

    @Override
    public synchronized ResourcesGlobalHash findResourcesGlobalHash() {
        return resourceRepositoryDescriptor.resourcesGlobalHash();
    }

    @Override
    public synchronized ResourcesGlobalHash saveResourcesGlobalHash(ResourcesGlobalHash resourcesGlobalHash) throws IOException {
        ResourceRepositoryDescriptor newRepositoryDescriptor = new ResourceRepositoryDescriptor(resourcesGlobalHash, resourceRepositoryDescriptor.resourceItems());
        persist(newRepositoryDescriptor);
        resourceRepositoryDescriptor = newRepositoryDescriptor;
        return resourcesGlobalHash;
    }

    @Override
    public synchronized List<ResourceItem> findAllResourceItems() {
        return resourceRepositoryDescriptor.resourceItems();
    }

    @Override
    public synchronized ResourceItem addResourceItem(ResourceItem resourceItem) throws IOException {
        List<ResourceItem> newItems = new ArrayList<>(resourceRepositoryDescriptor.resourceItems());
        newItems.add(resourceItem);
        ResourceRepositoryDescriptor newRepositoryDescriptor = new ResourceRepositoryDescriptor(resourceRepositoryDescriptor.resourcesGlobalHash(), newItems);
        persist(newRepositoryDescriptor);
        resourceRepositoryDescriptor = newRepositoryDescriptor;
        return resourceItem;
    }

    @Override
    public synchronized ResourceItem deleteResourceItem(ResourceItem resourceItem) throws IOException {
        List<ResourceItem> truncatedItems = resourceRepositoryDescriptor.resourceItems()
            .stream()
            .filter(syncAssetItem -> !syncAssetItem.getResourceId().equals(resourceItem.getResourceId()))
            .collect(toList());
        ResourceRepositoryDescriptor newRepositoryDescriptor = new ResourceRepositoryDescriptor(resourceRepositoryDescriptor.resourcesGlobalHash(), truncatedItems);
        persist(newRepositoryDescriptor);
        resourceRepositoryDescriptor = newRepositoryDescriptor;
        return resourceItem;
    }

    private void initialize() throws IOException {
        if (!exists(resourceRepositoryFile)) {
            persist(EMTPY_HOLDER);
            resourceRepositoryDescriptor = EMTPY_HOLDER;
        } else {
            load();
        }
    }

    private void persist(ResourceRepositoryDescriptor resourceRepositoryDescriptor) throws IOException {
        String serializedDescriptor =
            c2Serializer.serialize(resourceRepositoryDescriptor).orElseThrow(() -> new IllegalStateException("Unable to serialize repository descriptor object"));
        writeString(resourceRepositoryFile, serializedDescriptor, CREATE, TRUNCATE_EXISTING, WRITE, SYNC);
    }

    private void load() throws IOException {
        String rawResourceRepository = readString(resourceRepositoryFile);
        ResourceRepositoryDescriptor deserializedDescriptor = c2Serializer.deserialize(rawResourceRepository, ResourceRepositoryDescriptor.class)
            .orElseThrow(() -> new IllegalStateException("Unable to deserialize repository descriptor object"));
        resourceRepositoryDescriptor = deserializedDescriptor;
    }

    record ResourceRepositoryDescriptor(ResourcesGlobalHash resourcesGlobalHash, List<ResourceItem> resourceItems) {
    }
}
