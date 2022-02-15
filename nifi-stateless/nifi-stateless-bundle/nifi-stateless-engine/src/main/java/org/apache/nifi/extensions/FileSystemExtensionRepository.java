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

package org.apache.nifi.extensions;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarLoadResult;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.stateless.engine.NarUnpackLock;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class FileSystemExtensionRepository implements ExtensionRepository {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemExtensionRepository.class);

    private final ExtensionDiscoveringManager extensionManager;
    private final NarClassLoaders narClassLoaders;
    private final File narDirectory;
    private final File writableExtensionDirectory;
    private final Set<File> readOnlyExtensionDirectories;
    private final File workingDirectory;
    private final List<ExtensionClient> clients;


    public FileSystemExtensionRepository(final ExtensionDiscoveringManager extensionManager, final StatelessEngineConfiguration engineConfiguration, final NarClassLoaders narClassLoaders,
                                         final List<ExtensionClient> clients) {
        this.extensionManager = extensionManager;
        this.narDirectory = engineConfiguration.getNarDirectory();
        this.writableExtensionDirectory = engineConfiguration.getExtensionsDirectory();
        this.readOnlyExtensionDirectories = engineConfiguration.getReadOnlyExtensionsDirectories() == null
                ? Collections.emptySet()
                : new HashSet<>(engineConfiguration.getReadOnlyExtensionsDirectories());
        this.workingDirectory = engineConfiguration.getWorkingDirectory();
        this.narClassLoaders = narClassLoaders;
        this.clients = clients;
    }

    @Override
    public void initialize() throws IOException {
        final Set<File> narFiles = new HashSet<>();

        // if nar.directory and extensions.directory are the same, StatelessBootstrap has already loaded the nars
        if (writableExtensionDirectory != null && !writableExtensionDirectory.equals(narDirectory)) {
            narFiles.addAll(listNarFiles(writableExtensionDirectory));
        }

        for (final File extensionDir : readOnlyExtensionDirectories) {
            narFiles.addAll(listNarFiles(extensionDir));
        }

        loadExtensions(narFiles);
    }

    private Collection<File> listNarFiles(File extensionDir) {
        final File[] narFiles = extensionDir.listFiles(file -> file.getName().endsWith(".nar"));
        if (narFiles == null) {
            logger.warn("Failed to perform listing of extensions directory {}. Will not preload extensions from this directory.", extensionDir.getAbsolutePath());
            return Collections.emptyList();
        }
        return Arrays.asList(narFiles);
    }

    @Override
    public BundleAvailability getBundleAvailability(final BundleCoordinate bundleCoordinate) {
        final Bundle bundle = extensionManager.getBundle(bundleCoordinate);
        if (bundle == null) {
            return BundleAvailability.BUNDLE_NOT_AVAILABLE;
        }

        final BundleDetails details = bundle.getBundleDetails();
        final BundleCoordinate parentCoordinates = details.getDependencyCoordinate();
        final BundleAvailability parentAvailability = getBundleAvailability(parentCoordinates);

        switch (parentAvailability) {
            case BUNDLE_AVAILABLE:
                return BundleAvailability.BUNDLE_AVAILABLE;
            case BUNDLE_NOT_AVAILABLE:
            case PARENT_NOT_AVAILABLE:
                return BundleAvailability.PARENT_NOT_AVAILABLE;
            default:
                return BundleAvailability.BUNDLE_NOT_AVAILABLE;
        }
    }

    @Override
    public Future<Set<Bundle>> fetch(final Set<BundleCoordinate> bundleCoordinates, final ExecutorService executorService, final int concurrentDownloads) {
        if (clients.isEmpty()) {
            logger.info("Requested {} bundles for download but not configured with any Extension Clients so will not download any", bundleCoordinates.size());
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        final DownloadQueue downloadQueue = new DownloadQueue(extensionManager, executorService, concurrentDownloads, bundleCoordinates, writableExtensionDirectory, clients);
        final CompletableFuture<Void> downloadFuture = downloadQueue.download();
        logger.info("Beginning download of extensions {}", bundleCoordinates);

        // When the download completes, load the extensions & return that future.
        final CompletableFuture<Set<Bundle>> loadFuture = downloadFuture.thenApply(voidDownloadResult -> loadExtensions(downloadQueue));

        return loadFuture;
    }

    private Set<Bundle> loadExtensions(final DownloadQueue downloadQueue) {
        final Set<File> downloadedFiles = downloadQueue.getDownloadedFiles();
        logger.info("Completed download of {} bundles. Unpacking NAR files now", downloadedFiles.size());

        try {
            return loadExtensions(downloadedFiles);
        } catch (final Exception e) {
            throw new RuntimeException("Could not load extensions", e);
        }
    }

    private Set<Bundle> loadExtensions(final Set<File> downloadedFiles) throws IOException {
        final List<File> unpackedDirs = new ArrayList<>();

        final long start = System.currentTimeMillis();
        for (final File downloadedFile : downloadedFiles) {
            // Use a statically defined Lock to prevent multiple threads from unpacking their downloaded nars at the same time,
            // even if they use a different ExtensionRepository.
            NarUnpackLock.lock();
            try {
                logger.info("Unpacking {}", downloadedFile);
                final File extensionsWorkingDirectory = new File(workingDirectory, "extensions");
                final File unpackedDir = NarUnpacker.unpackNar(downloadedFile, extensionsWorkingDirectory, false);
                unpackedDirs.add(unpackedDir);
            } finally {
                NarUnpackLock.unlock();
            }
        }

        final long unpackMillis = System.currentTimeMillis() - start;
        logger.info("Unpacked {} bundles in {} millis. Loading Extensions now", downloadedFiles.size(), unpackMillis);

        final NarLoadResult narLoadResult = narClassLoaders.loadAdditionalNars(unpackedDirs);
        final Set<BundleDetails> bundleDetails = narLoadResult.getSkippedBundles();
        if (!bundleDetails.isEmpty()) {
            throw new IOException(String.format("After loading downloaded bundles, %s bundles were skipped: %s", bundleDetails.size(), bundleDetails));
        }

        final Set<Bundle> loadedBundles = narLoadResult.getLoadedBundles();
        extensionManager.discoverExtensions(loadedBundles);
        return loadedBundles;
    }
}
