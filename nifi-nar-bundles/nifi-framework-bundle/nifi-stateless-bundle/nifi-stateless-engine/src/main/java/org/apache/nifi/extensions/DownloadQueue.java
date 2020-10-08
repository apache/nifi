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
import org.apache.nifi.extensions.exception.BundleNotFoundException;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarManifestEntry;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public class DownloadQueue {
    private static final Logger logger = LoggerFactory.getLogger(DownloadQueue.class);

    private final ExtensionManager extensionManager;
    private final ExecutorService executorService;
    private final int concurrentDownloads;
    private final File narLibDirectory;
    private final List<ExtensionClient> clients;

    private final BlockingQueue<BundleCoordinate> toDownload = new LinkedBlockingQueue<>();
    private final Set<BundleCoordinate> allDownloads = new HashSet<>();

    public DownloadQueue(final ExtensionManager extensionManager, final ExecutorService executorService, final int concurrentDownloads, final Collection<BundleCoordinate> bundles,
                         final File narLibDirectory, final List<ExtensionClient> clients) {
        this.extensionManager = extensionManager;
        this.executorService = executorService;
        this.concurrentDownloads = concurrentDownloads;
        this.narLibDirectory = narLibDirectory;
        this.clients = clients;

        toDownload.addAll(bundles);
        allDownloads.addAll(bundles);
    }

    @SuppressWarnings("rawtypes")
    public CompletableFuture<Void> download() {
        final Set<File> downloaded = Collections.synchronizedSet(new HashSet<>());

        final CompletableFuture[] futures = new CompletableFuture[concurrentDownloads];
        for (int i=0; i < concurrentDownloads; i++) {
            final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            executorService.submit(new DownloadTask(toDownload, completableFuture, downloaded));
            futures[i] = completableFuture;
        }

        return CompletableFuture.allOf(futures);
    }

    public Set<BundleCoordinate> getDownloadedCoordinates() {
        return allDownloads.stream()
            .filter(this::isDownloadable)
            .collect(Collectors.toSet());
    }

    public Set<File> getDownloadedFiles() {
        return allDownloads.stream()
            .filter(this::isDownloadable)
            .map(this::getBundleFile)
            .collect(Collectors.toSet());
    }

    private boolean isDownloadable(final BundleCoordinate coordinate) {
        return !NarClassLoaders.JETTY_NAR_ID.equals(coordinate.getId())
            && !NarClassLoaders.FRAMEWORK_NAR_ID.equals(coordinate.getId());
    }

    private synchronized void queueParents(final BundleCoordinate parentCoordinates) {
        if (parentCoordinates == null) {
            return;
        }

        final Bundle existingBundle = extensionManager.getBundle(parentCoordinates);
        if (existingBundle == null) {
            if (allDownloads.contains(parentCoordinates)) {
                // Already queued for download.
                return;
            }

            // We don't have have the parent yet. Queue it for download.
            logger.debug("Enqueuing parent bundle {} to be downloaded", parentCoordinates);
            allDownloads.add(parentCoordinates);
            toDownload.add(parentCoordinates);
            return;
        }

        // Check/queue anything needed for download, recursively.
        queueParents(existingBundle.getBundleDetails().getDependencyCoordinate());
    }

    private File getBundleFile(final BundleCoordinate coordinate) {
        final String filename = coordinate.getId() + "-" + coordinate.getVersion() + ".nar";
        return new File(narLibDirectory, filename);
    }


    private class DownloadTask implements Runnable {
        private final BlockingQueue<BundleCoordinate> downloadQueue;
        private final CompletableFuture<Void> completableFuture;
        private final Set<File> filesDownloaded;

        public DownloadTask(final BlockingQueue<BundleCoordinate> downloadQueue, final CompletableFuture<Void> completableFuture, final Set<File> filesDownloaded) {
            this.downloadQueue = downloadQueue;
            this.completableFuture = completableFuture;
            this.filesDownloaded = filesDownloaded;
        }

        @Override
        public void run() {
            BundleCoordinate coordinate;
            while ((coordinate = downloadQueue.poll()) != null) {
                try {
                    final File downloaded = download(coordinate);
                    if (downloaded != null) {
                        filesDownloaded.add(downloaded);

                        final BundleCoordinate parentCoordinate = getParentCoordinate(downloaded);
                        queueParents(parentCoordinate);
                    }

                    final Bundle existingBundle = extensionManager.getBundle(coordinate);
                    if (existingBundle != null) {
                        final BundleCoordinate parentCoordinate = existingBundle.getBundleDetails().getDependencyCoordinate();
                        queueParents(parentCoordinate);
                    }
                } catch (final Exception e) {
                    logger.error("Failed to download {}", coordinate, e);
                    completableFuture.completeExceptionally(e);
                }
            }

            completableFuture.complete(null);
        }

        private BundleCoordinate getParentCoordinate(final File narFile) throws IOException {
            try (final JarFile nar = new JarFile(narFile)) {
                final Manifest manifest = nar.getManifest();

                final Attributes attributes = manifest.getMainAttributes();
                final String groupId = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_GROUP.getManifestName());
                final String narId = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_ID.getManifestName());
                final String version = attributes.getValue(NarManifestEntry.NAR_DEPENDENCY_VERSION.getManifestName());

                if (groupId == null || narId == null || version == null) {
                    return null;
                }

                return new BundleCoordinate(groupId, narId, version);
            }
        }

        private File download(final BundleCoordinate coordinate) throws BundleNotFoundException {
            final List<Exception> suppressed = new ArrayList<>();
            final File destinationFile = getBundleFile(coordinate);

            if (NarClassLoaders.JETTY_NAR_ID.equals(coordinate.getId())) {
                logger.debug("Requested to download {} but only a single Jetty NAR is allowed to exist so will not download.", coordinate);
                return null;
            }
            if (NarClassLoaders.FRAMEWORK_NAR_ID.equals(coordinate.getId())) {
                logger.debug("Requested to download {} but only a single NiFi Framework NAR is allowed to exist so will not download.", coordinate);
                return null;
            }

            if (destinationFile.exists()) {
                logger.debug("Requested to download {} but destination file {} already exists. Will not download.", coordinate, destinationFile);
                return null;
            }

            for (final ExtensionClient extensionClient : clients) {
                InputStream extensionStream = null;

                try {
                    extensionStream = extensionClient.getExtension(coordinate);
                    if (extensionStream == null) {
                        continue;
                    }

                    final long start = System.currentTimeMillis();
                    final File tmpFile = new File(destinationFile.getParentFile(), destinationFile.getName() + ".download");
                    try (final OutputStream out = new FileOutputStream(tmpFile)) {
                        StreamUtils.copy(extensionStream, out);
                    }

                    Files.move(tmpFile.toPath(), destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                    final long millis = System.currentTimeMillis() - start;
                    logger.info("Successfully downloaded {} to {} in {} millis", coordinate, destinationFile.getAbsolutePath(), millis);
                    return destinationFile;
                } catch (final Exception e) {
                    logger.error("Failed to fetch extension {} from {}", coordinate, extensionClient, e);
                    suppressed.add(e);
                } finally {
                    closeQuietly(extensionStream);
                }
            }

            final BundleNotFoundException bnfe = new BundleNotFoundException(coordinate, "Could not fetch bundle " + coordinate + " from any client");
            suppressed.forEach(bnfe::addSuppressed);
            throw bnfe;
        }

        private void closeQuietly(final Closeable closeable) {
            if (closeable == null) {
                return;
            }

            try {
                closeable.close();
            } catch (final IOException ioe) {
                logger.warn("Failed to close {}", closeable);
            }
        }
    }
}
