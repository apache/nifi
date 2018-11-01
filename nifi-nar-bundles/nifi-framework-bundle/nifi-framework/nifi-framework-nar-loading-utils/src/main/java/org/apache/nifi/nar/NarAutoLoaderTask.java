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
package org.apache.nifi.nar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The runnable task that polls the WatchService for new NAR files found in the auto-load directory.
 *
 * Each new NAR file found will be passed to the NarLoader to be unpacked and loaded into the ExtensionManager.
 *
 */
public class NarAutoLoaderTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoLoaderTask.class);

    private static final long MIN_FILE_AGE = 5000;

    private final Path autoLoadPath;
    private final WatchService watchService;
    private final long pollIntervalMillis;
    private final NarLoader narLoader;
    private final List<File> candidateNars;

    private volatile boolean stopped = false;

    private NarAutoLoaderTask(final Builder builder) {
        this.autoLoadPath = builder.autoLoadPath;
        this.watchService = builder.watchService;
        this.pollIntervalMillis = builder.pollIntervalMillis;
        this.narLoader = builder.narLoader;
        this.candidateNars = new ArrayList<>();
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                WatchKey key;
                try {
                    LOGGER.debug("Polling for new NARs at {}", new Object[]{autoLoadPath});
                    key = watchService.poll(pollIntervalMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException x) {
                    LOGGER.info("WatchService interrupted, returning...");
                    return;
                }

                // Key comes back as null when there are no new create events, but we still want to continue processing
                // so we can consider files added to the candidateNars list in previous iterations

                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        final WatchEvent.Kind<?> kind = event.kind();
                        if (kind == StandardWatchEventKinds.OVERFLOW) {
                            continue;
                        }

                        final WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        final Path filename = ev.context();

                        final Path autoLoadFile = autoLoadPath.resolve(filename);
                        final String autoLoadFilename = autoLoadFile.toFile().getName().toLowerCase();

                        if (!autoLoadFilename.endsWith(".nar")) {
                            LOGGER.info("Skipping non-nar file {}", new Object[]{autoLoadFilename});
                            continue;
                        }

                        if (autoLoadFilename.startsWith(".")) {
                            LOGGER.debug("Skipping partially written file {}", new Object[]{autoLoadFilename});
                            continue;
                        }

                        LOGGER.info("Found {} in auto-load directory", new Object[]{autoLoadFile});
                        candidateNars.add(autoLoadFile.toFile());
                    }

                    final boolean valid = key.reset();
                    if (!valid) {
                        LOGGER.error("NAR auto-load directory is no longer valid");
                        stop();
                    }
                }

                // Make sure that the created file is done being written by checking the last modified date of the file and
                // make sure a certain amount of time has passed indicating it is done being written to

                final List<File> readyNars = new ArrayList<>();
                final Iterator<File> candidateNarIter = candidateNars.iterator();
                while (candidateNarIter.hasNext()) {
                    final File candidateNar = candidateNarIter.next();
                    final long fileAge = System.currentTimeMillis() - candidateNar.lastModified();
                    if (fileAge >= MIN_FILE_AGE) {
                        readyNars.add(candidateNar);
                        candidateNarIter.remove();
                    } else {
                        LOGGER.debug("Candidate NAR {} not ready yet, will check again next time", new Object[]{candidateNar.getName()});
                    }
                }

                if (!readyNars.isEmpty()) {
                    narLoader.load(readyNars);
                }

            } catch (final Exception e) {
                LOGGER.error("Error loading NARs due to: " + e.getMessage(), e);
            }
        }
    }

    public void stop() {
        LOGGER.info("Stopping NAR Auto-loader");
        stopped = true;
    }

    /**
     * Builder for NarAutoLoaderTask.
     */
    public static class Builder {

        private Path autoLoadPath;
        private WatchService watchService;
        private long pollIntervalMillis;
        private NarLoader narLoader;

        public Builder autoLoadPath(final Path autoLoadPath) {
            this.autoLoadPath = autoLoadPath;
            return this;
        }

        public Builder watchService(final WatchService watchService) {
            this.watchService = watchService;
            return this;
        }

        public Builder pollIntervalMillis(final long pollIntervalMillis) {
            this.pollIntervalMillis = pollIntervalMillis;
            return this;
        }

        public Builder narLoader(final NarLoader narLoader) {
            this.narLoader = narLoader;
            return this;
        }

        public NarAutoLoaderTask build() {
            return new NarAutoLoaderTask(this);
        }

    }

}
