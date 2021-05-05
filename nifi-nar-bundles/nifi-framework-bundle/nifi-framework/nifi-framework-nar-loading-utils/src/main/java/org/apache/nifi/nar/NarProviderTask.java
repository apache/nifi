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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

final class NarProviderTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NarProviderTask.class);
    private static final String NAR_EXTENSION = "nar";

    // A unique id is necessary for temporary files not to collide with temporary files from other instances.
    private final String id = UUID.randomUUID().toString();

    private final NarProvider narProvider;
    private final ClassLoader narProviderClassLoader;
    private final long pollTimeInMs;
    private final File extensionDirectory;

    private volatile boolean stopped = false;

    NarProviderTask(final NarProvider narProvider, final ClassLoader narProviderClassLoader, final File extensionDirectory, final long pollTimeInMs) {
        this.narProvider = narProvider;
        this.narProviderClassLoader = narProviderClassLoader;
        this.pollTimeInMs = pollTimeInMs;
        this.extensionDirectory = extensionDirectory;
    }

    @Override
    public void run() {
        LOGGER.info("Nar provider task is started");

        while (!stopped) {
            try {
                LOGGER.debug("Task starts fetching NARs from provider");
                final Set<String> loadedNars = getLoadedNars();
                final Collection<String> availableNars;
                try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(narProviderClassLoader)) {
                    availableNars = narProvider.listNars();
                }

                for (final String availableNar : availableNars) {
                    if (!loadedNars.contains(availableNar)) {
                        final long startedAt = System.currentTimeMillis();
                        final InputStream inputStream;

                        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(narProviderClassLoader)) {
                            inputStream = narProvider.fetchNarContents(availableNar);
                        }

                        final File tempFile = new File(extensionDirectory, ".tmp_" + id + ".nar");
                        final File targetFile = new File(extensionDirectory, availableNar);
                        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        tempFile.renameTo(targetFile);

                        LOGGER.info("Downloaded NAR {} in {} ms", availableNar, (System.currentTimeMillis() - startedAt));
                    }
                }

                LOGGER.debug("Task finished fetching NARs from provider");
            } catch (final Throwable e) {
                LOGGER.error("Error during reaching the external source", e);
            }

            try {
                Thread.sleep(pollTimeInMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("NAR autoloader external source task is interrupted");
                stopped = true;
            }
        }
    }

    private Set<String> getLoadedNars() {
        return Arrays.stream(extensionDirectory.listFiles(file -> file.isFile() && file.getName().toLowerCase().endsWith("." + NAR_EXTENSION)))
                .map(file -> file.getName())
                .collect(Collectors.toSet());
    }

    void stop() {
        LOGGER.info("Nar provider task is stopped");
        stopped = true;
    }
}
