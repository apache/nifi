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

import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Starts a thread to monitor the auto-load directory for new NARs.
 */
public class NarAutoLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoLoader.class);

    private static final long POLL_INTERVAL_MS = 5000;

    private final NiFiProperties properties;
    private final NarLoader narLoader;

    private volatile NarAutoLoaderTask narAutoLoaderTask;
    private volatile boolean started = false;

    public NarAutoLoader(final NiFiProperties properties, final NarLoader narLoader) {
        this.properties = Objects.requireNonNull(properties);
        this.narLoader = Objects.requireNonNull(narLoader);
    }

    public synchronized void start() throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException, TlsException {
        if (started) {
            return;
        }

        final File autoLoadDir = properties.getNarAutoLoadDirectory();
        FileUtils.ensureDirectoryExistAndCanRead(autoLoadDir);

        final List<File> initialNars = Arrays.asList(autoLoadDir.listFiles((dir, name) -> name.endsWith(".nar") && !name.startsWith(".")));
        LOGGER.info("Found {} initial NARs from directory {}", initialNars.size(), autoLoadDir.getName());

        if (!initialNars.isEmpty()) {
            LOGGER.info("Loading initial NARs from directory {}...", autoLoadDir.getName());
            narLoader.load(initialNars);
        }

        final WatchService watcher = FileSystems.getDefault().newWatchService();
        final Path autoLoadPath = autoLoadDir.toPath();
        autoLoadPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);

        narAutoLoaderTask = new NarAutoLoaderTask.Builder()
                .autoLoadPath(autoLoadPath)
                .watchService(watcher)
                .pollIntervalMillis(POLL_INTERVAL_MS)
                .narLoader(narLoader)
                .build();

        LOGGER.info("Starting NAR Auto-Loader Thread for directory {} ...", new Object[]{autoLoadPath});

        final Thread autoLoaderThread = new Thread(narAutoLoaderTask);
        autoLoaderThread.setName("NAR Auto-Loader");
        autoLoaderThread.setDaemon(true);
        autoLoaderThread.start();
    }

    public synchronized void stop() {
        started = false;
        if (narAutoLoaderTask != null) {
            narAutoLoaderTask.stop();
            narAutoLoaderTask = null;
        }

        LOGGER.info("NAR Auto-Loader stopped");
    }
}
