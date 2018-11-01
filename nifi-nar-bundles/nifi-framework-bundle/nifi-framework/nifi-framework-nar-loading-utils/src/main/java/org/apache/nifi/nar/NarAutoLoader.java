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

import org.apache.nifi.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.Objects;

/**
 * Starts a thread to monitor the auto-load directory for new NARs.
 */
public class NarAutoLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoLoader.class);

    private static final long POLL_INTERVAL_MS = 5000;

    private final File autoLoadDir;
    private final NarLoader narLoader;

    private volatile NarAutoLoaderTask narAutoLoaderTask;
    private volatile boolean started = false;

    public NarAutoLoader(final File autoLoadDir, final NarLoader narLoader) {
        this.autoLoadDir = Objects.requireNonNull(autoLoadDir);
        this.narLoader = Objects.requireNonNull(narLoader);
    }

    public synchronized void start() throws IOException {
        if (started) {
            return;
        }

        FileUtils.ensureDirectoryExistAndCanReadAndWrite(autoLoadDir);

        final WatchService watcher = FileSystems.getDefault().newWatchService();

        final Path autoLoadPath = autoLoadDir.toPath();
        autoLoadPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);

        narAutoLoaderTask = new NarAutoLoaderTask.Builder()
                .autoLoadPath(autoLoadPath)
                .watchService(watcher)
                .pollIntervalMillis(POLL_INTERVAL_MS)
                .narLoader(narLoader)
                .build();

        LOGGER.info("Starting NAR Auto-Loader for directory {} ...", new Object[]{autoLoadPath});

        final Thread thread = new Thread(narAutoLoaderTask);
        thread.setName("NAR Auto-Loader");
        thread.setDaemon(true);
        thread.start();

        LOGGER.info("NAR Auto-Loader started");
        started = true;
    }

    public synchronized void stop() {
        started = false;
        narAutoLoaderTask.stop();
        LOGGER.info("NAR Auto-Loader stopped");
    }

}
