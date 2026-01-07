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

package org.apache.nifi.minifi.nar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.util.Objects.requireNonNull;

public class NarAutoUnloaderTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoUnloaderTask.class);
    private static final long POLL_INTERVAL_MS = 5000;

    private final Path autoLoadPath;
    private final WatchService watchService;
    private final NarAutoUnloadService narAutoUnloadService;

    private volatile boolean stopped = false;

    public NarAutoUnloaderTask(
            Path autoLoadPath,
            WatchService watchService,
            NarAutoUnloadService narAutoUnloadService) {
        this.autoLoadPath = requireNonNull(autoLoadPath);
        this.watchService = requireNonNull(watchService);
        this.narAutoUnloadService = requireNonNull(narAutoUnloadService);
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                WatchKey key;
                try {
                    LOGGER.debug("Polling for removed NARs at {}", autoLoadPath);
                    key = watchService.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException x) {
                    LOGGER.info("WatchService interrupted, returning...");
                    return;
                }
                if (key != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.kind() == ENTRY_DELETE) {
                            narAutoUnloadService.unloadNarFile(getFileName(event));
                        }
                    }
                    if (!key.reset()) {
                        LOGGER.error("NAR auto-load directory is no longer valid");
                        stop();
                    }
                }
            } catch (final Throwable t) {
                LOGGER.error("Error un-loading NARs", t);
            }
        }
    }

    public void stop() {
        LOGGER.info("Stopping NAR Auto-Unloader");
        stopped = true;
    }

    public Path getAutoLoadPath() {
        return autoLoadPath;
    }

    private String getFileName(WatchEvent<?> event) {
        WatchEvent<Path> ev = (WatchEvent<Path>) event;
        Path filename = ev.context();
        Path autoUnLoadFile = autoLoadPath.resolve(filename);
        return autoUnLoadFile.toFile().getName().toLowerCase();
    }
}
