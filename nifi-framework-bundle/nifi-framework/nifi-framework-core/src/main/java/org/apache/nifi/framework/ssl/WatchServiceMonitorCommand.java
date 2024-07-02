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
package org.apache.nifi.framework.ssl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runnable command to poll a configured File Watch Service and notify registered listeners
 */
public class WatchServiceMonitorCommand implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WatchServiceMonitorCommand.class);

    private final WatchService watchService;

    private final ChangedPathListener changedPathListener;

    public WatchServiceMonitorCommand(final WatchService watchService, final ChangedPathListener changedPathListener) {
        this.watchService = Objects.requireNonNull(watchService, "Watch Service required");
        this.changedPathListener = Objects.requireNonNull(changedPathListener, "Changed Path Listener required");
    }

    /**
     * Poll Watch Service and process events
     */
    public void run() {
        final WatchKey watchKey = watchService.poll();
        if (watchKey == null) {
            logger.debug("Watch Key not found");
        } else {
            try {
                processWatchKey(watchKey);
            } finally {
                if (watchKey.reset()) {
                    logger.debug("Watch Key reset completed");
                } else {
                    logger.warn("Watch Key reset failed: Watch Service no longer valid");
                }
            }
        }
    }

    private void processWatchKey(final WatchKey watchKey) {
        final List<WatchEvent<?>> events = watchKey.pollEvents();
        final List<Path> changedPaths = getChangedPaths(events);
        if (changedPaths.isEmpty()) {
            logger.debug("Changed Paths not found");
        } else {
            logger.debug("Changed Paths found {}", changedPaths);
            changedPathListener.onChanged(changedPaths);
        }
    }

    private List<Path> getChangedPaths(final List<WatchEvent<?>> events) {
        final List<Path> changedPaths = new ArrayList<>();

        for (final WatchEvent<?> event : events) {
            final WatchEvent.Kind<?> kind = event.kind();
            if (StandardWatchEventKinds.OVERFLOW == kind) {
                continue;
            }

            final Object context = event.context();
            if (context instanceof Path path) {
                changedPaths.add(path);
            }
        }

        return changedPaths;
    }

    /**
     * Changed Path Listener for handling file watch events
     */
    public interface ChangedPathListener {
        /**
         * Handle Changed Paths
         *
         * @param changedPaths Changed Paths
         */
        void onChanged(List<Path> changedPaths);
    }
}
