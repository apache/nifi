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
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.Optional;

/**
 * Starts a thread to monitor the auto-load directory for new NARs.
 */
public class NarAutoLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoLoader.class);
    private static final String NAR_AUTO_LOADER_TASK_TYPE_PROPERTY = "nifi.library.nar.autoload.externalsource";

    private static final long POLL_INTERVAL_MS = 5000;

    private final NiFiProperties properties;
    private final NarLoader narLoader;
    private final ExtensionManager extensionManager;

    private volatile Optional<NarAutoLoaderExternalSourceTask> externalSourceTask = Optional.empty();
    private volatile NarAutoLoaderTask narAutoLoaderTask;
    private volatile boolean started = false;

    public NarAutoLoader(final NiFiProperties properties, final NarLoader narLoader, final ExtensionManager extensionManager) {
        this.properties = Objects.requireNonNull(properties);
        this.narLoader = Objects.requireNonNull(narLoader);
        this.extensionManager = Objects.requireNonNull(extensionManager);
    }

    public synchronized void start() throws IllegalAccessException, InstantiationException, ClassNotFoundException, IOException {
        if (started) {
            return;
        }

        final File autoLoadDir = properties.getNarAutoLoadDirectory();
        FileUtils.ensureDirectoryExistAndCanRead(autoLoadDir);

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

        final Thread autoLoaderThread = new Thread(narAutoLoaderTask);
        autoLoaderThread.setName("NAR Auto-Loader");
        autoLoaderThread.setDaemon(true);
        autoLoaderThread.start();

        final Optional<NarAutoLoaderExternalSource> externalSource = getExternalSource();

        if (externalSource.isPresent()) {
            externalSourceTask = Optional.of(new NarAutoLoaderExternalSourceTask(externalSource.get(), 5000)); // TODO
            final Thread externalSourceThread = new Thread(externalSourceTask.get());
            externalSourceThread.setName("NAR Auto-Loader External Source Task");
            externalSourceThread.setDaemon(true);
            externalSourceThread.setContextClassLoader(externalSource.get().getClass().getClassLoader());
            externalSourceThread.start();
        }
    }

    private Optional<NarAutoLoaderExternalSource> getExternalSource() throws IllegalAccessException, ClassNotFoundException, InstantiationException  {
        final String loaderTaskType = properties.getProperty(NAR_AUTO_LOADER_TASK_TYPE_PROPERTY);

        if (loaderTaskType == null) {
            return Optional.empty();
        }

        final NarAutoLoaderExternalSource instance = NarThreadContextClassLoader.createInstance(extensionManager, loaderTaskType, NarAutoLoaderExternalSource.class, properties);
        instance.start(new PropertyBasedNarAutoLoaderContext(properties));
        return Optional.of(instance);
    }

    public synchronized void stop() {
        started = false;
        narAutoLoaderTask.stop();
        narAutoLoaderTask = null;
        externalSourceTask.ifPresent(task -> task.stop());
        LOGGER.info("NAR Auto-Loader stopped");
    }
}
