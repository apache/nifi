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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Starts a thread to monitor the auto-load directory for new NARs.
 */
public class NarAutoLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoLoader.class);
    private static final String NAR_PROVIDER_PREFIX = "nifi.nar.library.provider.";
    private static final String IMPLEMENTATION_PROPERTY = "implementation";

    private static final long POLL_INTERVAL_MS = 5000;
    private static final long POLL_INTERVAL_NAR_PROVIDER_MS = 300000;

    private final NiFiProperties properties;
    private final NarLoader narLoader;
    private final ExtensionManager extensionManager;

    private volatile Set<NarProviderTask> narProviderTasks;
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

        narProviderTasks = new HashSet<>();

        for (final String externalSourceName : properties.getDirectSubsequentTokens(NAR_PROVIDER_PREFIX)) {
            LOGGER.info("NAR Provider {} found in configuration", externalSourceName);

            final NarProviderInitializationContext context = new PropertyBasedNarProviderInitializationContext(properties, externalSourceName);
            final String implementationClass = properties.getProperty(NAR_PROVIDER_PREFIX + externalSourceName + "." + IMPLEMENTATION_PROPERTY);
            final String providerId = UUID.randomUUID().toString();
            final NarProvider provider = NarThreadContextClassLoader.createInstance(extensionManager, implementationClass, NarProvider.class, properties, providerId);
            provider.initialize(context);

            final ClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(providerId);
            final ClassLoader providerClassLoader = instanceClassLoader == null ? provider.getClass().getClassLoader() : instanceClassLoader;
            final NarProviderTask task = new NarProviderTask(provider, providerClassLoader, properties.getNarAutoLoadDirectory(), POLL_INTERVAL_NAR_PROVIDER_MS);
            narProviderTasks.add(task);

            final Thread providerThread = new Thread(task);
            providerThread.setName("NAR Provider Task - " + externalSourceName);
            providerThread.setDaemon(true);
            providerThread.setContextClassLoader(provider.getClass().getClassLoader());
            providerThread.start();
        }
    }

    public synchronized void stop() {
        started = false;
        narAutoLoaderTask.stop();
        narAutoLoaderTask = null;

        narProviderTasks.forEach(NarProviderTask::stop);
        narProviderTasks = null;

        LOGGER.info("NAR Auto-Loader stopped");
    }
}
