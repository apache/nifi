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
package org.apache.nifi.flow.resource;

import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * This implementation of the worker uses a strategy in order to decide if an available resources is to download. Using
 * different strategies result different behaviour. Like for example with {@code DoNotReplaceResolutionStrategy}
 * the worker will not download a file with the same name until the given file is in the target directory, but in the other hand
 * {@code ReplaceWithNewerResolutionStrategy} will replace resources if a new version is available in the external source.
 */
abstract class ConflictResolvingFlowResourceProviderWorker implements FlowResourceProviderWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConflictResolvingFlowResourceProviderWorker.class);

    // A unique id is necessary for temporary files not to collide with temporary files from other instances.
    private final String id = UUID.randomUUID().toString();

    private final String name;
    private final ClassLoader providerClassLoader;
    private final FlowResourceProvider provider;
    private final File targetDirectory;
    private final long pollTimeInMs;
    private final FlowResourceConflictResolutionStrategy resolutionStrategy;
    private final CountDownLatch restrainStartupLatch;

    private volatile boolean stopped = false;

    ConflictResolvingFlowResourceProviderWorker(
        final String namePrefix,
        final ClassLoader providerClassLoader,
        final FlowResourceProvider provider,
        final FlowResourceConflictResolutionStrategy resolutionStrategy,
        final File targetDirectory,
        final long pollTimeInMs,
        final CountDownLatch restrainStartupLatch
    ) {
        this.name = namePrefix + " - " + id;
        this.providerClassLoader = providerClassLoader;
        this.provider = provider;
        this.resolutionStrategy = resolutionStrategy;
        this.targetDirectory = targetDirectory;
        this.pollTimeInMs = pollTimeInMs;
        this.restrainStartupLatch = restrainStartupLatch;
    }

    @Override
    public void run() {
        LOGGER.info("Flow resource provider worker is started");

        while (!stopped) {
            try {
                FileUtils.ensureDirectoryExistAndCanReadAndWrite(targetDirectory);
            } catch (final IOException e) {
                LOGGER.error("Could not ensure that target directory is accessible", e);
                stopped = true;
            }

            if (!stopped) {
                try {
                    poll();
                    restrainStartupLatch.countDown();
                } catch (final Throwable e) {
                    LOGGER.error("Error during polling for flow resources", e);
                }
            }

            try {
                Thread.sleep(pollTimeInMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Flow resource provider worker is interrupted");
                stopped = true;
            }
        }
    }

    private void poll() throws IOException {
        LOGGER.debug("Worker starts polling provider for resources");

        final Collection<FlowResourceDescriptor> availableResources;
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(providerClassLoader)) {
            availableResources = provider.listResources();
        }

        for (final FlowResourceDescriptor availableResource : availableResources) {
            if (resolutionStrategy.shouldBeFetched(targetDirectory, availableResource)) {
                acquireResource(availableResource);
            } else {
                LOGGER.trace("Flow resource {} is not to be fetched", availableResource.getFileName());
            }
        }
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final FlowResourceProvider getProvider() {
        return provider;
    }

    @Override
    public final boolean isRunning() {
        return !stopped;
    }

    @Override
    public final void stop() {
        LOGGER.info("Flow resource provider worker is stopped");
        stopped = true;
    }

    protected final String getId() {
        return id;
    }

    protected final File getTargetDirectory() {
        return targetDirectory;
    }

    protected final ClassLoader getProviderClassLoader() {
        return providerClassLoader;
    }

    /**
     * This method is responsible to acquire the resource based on the provided descriptor.
     */
    protected abstract void acquireResource(final FlowResourceDescriptor availableResource) throws IOException;
}
