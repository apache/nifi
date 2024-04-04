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

import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class ExternalResourceProviderServiceBuilder {
    private final static Map<String, Supplier<ExternalResourceConflictResolutionStrategy>> STRATEGIES = new HashMap<>();

    static {
        STRATEGIES.put("REPLACE", ReplaceWithNewerResolutionStrategy::new);
        STRATEGIES.put("IGNORE", DoNotReplaceResolutionStrategy::new);
    }

    private final String serviceName;
    private final ExtensionManager extensionManager;

    private File targetDirectory;
    private long pollIntervalInMillis;
    private ExternalResourceConflictResolutionStrategy conflictResolutionStrategy;
    private Map<String, ExternalResourceProvider> providers;
    private boolean restrainStartup = true;

    public ExternalResourceProviderServiceBuilder(
            final String serviceName,
            final ExtensionManager extensionManager) {
        this.serviceName = serviceName;
        this.extensionManager = extensionManager;
    }

    public ExternalResourceProviderServiceBuilder targetDirectory(final File targetDirectory) {
        this.targetDirectory = targetDirectory;
        return this;
    }

    public ExternalResourceProviderServiceBuilder providers(final Map<String, ExternalResourceProvider> providers) {
        this.providers = providers;
        return this;
    }

    public ExternalResourceProviderServiceBuilder conflictResolutionStrategy(final String conflictResolutionStrategy) {
        this.conflictResolutionStrategy = getExternalResourceConflictResolutionStrategy(conflictResolutionStrategy);
        return this;
    }

    public ExternalResourceProviderServiceBuilder pollInterval(final String pollInterval) {
        this.pollIntervalInMillis = Math.round(FormatUtils.getPreciseTimeDuration(pollInterval.trim(), TimeUnit.MILLISECONDS));
        return this;
    }

    public ExternalResourceProviderServiceBuilder restrainingStartup(final boolean restrainStartup) {
        this.restrainStartup = restrainStartup;
        return this;
    }

    public ExternalResourceProviderService build() throws ClassNotFoundException, InstantiationException, IllegalAccessException, TlsException {
        if (targetDirectory == null) {
            throw new IllegalArgumentException("Target directory must be specified");
        }

        final Set<ExternalResourceProviderWorker> workers = new HashSet<>();
        final CountDownLatch restrainStartupLatch =  new CountDownLatch(restrainStartup ? providers.size() : 0);

        for(final Map.Entry<String, ExternalResourceProvider> provider : providers.entrySet()) {
            final ClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(provider.getKey());
            final ClassLoader providerClassLoader = instanceClassLoader == null ? provider.getValue().getClass().getClassLoader() : instanceClassLoader;
            final ExternalResourceProviderWorker providerWorker = new CollisionAwareResourceProviderWorker(
                    serviceName, providerClassLoader, provider.getValue(), conflictResolutionStrategy, targetDirectory, pollIntervalInMillis, restrainStartupLatch);

            workers.add(providerWorker);
        }

        return new CompositeExternalResourceProviderService(serviceName, workers, restrainStartupLatch);
    }

    private static ExternalResourceConflictResolutionStrategy getExternalResourceConflictResolutionStrategy(final String conflictResolutionStrategy) {
        if (!STRATEGIES.containsKey(conflictResolutionStrategy)) {
            throw new IllegalArgumentException("Unknown conflict resolution strategy: " + conflictResolutionStrategy);
        }

        return STRATEGIES.get(conflictResolutionStrategy).get();
    }
}
