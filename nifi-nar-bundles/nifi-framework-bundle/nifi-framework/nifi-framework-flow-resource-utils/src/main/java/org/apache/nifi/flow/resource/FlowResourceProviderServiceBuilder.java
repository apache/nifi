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
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

public final class FlowResourceProviderServiceBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowResourceProviderServiceBuilder.class);

    private static final String IMPLEMENTATION_PROPERTY = "implementation";
    private static final long DEFAULT_POLL_INTERVAL_MS = 300000;
    private static final FlowResourceConflictResolutionStrategy DEFAULT_CONFLICT_RESOLUTION = new DoNotReplaceResolutionStrategy();

    private final String serviceName;
    private final ExtensionManager extensionManager;
    private final NiFiProperties properties;
    private final String providerPropertyPrefix;

    private File targetDirectory;
    private long pollInterval = DEFAULT_POLL_INTERVAL_MS;
    private FlowResourceConflictResolutionStrategy conflictResolutionStrategy = DEFAULT_CONFLICT_RESOLUTION;
    private Optional<Predicate<FlowResourceDescriptor>> filter = Optional.empty();
    private boolean restrainStartup = true;

    public FlowResourceProviderServiceBuilder(
            final String serviceName,
            final ExtensionManager extensionManager,
            final NiFiProperties properties,
            final String providerPropertyPrefix) {
        this.serviceName = serviceName;
        this.extensionManager = extensionManager;
        this.properties = properties;
        this.providerPropertyPrefix = providerPropertyPrefix;
    }

    public FlowResourceProviderServiceBuilder withTargetDirectory(final File targetDirectory) {
        this.targetDirectory = targetDirectory;
        return this;
    }

    public FlowResourceProviderServiceBuilder withTargetDirectoryProperty(final String propertyName, final String defaultValue) {
        this.targetDirectory = new File(properties.getProperty(propertyName, defaultValue));
        return this;
    }

    public FlowResourceProviderServiceBuilder withConflictResolutionStrategy(final FlowResourceConflictResolutionStrategy conflictResolutionStrategy) {
        this.conflictResolutionStrategy = conflictResolutionStrategy;
        return this;
    }

    public FlowResourceProviderServiceBuilder withPollInterval(final long pollInterval) {
        this.pollInterval = pollInterval;
        return this;
    }

    public FlowResourceProviderServiceBuilder withPollIntervalProperty(final String propertyName) {
        return withPollIntervalProperty(propertyName, DEFAULT_POLL_INTERVAL_MS);
    }

    public FlowResourceProviderServiceBuilder withPollIntervalProperty(final String propertyName, final long defaultValue) {
        this.pollInterval = properties.getLongProperty(propertyName, defaultValue);
        return this;
    }

    public FlowResourceProviderServiceBuilder withResourceFilter(final Predicate<FlowResourceDescriptor> filter) {
        this.filter = Optional.of(filter);
        return this;
    }

    public FlowResourceProviderServiceBuilder withRestrainingStartup(final String propertyName, final boolean defaultValue) {
        this.restrainStartup =  Boolean.parseBoolean(properties.getProperty(propertyName, Boolean.toString(defaultValue)));
        return this;
    }

    public FlowResourceProviderService build() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (targetDirectory == null) {
            throw new IllegalArgumentException("Target directory must be specified");
        }

        final Set<FlowResourceProviderWorker> workers = new HashSet<>();
        final Set<String> externalSourceNames = properties.getDirectSubsequentTokens(providerPropertyPrefix);
        final CountDownLatch restrainStartupLatch =  new CountDownLatch(restrainStartup ? externalSourceNames.size() : 0);

        for(final String externalSourceName : externalSourceNames) {
            LOGGER.info("Flow resource provider \'{}\' found in configuration", externalSourceName);

            // Create provider
            final FlowResourceProviderInitializationContext context
                    = new PropertyBasedFlowResourceProviderInitializationContext(properties, providerPropertyPrefix + externalSourceName + ".", filter);
            final String providerClass = properties.getProperty(providerPropertyPrefix + externalSourceName + "." + IMPLEMENTATION_PROPERTY);
            final String providerId = UUID.randomUUID().toString();
            final FlowResourceProvider provider = NarThreadContextClassLoader.createInstance(extensionManager, providerClass, FlowResourceProvider.class, properties, providerId);
            provider.initialize(context);

            // Create provider worker
            final ClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(providerId);
            final ClassLoader providerClassLoader = instanceClassLoader == null ? provider.getClass().getClassLoader() : instanceClassLoader;
            final FlowResourceProviderWorker providerWorker
                    = new BufferingFlowResourceProviderWorker(serviceName, providerClassLoader, provider, conflictResolutionStrategy, targetDirectory, pollInterval, restrainStartupLatch);

            workers.add(providerWorker);
        }

        return new CompositeFlowResourceProviderService(serviceName, workers, restrainStartupLatch);
    }
}
