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
import org.apache.nifi.nar.NarProvider;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.security.util.TlsException;
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

public final class ExternalResourceProviderServiceBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalResourceProviderServiceBuilder.class);

    private static final String IMPLEMENTATION_PROPERTY = "implementation";
    private static final long DEFAULT_POLL_INTERVAL_MS = 300000;

    private final String serviceName;
    private final ExtensionManager extensionManager;
    private final NiFiProperties properties;
    private final String providerPropertyPrefix;

    private File targetDirectory;
    private long pollInterval = DEFAULT_POLL_INTERVAL_MS;
    private String conflictResolutionStrategy;
    private Optional<Predicate<ExternalResourceDescriptor>> filter = Optional.empty();
    private boolean restrainStartup = true;

    public ExternalResourceProviderServiceBuilder(
            final String serviceName,
            final ExtensionManager extensionManager,
            final NiFiProperties properties,
            final String providerPropertyPrefix) {
        this.serviceName = serviceName;
        this.extensionManager = extensionManager;
        this.properties = properties;
        this.providerPropertyPrefix = providerPropertyPrefix;
    }

    public ExternalResourceProviderServiceBuilder targetDirectory(final File targetDirectory) {
        this.targetDirectory = targetDirectory;
        return this;
    }

    public ExternalResourceProviderServiceBuilder targetDirectoryProperty(final String propertyName, final String defaultValue) {
        this.targetDirectory = new File(properties.getProperty(propertyName, defaultValue));
        return this;
    }

    public ExternalResourceProviderServiceBuilder conflictResolutionStrategy(final String propertyName, final String defaultValue) {
        this.conflictResolutionStrategy = properties.getProperty(propertyName, defaultValue);
        return this;
    }

    public ExternalResourceProviderServiceBuilder pollInterval(final long pollInterval) {
        this.pollInterval = pollInterval;
        return this;
    }

    public ExternalResourceProviderServiceBuilder pollIntervalProperty(final String propertyName) {
        return pollIntervalProperty(propertyName, DEFAULT_POLL_INTERVAL_MS);
    }

    public ExternalResourceProviderServiceBuilder pollIntervalProperty(final String propertyName, final long defaultValue) {
        this.pollInterval = properties.getLongProperty(propertyName, defaultValue);
        return this;
    }

    public ExternalResourceProviderServiceBuilder resourceFilter(final Predicate<ExternalResourceDescriptor> filter) {
        this.filter = Optional.of(filter);
        return this;
    }

    public ExternalResourceProviderServiceBuilder restrainingStartup(final String propertyName, final boolean defaultValue) {
        this.restrainStartup = Boolean.parseBoolean(properties.getProperty(propertyName, Boolean.toString(defaultValue)));
        return this;
    }

    public ExternalResourceProviderService build() throws ClassNotFoundException, InstantiationException, IllegalAccessException, TlsException {
        if (targetDirectory == null) {
            throw new IllegalArgumentException("Target directory must be specified");
        }

        final ExternalResourceConflictResolutionStrategy conflictResolutionStrategy = NarThreadContextClassLoader
                .createInstance(extensionManager, this.conflictResolutionStrategy, ExternalResourceConflictResolutionStrategy.class, properties, "conflictResolutionStrategy");

        final Set<ExternalResourceProviderWorker> workers = new HashSet<>();
        final Set<String> externalSourceNames = properties.getDirectSubsequentTokens(providerPropertyPrefix);
        final CountDownLatch restrainStartupLatch =  new CountDownLatch(restrainStartup ? externalSourceNames.size() : 0);

        for(final String externalSourceName : externalSourceNames) {
            LOGGER.info("External resource provider \'{}\' found in configuration", externalSourceName);

            // Create provider
            final String providerClass = properties.getProperty(providerPropertyPrefix + externalSourceName + "." + IMPLEMENTATION_PROPERTY);
            final String providerId = UUID.randomUUID().toString();

            final ExternalResourceProviderInitializationContext context
                    = new PropertyBasedExternalResourceProviderInitializationContext(properties, providerPropertyPrefix + externalSourceName + ".", filter);
            final ExternalResourceProvider provider = createProviderInstance(providerClass, providerId, context);

            // Create provider worker
            final ClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(providerId);
            final ClassLoader providerClassLoader = instanceClassLoader == null ? provider.getClass().getClassLoader() : instanceClassLoader;
            final ExternalResourceProviderWorker providerWorker
                    = new BufferingExternalResourceProviderWorker(serviceName, providerClassLoader, provider, conflictResolutionStrategy, targetDirectory, pollInterval, restrainStartupLatch);

            workers.add(providerWorker);
        }

        return new CompositeExternalResourceProviderService(serviceName, workers, restrainStartupLatch);
    }

    /**
     * In case the provider class is not an implementation of {@code ExternalResourceProvider} the method tries to instantiate it as a {@code NarProvider}. {@code NarProvider} instances
     * are wrapped into an adapter in order to envelope the support.
     */
    private ExternalResourceProvider createProviderInstance(
            final String providerClass,
            final String providerId,
            final ExternalResourceProviderInitializationContext context
    ) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        ExternalResourceProvider provider;

        try {
            provider = NarThreadContextClassLoader.createInstance(extensionManager, providerClass, ExternalResourceProvider.class, properties, providerId);
        } catch (final ClassCastException e) {
            LOGGER.warn("Class {} does not implement \"ExternalResourceProvider\" falling back to \"NarProvider\"");
            provider = new NarProviderAdapter(NarThreadContextClassLoader.createInstance(extensionManager, providerClass, NarProvider.class, properties, providerId));
        }

        provider.initialize(context);
        return provider;
    }
}
