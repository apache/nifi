/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NarPersistenceProviderFactoryBean implements FactoryBean<NarPersistenceProvider>, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(NarPersistenceProviderFactoryBean.class);

    public static final String DEFAULT_NAR_MANAGER_IMPLEMENTATION = StandardNarPersistenceProvider.class.getName();

    private final NiFiProperties properties;
    private final ExtensionManager extensionManager;

    private NarPersistenceProvider narPersistenceProvider;

    public NarPersistenceProviderFactoryBean(final NiFiProperties properties, final ExtensionManager extensionManager) {
        this.properties = properties;
        this.extensionManager = extensionManager;
    }

    @Override
    public NarPersistenceProvider getObject() {
        if (narPersistenceProvider == null) {
            final String implementationClassName = properties.getProperty(NiFiProperties.NAR_PERSISTENCE_PROVIDER_IMPLEMENTATION_CLASS, DEFAULT_NAR_MANAGER_IMPLEMENTATION);
            if (StringUtils.isBlank(implementationClassName)) {
                throw new RuntimeException("Cannot create NAR Persistence Provider because NiFi Properties is missing the following property: "
                        + NiFiProperties.NAR_PERSISTENCE_PROVIDER_IMPLEMENTATION_CLASS);
            }

            logger.info("Creating NAR Persistence Provider [{}]", implementationClassName);
            try {
                final NarPersistenceProvider initialNarPersistenceProvider = NarThreadContextClassLoader.createInstance(
                        extensionManager, implementationClassName, NarPersistenceProvider.class, properties);

                final Map<String, String> initializationProperties = properties.getPropertiesWithPrefix(NiFiProperties.NAR_PERSISTENCE_PROVIDER_PROPERTIES_PREFIX).entrySet().stream()
                        .map(entry -> new Tuple<>(entry.getKey().replace(NiFiProperties.NAR_PERSISTENCE_PROVIDER_PROPERTIES_PREFIX, ""), entry.getValue()))
                        .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

                final NarPersistenceProvider wrappedNarPersistenceProvider = wrapWithComponentNarLoader(initialNarPersistenceProvider);
                wrappedNarPersistenceProvider.initialize(new StandardNarPersistenceProviderInitializationContext(initializationProperties));
                narPersistenceProvider = wrappedNarPersistenceProvider;
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create NAR Persistence Provider", e);
            }
        }
        return narPersistenceProvider;
    }

    @Override
    public Class<?> getObjectType() {
        return NarPersistenceProvider.class;
    }

    @Override
    public void destroy() {
        if (narPersistenceProvider != null) {
            narPersistenceProvider.shutdown();
        }
    }

    private NarPersistenceProvider wrapWithComponentNarLoader(final NarPersistenceProvider originalInstance) {
        final ClassLoader originalClassLoader = originalInstance.getClass().getClassLoader();
        return new NarPersistenceProvider() {
            @Override
            public void initialize(final NarPersistenceProviderInitializationContext initializationContext) throws IOException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    originalInstance.initialize(initializationContext);
                }
            }

            @Override
            public void shutdown() {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    originalInstance.shutdown();
                }
            }

            @Override
            public File createTempFile(final InputStream inputStream) throws IOException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    return originalInstance.createTempFile(inputStream);
                }
            }

            @Override
            public NarPersistenceInfo saveNar(final NarPersistenceContext persistenceContext, final File tempNarFile) throws IOException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    return originalInstance.saveNar(persistenceContext, tempNarFile);
                }
            }

            @Override
            public void deleteNar(final BundleCoordinate narCoordinate) throws IOException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    originalInstance.deleteNar(narCoordinate);
                }
            }

            @Override
            public InputStream readNar(final BundleCoordinate narCoordinate) throws FileNotFoundException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    return originalInstance.readNar(narCoordinate);
                }
            }

            @Override
            public boolean exists(final BundleCoordinate narCoordinate) {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    return originalInstance.exists(narCoordinate);
                }
            }

            @Override
            public NarPersistenceInfo getNarInfo(final BundleCoordinate narCoordinate) throws IOException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    return originalInstance.getNarInfo(narCoordinate);
                }
            }

            @Override
            public Set<NarPersistenceInfo> getAllNarInfo() throws IOException {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(originalClassLoader)) {
                    return originalInstance.getAllNarInfo();
                }
            }
        };
    }
}
