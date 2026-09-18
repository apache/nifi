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
package org.apache.nifi.migration;

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.reporting.ReportingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Runs {@code migrateProperties} against a copy of a property map so callers can compare the
 * migrated result with the live configuration without persisting changes or creating Controller Services.
 */
public final class PropertyMigrationPreview {
    private static final Logger logger = LoggerFactory.getLogger(PropertyMigrationPreview.class);

    private static final ControllerServiceFactory NO_OP_FACTORY = new ControllerServiceFactory() {
        @Override
        public ControllerServiceCreationDetails getCreationDetails(final String implementationClassName, final Map<String, String> propertyValues) {
            return new ControllerServiceCreationDetails("property-migration-preview", implementationClassName, null, propertyValues,
                    ControllerServiceCreationDetails.CreationState.SERVICE_ALREADY_EXISTS);
        }

        @Override
        public ControllerServiceNode create(final ControllerServiceCreationDetails creationDetails) {
            return null;
        }
    };

    private PropertyMigrationPreview() {
    }

    /**
     * @param component the component whose {@code migrateProperties} implementation should be invoked
     * @param extensionManager the extension manager used to jail the call to the component NAR; may be {@code null} in tests
     * @param componentId the component instance identifier
     * @param componentDescription a description used in log messages
     * @param effectiveValueResolver maps raw property values to effective values (parameter substitution)
     * @param originalPropertyValues the snapshot property values to migrate
     * @return the migrated raw properties when migration modifies the configuration; otherwise empty
     */
    public static Optional<Map<String, String>> preview(final ConfigurableComponent component, final ExtensionManager extensionManager,
                                                        final String componentId, final String componentDescription,
                                                        final Function<String, String> effectiveValueResolver,
                                                        final Map<String, String> originalPropertyValues) {
        if (component == null || originalPropertyValues == null) {
            return Optional.empty();
        }

        try {
            final Map<String, String> rawProperties = new LinkedHashMap<>(originalPropertyValues);
            final Map<String, String> effectiveProperties = new LinkedHashMap<>();
            rawProperties.forEach((key, value) -> effectiveProperties.put(key, effectiveValueResolver.apply(value)));

            final StandardPropertyConfiguration propertyConfig = new StandardPropertyConfiguration(
                    effectiveProperties, rawProperties, effectiveValueResolver, componentDescription, NO_OP_FACTORY);

            if (extensionManager == null) {
                migrateProperties(component, propertyConfig);
            } else {
                try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, component.getClass(), componentId)) {
                    migrateProperties(component, propertyConfig);
                }
            }

            if (!propertyConfig.isModified()) {
                return Optional.empty();
            }

            return Optional.of(new LinkedHashMap<>(propertyConfig.getRawProperties()));
        } catch (final Exception e) {
            logger.debug("Failed to preview property migration for {}", componentDescription, e);
            return Optional.empty();
        }
    }

    private static void migrateProperties(final ConfigurableComponent component, final PropertyConfiguration propertyConfig) {
        switch (component) {
            case Processor processor -> processor.migrateProperties(propertyConfig);
            case ControllerService controllerService -> controllerService.migrateProperties(propertyConfig);
            case ReportingTask reportingTask -> reportingTask.migrateProperties(propertyConfig);
            case ParameterProvider parameterProvider -> parameterProvider.migrateProperties(propertyConfig);
            case FlowRegistryClient flowRegistryClient -> flowRegistryClient.migrateProperties(propertyConfig);
            default -> logger.debug("Cannot preview property migration for {}", component.getClass().getName());
        }
    }
}
