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
package org.apache.nifi.stateless.core;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComponentFactory {
    private static final Logger logger = LoggerFactory.getLogger(ComponentFactory.class);
    private final ExtensionManager extensionManager;

    public ComponentFactory(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }


    public StatelessProcessorWrapper createProcessor(final VersionedProcessor versionedProcessor, final boolean materializeContent, final StatelessControllerServiceLookup controllerServiceLookup,
                                                     final VariableRegistry variableRegistry, final Set<URL> classpathUrls, final ParameterContext parameterContext)
        throws ProcessorInstantiationException {

        final String type = versionedProcessor.getType();
        final String identifier = versionedProcessor.getIdentifier();

        final Bundle bundle = getAvailableBundle(versionedProcessor.getBundle(), type);
        if (bundle == null) {
            throw new IllegalStateException("Unable to find bundle for coordinate "
                + versionedProcessor.getBundle().getGroup() + ":"
                + versionedProcessor.getBundle().getArtifact() + ":"
                + versionedProcessor.getBundle().getVersion());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle,
                classpathUrls == null ? Collections.emptySet() : classpathUrls);

            logger.debug("Setting context class loader to {} (parent = {}) to create {}", detectedClassLoader, detectedClassLoader.getParent(), type);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Object extensionInstance = rawClass.newInstance();
            final ComponentLog componentLog = new SLF4JComponentLog(extensionInstance);

            final Processor processor = (Processor) extensionInstance;
            final ProcessorInitializationContext initializationContext = new StatelessProcessorInitializationContext(versionedProcessor.getIdentifier(), processor, controllerServiceLookup);
            processor.initialize(initializationContext);

            // If no classpath urls were provided, check if we need to add additional classpath URL's based on configured properties.
            if (classpathUrls == null) {
                final Set<URL> additionalClasspathUrls = getAdditionalClasspathResources(processor.getPropertyDescriptors(), processor.getIdentifier(), versionedProcessor.getProperties(),
                    parameterContext, variableRegistry,componentLog);

                if (!additionalClasspathUrls.isEmpty()) {
                    return createProcessor(versionedProcessor, materializeContent, controllerServiceLookup, variableRegistry, additionalClasspathUrls, parameterContext);
                }
            }

            final StatelessProcessorWrapper processorWrapper = new StatelessProcessorWrapper(versionedProcessor.getIdentifier(), processor, null,
                controllerServiceLookup, variableRegistry, materializeContent, detectedClassLoader, parameterContext);

            // Configure the Processor
            processorWrapper.setAnnotationData(versionedProcessor.getAnnotationData());
            versionedProcessor.getProperties().forEach(processorWrapper::setProperty);
            for (String relationship : versionedProcessor.getAutoTerminatedRelationships()) {
                processorWrapper.addAutoTermination(new Relationship.Builder().name(relationship).build());
            }

            return processorWrapper;
        } catch (final Exception e) {
            throw new ProcessorInstantiationException(type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }


    private Set<URL> getAdditionalClasspathResources(final List<PropertyDescriptor> propertyDescriptors, final String componentId, final Map<String, String> properties,
                                                     final ParameterLookup parameterLookup, final VariableRegistry variableRegistry, final ComponentLog logger) {
        final Set<String> modulePaths = new LinkedHashSet<>();
        for (final PropertyDescriptor descriptor : propertyDescriptors) {
            if (descriptor.isDynamicClasspathModifier()) {
                final String value = properties.get(descriptor.getName());
                if (!StringUtils.isEmpty(value)) {
                    final StandardPropertyValue propertyValue = new StandardPropertyValue(value, null, parameterLookup, variableRegistry);
                    modulePaths.add(propertyValue.evaluateAttributeExpressions().getValue());
                }
            }
        }

        final Set<URL> additionalUrls = new LinkedHashSet<>();
        try {
            final URL[] urls = ClassLoaderUtils.getURLsForClasspath(modulePaths, null, true);
            if (urls != null) {
                additionalUrls.addAll(Arrays.asList(urls));
            }
        } catch (MalformedURLException mfe) {
            logger.error("Error processing classpath resources for " + componentId + ": " + mfe.getMessage(), mfe);
        }

        return additionalUrls;
    }


    public ControllerService createControllerService(final VersionedControllerService versionedControllerService, final VariableRegistry variableRegistry,
                                                     final ControllerServiceLookup serviceLookup, final StateManager stateManager, final ParameterLookup parameterLookup) {
        return createControllerService(versionedControllerService, variableRegistry, null, serviceLookup, stateManager, parameterLookup);
    }


    private ControllerService createControllerService(final VersionedControllerService versionedControllerService, final VariableRegistry variableRegistry, final Set<URL> classpathUrls,
                                                      final ControllerServiceLookup serviceLookup, final StateManager stateManager, final ParameterLookup parameterLookup) {

        final String type = versionedControllerService.getType();
        final String identifier = versionedControllerService.getIdentifier();

        final Bundle bundle = getAvailableBundle(versionedControllerService.getBundle(), type);
        if (bundle == null) {
            throw new IllegalStateException("Unable to find bundle for coordinate "
                + versionedControllerService.getBundle().getGroup() + ":"
                + versionedControllerService.getBundle().getArtifact() + ":"
                + versionedControllerService.getBundle().getVersion());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(type, identifier, bundle,
                classpathUrls == null ? Collections.emptySet() : classpathUrls);

            logger.debug("Setting context class loader to {} (parent = {}) to create {}", detectedClassLoader, detectedClassLoader.getParent(), type);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Object extensionInstance = rawClass.newInstance();
            final ComponentLog componentLog = new SLF4JComponentLog(extensionInstance);

            final ControllerService service = (ControllerService) extensionInstance;
            final ControllerServiceInitializationContext initializationContext = new StatelessControllerServiceInitializationContext(identifier, service, serviceLookup, stateManager);
            service.initialize(initializationContext);

            // If no classpath urls were provided, check if we need to add additional classpath URL's based on configured properties.
            if (classpathUrls == null) {
                final Set<URL> additionalClasspathUrls = getAdditionalClasspathResources(service.getPropertyDescriptors(), service.getIdentifier(), versionedControllerService.getProperties(),
                    parameterLookup, variableRegistry, componentLog);

                if (!additionalClasspathUrls.isEmpty()) {
                    return createControllerService(versionedControllerService, variableRegistry, additionalClasspathUrls, serviceLookup, stateManager, parameterLookup);
                }
            }

            return service;
        } catch (final Exception e) {
            throw new ControllerServiceInstantiationException(type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    private Bundle getAvailableBundle(final org.apache.nifi.registry.flow.Bundle bundle, final String componentType) {
        final BundleCoordinate bundleCoordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        final Bundle availableBundle = extensionManager.getBundle(bundleCoordinate);
        if (availableBundle != null) {
            return availableBundle;
        }

        final List<Bundle> possibleBundles = extensionManager.getBundles(componentType);
        if (possibleBundles.isEmpty()) {
            throw new IllegalStateException("Could not find any NiFi Bundles that contain the Extension [" + componentType + "]");
        }

        if (possibleBundles.size() > 1) {
            throw new IllegalStateException("Found " + possibleBundles.size() + " different NiFi Bundles that contain the Extension [" + componentType + "] but none of them had a version of " +
                bundle.getVersion());
        }

        return possibleBundles.get(0);
    }
}
