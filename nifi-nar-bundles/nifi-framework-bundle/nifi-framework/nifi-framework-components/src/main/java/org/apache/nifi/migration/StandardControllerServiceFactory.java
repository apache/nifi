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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

public class StandardControllerServiceFactory implements ControllerServiceFactory {
    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceFactory.class);

    private final ExtensionManager extensionManager;
    private final FlowManager flowManager;
    private final ControllerServiceProvider serviceProvider;
    private final ComponentNode creator;

    public StandardControllerServiceFactory(final ExtensionManager extensionManager, final FlowManager flowManager, final ControllerServiceProvider serviceProvider,
                                            final ComponentNode creator) {
        this.extensionManager = extensionManager;
        this.flowManager = flowManager;
        this.serviceProvider = serviceProvider;
        this.creator = creator;
    }


    @Override
    public ControllerServiceCreationDetails getCreationDetails(final String implementationClassName, final Map<String, String> propertyValues) {
        final String serviceId = determineServiceId(implementationClassName, propertyValues);
        final ControllerServiceNode existingNode = flowManager.getControllerServiceNode(serviceId);
        if (existingNode != null) {
            final Class<? extends ControllerService> serviceClass = existingNode.getControllerServiceImplementation().getClass();
            if (isImplementation(serviceClass, implementationClassName)) {
                logger.debug("Found existing Controller Service with ID {} for implementation {}", serviceId, implementationClassName);
                return alreadyExists(existingNode, implementationClassName);
            }

            throw new IllegalArgumentException(String.format("Determined from provided implementation classname, Process Group of creator," +
                    "and provided property values that the Controller Service to create should have an ID of %s. However," +
                    "there already exists a Controller Service with that ID (%s) and it is not of the correct type: %s",
                    serviceId, existingNode, implementationClassName));
        }

        // There is no match. Create a new Controller Service
        final Bundle bundle = determineBundle(implementationClassName);
        return toBeCreated(serviceId, implementationClassName, bundle.getBundleDetails().getCoordinate(), propertyValues);
    }

    @Override
    public ControllerServiceNode create(final ControllerServiceCreationDetails creationDetails) {
        final ControllerServiceNode serviceNode = flowManager.createControllerService(creationDetails.type(), creationDetails.serviceIdentifier(), creationDetails.serviceBundleCoordinate(),
                Collections.emptySet(), true, true, null);

        final Optional<ProcessGroup> group = creator.getParentProcessGroup();
        if (group.isPresent()) {
            group.get().addControllerService(serviceNode);
            logger.info("Created {} in {} as a step in the migration of {}", serviceNode, group, creator);
        } else {
            flowManager.addRootControllerService(serviceNode);
            logger.info("Created {} as a Controller-Level Controller Service as a step in the migration of {}", serviceNode, creator);
        }

        serviceNode.setProperties(creationDetails.serviceProperties());

        final ControllerServiceFactory serviceFactory = new StandardControllerServiceFactory(extensionManager, flowManager, serviceProvider, serviceNode);
        serviceNode.migrateConfiguration(creationDetails.serviceProperties(), serviceFactory);

        if (isEnable()) {
            final ValidationStatus validationStatus = serviceNode.performValidation();
            if (validationStatus == ValidationStatus.VALID) {
                serviceProvider.enableControllerService(serviceNode);
                logger.info("Enabled newly created Controller Service {}", serviceNode);
            }
        }

        return serviceNode;
    }

    private boolean isEnable() {
        // Do not enable any Controller Services if it's added to a stateless group. Let the stateless group handle
        // the lifecycle of Controller Services on its own.
        final Optional<ProcessGroup> optionalGroup = creator.getParentProcessGroup();
        if (optionalGroup.isPresent()) {
            final ExecutionEngine executionEngine = optionalGroup.get().resolveExecutionEngine();
            if (executionEngine == ExecutionEngine.STATELESS) {
                logger.debug("Will not enable newly created Controller Services because parent group {} is stateless", optionalGroup.get());
                return false;
            }
        }

        return true;
    }

    private ControllerServiceCreationDetails toBeCreated(final String serviceId, final String type, final BundleCoordinate bundleCoordinate, final Map<String, String> propertyValues) {
        return new ControllerServiceCreationDetails(serviceId, type, bundleCoordinate, propertyValues, ControllerServiceCreationDetails.CreationState.SERVICE_TO_BE_CREATED);
    }

    private ControllerServiceCreationDetails alreadyExists(final ControllerServiceNode serviceNode, final String type) {
        final Map<String, String> propertyValues = new HashMap<>();
        serviceNode.getRawPropertyValues().forEach((key, value) -> propertyValues.put(key.getName(), value));

        return new ControllerServiceCreationDetails(serviceNode.getIdentifier(),
                type,
                serviceNode.getBundleCoordinate(),
                propertyValues,
                ControllerServiceCreationDetails.CreationState.SERVICE_ALREADY_EXISTS);
    }

    private boolean isImplementation(final Class<?> clazz, final String className) {
        if (className.equals(clazz.getName())) {
            return true;
        }

        final Class<?> superClass = clazz.getSuperclass();
        if (Object.class.equals(superClass)) {
            return false;
        }

        return isImplementation(superClass, className);
    }

    /**
     * Creates a deterministic UUID for the Controller Service based on the Process Group that the creator resides in,
     * if any, the implementation class name, and the given properties
     * @param className the classname of the Controller Service
     * @param propertyValues the property values
     * @return a UUID for the Controller Service
     */
    // Visible for testing
    protected String determineServiceId(final String className, final Map<String, String> propertyValues) {
        final SortedMap<String, String> sortedProperties = new TreeMap<>(propertyValues);
        final String componentDescription = creator.getProcessGroupIdentifier() + className + sortedProperties;
        final String serviceId = UUID.nameUUIDFromBytes(componentDescription.getBytes(StandardCharsets.UTF_8)).toString();
        logger.debug("For Controller Service of type {} created from {} will use UUID {}", className, creator, serviceId);
        return serviceId;
    }

    private Bundle determineBundle(final String implementationClassName) {
        logger.debug("Determining which Bundle should be used to create Controller Service of type {} for {}", implementationClassName, creator);

        // Get all available bundles for the given implementation type
        final List<Bundle> availableBundles = extensionManager.getBundles(implementationClassName);

        // If no versions are available, throw an Exception
        if (availableBundles.isEmpty()) {
            throw new IllegalArgumentException("Cannot create Controller Service because the implementation Class [%s] is not a known Controller Service".formatted(implementationClassName));
        }

        // If exactly 1 version is available, use it.
        if (availableBundles.size() == 1) {
            logger.debug("Found exactly 1 Bundle for Controller Service of type {}: {}", implementationClassName, availableBundles.get(0));
            return availableBundles.get(0);
        }

        // If there's a version that's in the same bundle as the creator, use it.
        logger.debug("There are {} available Bundles for Controller Service of type {}", availableBundles.size(), implementationClassName);
        final Optional<Bundle> sameBundleMatch = availableBundles.stream()
                .filter(bundle -> bundle.getBundleDetails().getCoordinate().equals(creator.getBundleCoordinate()))
                .findFirst();

        if (sameBundleMatch.isPresent()) {
            logger.debug("Found one Bundle that contains the Controller Service implementation {} that also contains the creator ({}). Will use it: {}",
                    implementationClassName, creator, sameBundleMatch.get());
            return sameBundleMatch.get();
        }

        // If there's a version that is the same as the creator's version, use it.
        final List<Bundle> sameVersionBundleMatch = availableBundles.stream()
                .filter(bundle -> bundle.getBundleDetails().getCoordinate().getVersion().equals(creator.getBundleCoordinate().getVersion()))
                .toList();

        if (sameVersionBundleMatch.size() == 1) {
            logger.debug("Found one Bundle that contains the Controller Service implementation {} that also contains the same version as the creator ({}). Will use it: {}",
                    implementationClassName, creator, sameVersionBundleMatch.get(0));
            return sameVersionBundleMatch.get(0);
        }

        // If there's a version that is the same as the framework version, use it.
        final Bundle frameworkBundle = getFrameworkBundle();
        final String frameworkVersion = frameworkBundle.getBundleDetails().getCoordinate().getVersion();
        final Optional<Bundle> sameVersionAsFrameworkMatch = availableBundles.stream()
                .filter(bundle -> bundle.getBundleDetails().getCoordinate().getVersion().equals(frameworkVersion))
                .findFirst();

        if (sameVersionAsFrameworkMatch.isPresent()) {
            logger.debug("Found one Bundle that contains the Controller Service implementation {} that also contains the same version as the NiFi Framework. Will use it: {}",
                    implementationClassName, sameVersionAsFrameworkMatch.get());
            return sameVersionAsFrameworkMatch.get();
        }

        // Unable to determine which version to use. Throw an Exception.
        logger.debug("Could not find a suitable Bundle for creating Controller Service implementation {} from creator {}", implementationClassName, creator);
        throw new IllegalArgumentException(String.format("There are %s versions of the %s Controller Service, but the appropriate version could not be resolved " +
                "from extension %s that is attempting to create the Controller Service", availableBundles.size(), implementationClassName, creator));
    }

    // Visible for testing
    protected Bundle getFrameworkBundle() {
        return NarClassLoadersHolder.getInstance().getFrameworkBundle();
    }
}
