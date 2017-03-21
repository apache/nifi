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

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.init.ConfigurableComponentInitializer;
import org.apache.nifi.init.ConfigurableComponentInitializerFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Scans through the classpath to load all FlowFileProcessors, FlowFileComparators, and ReportingTasks using the service provider API and running through all classloaders (root, NARs).
 *
 * @ThreadSafe - is immutable
 */
@SuppressWarnings("rawtypes")
public class ExtensionManager {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionManager.class);

    public static final BundleCoordinate SYSTEM_BUNDLE_COORDINATE = new BundleCoordinate(
            BundleCoordinate.DEFAULT_GROUP, "system", BundleCoordinate.DEFAULT_VERSION);

    // Maps a service definition (interface) to those classes that implement the interface
    private static final Map<Class, Set<Class>> definitionMap = new HashMap<>();

    private static final Map<String, List<Bundle>> classNameBundleLookup = new HashMap<>();
    private static final Map<BundleCoordinate, Bundle> bundleCoordinateBundleLookup = new HashMap<>();
    private static final Map<ClassLoader, Bundle> classLoaderBundleLookup = new HashMap<>();

    private static final Set<String> requiresInstanceClassLoading = new HashSet<>();
    private static final Map<String, ClassLoader> instanceClassloaderLookup = new ConcurrentHashMap<>();

    static {
        definitionMap.put(Processor.class, new HashSet<>());
        definitionMap.put(FlowFilePrioritizer.class, new HashSet<>());
        definitionMap.put(ReportingTask.class, new HashSet<>());
        definitionMap.put(ControllerService.class, new HashSet<>());
        definitionMap.put(Authorizer.class, new HashSet<>());
        definitionMap.put(LoginIdentityProvider.class, new HashSet<>());
        definitionMap.put(ProvenanceRepository.class, new HashSet<>());
        definitionMap.put(ComponentStatusRepository.class, new HashSet<>());
        definitionMap.put(FlowFileRepository.class, new HashSet<>());
        definitionMap.put(FlowFileSwapManager.class, new HashSet<>());
        definitionMap.put(ContentRepository.class, new HashSet<>());
        definitionMap.put(StateProvider.class, new HashSet<>());
    }

    /**
     * Loads all FlowFileProcessor, FlowFileComparator, ReportingTask class types that can be found on the bootstrap classloader and by creating classloaders for all NARs found within the classpath.
     * @param narBundles the bundles to scan through in search of extensions
     */
    public static void discoverExtensions(final Bundle systemBundle, final Set<Bundle> narBundles) {
        // get the current context class loader
        ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();

        // load the system bundle first so that any extensions found in JARs directly in lib will be registered as
        // being from the system bundle and not from all the other NARs
        loadExtensions(systemBundle);
        bundleCoordinateBundleLookup.put(systemBundle.getBundleDetails().getCoordinate(), systemBundle);

        // consider each nar class loader
        for (final Bundle bundle : narBundles) {
            // Must set the context class loader to the nar classloader itself
            // so that static initialization techniques that depend on the context class loader will work properly
            final ClassLoader ncl = bundle.getClassLoader();
            Thread.currentThread().setContextClassLoader(ncl);
            loadExtensions(bundle);

            // Create a look-up from coordinate to bundle
            bundleCoordinateBundleLookup.put(bundle.getBundleDetails().getCoordinate(), bundle);
        }

        // restore the current context class loader if appropriate
        if (currentContextClassLoader != null) {
            Thread.currentThread().setContextClassLoader(currentContextClassLoader);
        }
    }

    /**
     * Returns a bundle representing the system class loader.
     *
     * @param niFiProperties a NiFiProperties instance which will be used to obtain the default NAR library path,
     *                       which will become the working directory of the returned bundle
     * @return a bundle for the system class loader
     */
    public static Bundle createSystemBundle(final NiFiProperties niFiProperties) {
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

        final String narLibraryDirectory = niFiProperties.getProperty(NiFiProperties.NAR_LIBRARY_DIRECTORY);
        if (StringUtils.isBlank(narLibraryDirectory)) {
            throw new IllegalStateException("Unable to create system bundle because " + NiFiProperties.NAR_LIBRARY_DIRECTORY + " was null or empty");
        }

        final BundleDetails systemBundleDetails = new BundleDetails.Builder()
                .workingDir(new File(narLibraryDirectory))
                .coordinate(SYSTEM_BUNDLE_COORDINATE)
                .build();

        return new Bundle(systemBundleDetails, systemClassLoader);
    }

    /**
     * Loads extensions from the specified bundle.
     *
     * @param bundle from which to load extensions
     */
    @SuppressWarnings("unchecked")
    private static void loadExtensions(final Bundle bundle) {
        for (final Map.Entry<Class, Set<Class>> entry : definitionMap.entrySet()) {
            final boolean isControllerService = ControllerService.class.equals(entry.getKey());
            final boolean isProcessor = Processor.class.equals(entry.getKey());
            final boolean isReportingTask = ReportingTask.class.equals(entry.getKey());

            final ServiceLoader<?> serviceLoader = ServiceLoader.load(entry.getKey(), bundle.getClassLoader());
            for (final Object o : serviceLoader) {
                // only consider extensions discovered directly in this bundle
                boolean registerExtension = bundle.getClassLoader().equals(o.getClass().getClassLoader());

                if (registerExtension) {
                    final Class extensionType = o.getClass();
                    if (isControllerService && !checkControllerServiceEligibility(extensionType)) {
                        registerExtension = false;
                        logger.error(String.format(
                                "Skipping Controller Service %s because it is bundled with its supporting APIs and requires instance class loading.", extensionType.getName()));
                    }

                    final boolean canReferenceControllerService = (isControllerService || isProcessor || isReportingTask) && o instanceof ConfigurableComponent;
                    if (canReferenceControllerService && !checkControllerServiceReferenceEligibility((ConfigurableComponent) o, bundle.getClassLoader())) {
                        registerExtension = false;
                        logger.error(String.format(
                                "Skipping component %s because it is bundled with its referenced Controller Service APIs and requires instance class loading.", extensionType.getName()));
                    }

                    if (registerExtension) {
                        registerServiceClass(o.getClass(), classNameBundleLookup, bundle, entry.getValue());
                    }
                }
            }

            classLoaderBundleLookup.put(bundle.getClassLoader(), bundle);
        }
    }

    private static boolean checkControllerServiceReferenceEligibility(final ConfigurableComponent component, final ClassLoader classLoader) {
        // if the extension does not require instance classloading, its eligible
        final boolean requiresInstanceClassLoading = component.getClass().isAnnotationPresent(RequiresInstanceClassLoading.class);

        ConfigurableComponentInitializer initializer = null;
        try {
            initializer = ConfigurableComponentInitializerFactory.createComponentInitializer(component.getClass());
            initializer.initialize(component);

            final Set<Class> cobundledApis = new HashSet<>();
            try (final NarCloseable closeable = NarCloseable.withComponentNarLoader(component.getClass().getClassLoader())) {
                final List<PropertyDescriptor> descriptors = component.getPropertyDescriptors();
                if (descriptors != null && !descriptors.isEmpty()) {
                    for (final PropertyDescriptor descriptor : descriptors) {
                        final Class<? extends ControllerService> serviceApi = descriptor.getControllerServiceDefinition();
                        if (serviceApi != null && classLoader.equals(serviceApi.getClassLoader())) {
                            cobundledApis.add(serviceApi);
                        }
                    }
                }
            }

            if (!cobundledApis.isEmpty()) {
                logger.warn(String.format(
                        "Component %s is bundled with its referenced Controller Service APIs %s. The service APIs should not be bundled with component implementations that reference it.",
                        component.getClass().getName(), StringUtils.join(cobundledApis.stream().map(cls -> cls.getName()).collect(Collectors.toSet()), ", ")));
            }

            // the component is eligible when it does not require instance classloading or when the supporting APIs are bundled in a parent NAR
            return requiresInstanceClassLoading == false || cobundledApis.isEmpty();
        } catch (final InitializationException e) {
            logger.warn(String.format("Unable to verify if component %s references any bundled Controller Service APIs due to %s", component.getClass().getName(), e.getMessage()));
            return true;
        } finally {
            if (initializer != null) {
                initializer.teardown(component);
            }
        }
    }

    private static boolean checkControllerServiceEligibility(Class extensionType) {
        final Class originalExtensionType = extensionType;
        final ClassLoader originalExtensionClassLoader = extensionType.getClassLoader();

        // if the extension does not require instance classloading, its eligible
        final boolean requiresInstanceClassLoading = extensionType.isAnnotationPresent(RequiresInstanceClassLoading.class);

        final Set<Class> cobundledApis = new HashSet<>();
        while (extensionType != null) {
            for (final Class i : extensionType.getInterfaces()) {
                if (originalExtensionClassLoader.equals(i.getClassLoader())) {
                    cobundledApis.add(i);
                }
            }

            extensionType = extensionType.getSuperclass();
        }

        if (!cobundledApis.isEmpty()) {
            logger.warn(String.format("Controller Service %s is bundled with its supporting APIs %s. The service APIs should not be bundled with the implementations.",
                    originalExtensionType.getName(), StringUtils.join(cobundledApis.stream().map(cls -> cls.getName()).collect(Collectors.toSet()), ", ")));
        }

        // the service is eligible when it does not require instance classloading or when the supporting APIs are bundled in a parent NAR
        return requiresInstanceClassLoading == false || cobundledApis.isEmpty();
    }

    /**
     * Registers extension for the specified type from the specified Bundle.
     *
     * @param type the extension type
     * @param classNameBundleMap mapping of classname to Bundle
     * @param bundle the Bundle being mapped to
     * @param classes to map to this classloader but which come from its ancestors
     */
    private static void registerServiceClass(final Class<?> type, final Map<String, List<Bundle>> classNameBundleMap, final Bundle bundle, final Set<Class> classes) {
        final String className = type.getName();

        // get the bundles that have already been registered for the class name
        List<Bundle> registeredBundles = classNameBundleMap.get(className);

        if (registeredBundles == null) {
            registeredBundles = new ArrayList<>();
            classNameBundleMap.put(className, registeredBundles);
        }

        boolean alreadyRegistered = false;
        for (final Bundle registeredBundle : registeredBundles) {
            final BundleCoordinate registeredCoordinate = registeredBundle.getBundleDetails().getCoordinate();

            // if the incoming bundle has the same coordinate as one of the registered bundles then consider it already registered
            if (registeredCoordinate.equals(bundle.getBundleDetails().getCoordinate())) {
                alreadyRegistered = true;
                break;
            }

            // if the type wasn't loaded from an ancestor, and the type isn't a processor, cs, or reporting task, then
            // fail registration because we don't support multiple versions of any other types
            if (!multipleVersionsAllowed(type)) {
                throw new IllegalStateException("Attempt was made to load " + className + " from "
                        + bundle.getBundleDetails().getCoordinate().getCoordinate()
                        + " but that class name is already loaded/registered from " + registeredBundle.getBundleDetails().getCoordinate()
                        + " and multiple versions are not supported for this type"
                );
            }
        }

        // if none of the above was true then register the new bundle
        if (!alreadyRegistered) {
            registeredBundles.add(bundle);
            classes.add(type);

            if (type.isAnnotationPresent(RequiresInstanceClassLoading.class)) {
                requiresInstanceClassLoading.add(className);
            }
        }

    }

    /**
     * @param type a Class that we found from a service loader
     * @return true if the given class is a processor, controller service, or reporting task
     */
    private static boolean multipleVersionsAllowed(Class<?> type) {
        return Processor.class.isAssignableFrom(type) || ControllerService.class.isAssignableFrom(type) || ReportingTask.class.isAssignableFrom(type);
    }

    /**
     * Determines the effective ClassLoader for the instance of the given type.
     *
     * @param classType the type of class to lookup the ClassLoader for
     * @param instanceIdentifier the identifier of the specific instance of the classType to look up the ClassLoader for
     * @param bundle the bundle where the classType exists
     * @return the ClassLoader for the given instance of the given type, or null if the type is not a detected extension type
     */
    public static ClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle) {
        if (StringUtils.isEmpty(classType)) {
            throw new IllegalArgumentException("Class-Type is required");
        }

        if (StringUtils.isEmpty(instanceIdentifier)) {
            throw new IllegalArgumentException("Instance Identifier is required");
        }

        if (bundle == null) {
            throw new IllegalArgumentException("Bundle is required");
        }

        final ClassLoader bundleClassLoader = bundle.getClassLoader();

        // If the class is annotated with @RequiresInstanceClassLoading and the registered ClassLoader is a URLClassLoader
        // then make a new InstanceClassLoader that is a full copy of the NAR Class Loader, otherwise create an empty
        // InstanceClassLoader that has the NAR ClassLoader as a parent
        ClassLoader instanceClassLoader;
        if (requiresInstanceClassLoading.contains(classType) && (bundleClassLoader instanceof URLClassLoader)) {
            final URLClassLoader registeredUrlClassLoader = (URLClassLoader) bundleClassLoader;
            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, registeredUrlClassLoader.getURLs(), registeredUrlClassLoader.getParent());
        } else {
            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, new URL[0], bundleClassLoader);
        }

        instanceClassloaderLookup.put(instanceIdentifier, instanceClassLoader);
        return instanceClassLoader;
    }

    /**
     * Retrieves the InstanceClassLoader for the component with the given identifier.
     *
     * @param instanceIdentifier the identifier of a component
     * @return the instance class loader for the component
     */
    public static ClassLoader getInstanceClassLoader(final String instanceIdentifier) {
        return instanceClassloaderLookup.get(instanceIdentifier);
    }

    /**
     * Removes the ClassLoader for the given instance and closes it if necessary.
     *
     * @param instanceIdentifier the identifier of a component to remove the ClassLoader for
     * @return the removed ClassLoader for the given instance, or null if not found
     */
    public static ClassLoader removeInstanceClassLoaderIfExists(final String instanceIdentifier) {
        if (instanceIdentifier == null) {
            return null;
        }

        final ClassLoader classLoader = instanceClassloaderLookup.remove(instanceIdentifier);
        if (classLoader != null && (classLoader instanceof URLClassLoader)) {
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            try {
                urlClassLoader.close();
            } catch (IOException e) {
                logger.warn("Unable to class URLClassLoader for " + instanceIdentifier);
            }
        }
        return classLoader;
    }

    /**
     * Checks if the given class type requires per-instance class loading (i.e. contains the @RequiresInstanceClassLoading annotation)
     *
     * @param classType the class to check
     * @return true if the class is found in the set of classes requiring instance level class loading, false otherwise
     */
    public static boolean requiresInstanceClassLoading(final String classType) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }
        return requiresInstanceClassLoading.contains(classType);
    }

    /**
     * Retrieves the bundles that have a class with the given name.
     *
     * @param classType the class name of an extension
     * @return the list of bundles that contain an extension with the given class name
     */
    public static List<Bundle> getBundles(final String classType) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }
        final List<Bundle> bundles = classNameBundleLookup.get(classType);
        return bundles == null ? Collections.emptyList() : new ArrayList<>(bundles);
    }

    /**
     * Retrieves the bundle with the given coordinate.
     *
     * @param bundleCoordinate a coordinate to look up
     * @return the bundle with the given coordinate, or null if none exists
     */
    public static Bundle getBundle(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        return bundleCoordinateBundleLookup.get(bundleCoordinate);
    }

    /**
     * Retrieves the bundle for the given class loader.
     *
     * @param classLoader the class loader to look up the bundle for
     * @return the bundle for the given class loader
     */
    public static Bundle getBundle(final ClassLoader classLoader) {
        if (classLoader == null) {
            throw new IllegalArgumentException("ClassLoader cannot be null");
        }
        return classLoaderBundleLookup.get(classLoader);
    }

    public static Set<Class> getExtensions(final Class<?> definition) {
        if (definition == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        final Set<Class> extensions = definitionMap.get(definition);
        return (extensions == null) ? Collections.<Class>emptySet() : extensions;
    }

    public static void logClassLoaderMapping() {
        final StringBuilder builder = new StringBuilder();

        builder.append("Extension Type Mapping to Bundle:");
        for (final Map.Entry<Class, Set<Class>> entry : definitionMap.entrySet()) {
            builder.append("\n\t=== ").append(entry.getKey().getSimpleName()).append(" Type ===");

            for (final Class type : entry.getValue()) {
                final List<Bundle> bundles = classNameBundleLookup.containsKey(type.getName())
                        ? classNameBundleLookup.get(type.getName()) : Collections.emptyList();

                builder.append("\n\t").append(type.getName());

                for (final Bundle bundle : bundles) {
                    final String coordinate = bundle.getBundleDetails().getCoordinate().getCoordinate();
                    final String workingDir = bundle.getBundleDetails().getWorkingDirectory().getPath();
                    builder.append("\n\t\t").append(coordinate).append(" || ").append(workingDir);
                }
            }

            builder.append("\n\t=== End ").append(entry.getKey().getSimpleName()).append(" types ===");
        }

        logger.info(builder.toString());
    }
}
