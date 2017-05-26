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
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
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
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

    // Maps a service definition (interface) to those classes that implement the interface
    private static final Map<Class, Set<Class>> definitionMap = new HashMap<>();

    private static final Map<String, List<Bundle>> classNameBundleLookup = new HashMap<>();
    private static final Map<BundleCoordinate, Bundle> bundleCoordinateBundleLookup = new HashMap<>();
    private static final Map<ClassLoader, Bundle> classLoaderBundleLookup = new HashMap<>();
    private static final Map<String, ConfigurableComponent> tempComponentLookup = new HashMap<>();

    private static final Map<String, Class<?>> requiresInstanceClassLoading = new HashMap<>();
    private static final Map<String, InstanceClassLoader> instanceClassloaderLookup = new ConcurrentHashMap<>();

    static {
        definitionMap.put(Processor.class, new HashSet<>());
        definitionMap.put(FlowFilePrioritizer.class, new HashSet<>());
        definitionMap.put(ReportingTask.class, new HashSet<>());
        definitionMap.put(ControllerService.class, new HashSet<>());
        definitionMap.put(Authorizer.class, new HashSet<>());
        definitionMap.put(UserGroupProvider.class, new HashSet<>());
        definitionMap.put(AccessPolicyProvider.class, new HashSet<>());
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
                // create a cache of temp ConfigurableComponent instances, the initialize here has to happen before the checks below
                if ((isControllerService || isProcessor || isReportingTask) && o instanceof ConfigurableComponent) {
                    final ConfigurableComponent configurableComponent = (ConfigurableComponent) o;
                    initializeTempComponent(configurableComponent);

                    final String cacheKey = getClassBundleKey(o.getClass().getCanonicalName(), bundle.getBundleDetails().getCoordinate());
                    tempComponentLookup.put(cacheKey, (ConfigurableComponent)o);
                }

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

    private static void initializeTempComponent(final ConfigurableComponent configurableComponent) {
        ConfigurableComponentInitializer initializer = null;
        try {
            initializer = ConfigurableComponentInitializerFactory.createComponentInitializer(configurableComponent.getClass());
            initializer.initialize(configurableComponent);
        } catch (final InitializationException e) {
            logger.warn(String.format("Unable to initialize component %s due to %s", configurableComponent.getClass().getName(), e.getMessage()));
        }
    }

    private static boolean checkControllerServiceReferenceEligibility(final ConfigurableComponent component, final ClassLoader classLoader) {
        // if the extension does not require instance classloading, its eligible
        final boolean requiresInstanceClassLoading = component.getClass().isAnnotationPresent(RequiresInstanceClassLoading.class);

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
                final String cacheKey = getClassBundleKey(className, bundle.getBundleDetails().getCoordinate());
                requiresInstanceClassLoading.put(cacheKey, type);
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
     * @param additionalUrls additional URLs to add to the instance class loader
     * @return the ClassLoader for the given instance of the given type, or null if the type is not a detected extension type
     */
    public static InstanceClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle, final Set<URL> additionalUrls) {
        if (StringUtils.isEmpty(classType)) {
            throw new IllegalArgumentException("Class-Type is required");
        }

        if (StringUtils.isEmpty(instanceIdentifier)) {
            throw new IllegalArgumentException("Instance Identifier is required");
        }

        if (bundle == null) {
            throw new IllegalArgumentException("Bundle is required");
        }

        // If the class is annotated with @RequiresInstanceClassLoading and the registered ClassLoader is a URLClassLoader
        // then make a new InstanceClassLoader that is a full copy of the NAR Class Loader, otherwise create an empty
        // InstanceClassLoader that has the NAR ClassLoader as a parent

        InstanceClassLoader instanceClassLoader;
        final ClassLoader bundleClassLoader = bundle.getClassLoader();
        final String key = getClassBundleKey(classType, bundle.getBundleDetails().getCoordinate());

        if (requiresInstanceClassLoading.containsKey(key) && bundleClassLoader instanceof NarClassLoader) {
            final Class<?> type = requiresInstanceClassLoading.get(key);
            final RequiresInstanceClassLoading requiresInstanceClassLoading = type.getAnnotation(RequiresInstanceClassLoading.class);

            final NarClassLoader narBundleClassLoader = (NarClassLoader) bundleClassLoader;
            logger.debug("Including ClassLoader resources from {} for component {}", new Object[] {bundle.getBundleDetails(), instanceIdentifier});

            final Set<URL> instanceUrls = new LinkedHashSet<>();
            for (final URL url : narBundleClassLoader.getURLs()) {
                instanceUrls.add(url);
            }

            ClassLoader ancestorClassLoader = narBundleClassLoader.getParent();

            if (requiresInstanceClassLoading.cloneAncestorResources()) {
                final ConfigurableComponent component = getTempComponent(classType, bundle.getBundleDetails().getCoordinate());
                final Set<BundleCoordinate> reachableApiBundles = findReachableApiBundles(component);

                while (ancestorClassLoader != null && ancestorClassLoader instanceof NarClassLoader) {
                    final Bundle ancestorNarBundle = classLoaderBundleLookup.get(ancestorClassLoader);

                    // stop including ancestor resources when we reach one of the APIs, or when we hit the Jetty NAR
                    if (ancestorNarBundle == null || reachableApiBundles.contains(ancestorNarBundle.getBundleDetails().getCoordinate())
                            || ancestorNarBundle.getBundleDetails().getCoordinate().getId().equals(NarClassLoaders.JETTY_NAR_ID)) {
                        break;
                    }

                    final NarClassLoader ancestorNarClassLoader = (NarClassLoader) ancestorClassLoader;
                    for (final URL url : ancestorNarClassLoader.getURLs()) {
                        instanceUrls.add(url);
                    }
                    ancestorClassLoader = ancestorNarClassLoader.getParent();
                }
            }

            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, instanceUrls, additionalUrls, ancestorClassLoader);
        } else {
            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, Collections.emptySet(), additionalUrls, bundleClassLoader);
        }

        if (logger.isTraceEnabled()) {
            for (URL url : instanceClassLoader.getURLs()) {
                logger.trace("URL resource {} for {}...", new Object[]{url.toExternalForm(), instanceIdentifier});
            }
        }

        instanceClassloaderLookup.put(instanceIdentifier, instanceClassLoader);
        return instanceClassLoader;
    }

    /**
     * Find the bundle coordinates for any service APIs that are referenced by this component and not part of the same bundle.
     *
     * @param component the component being instantiated
     */
    protected static Set<BundleCoordinate> findReachableApiBundles(final ConfigurableComponent component) {
        final Set<BundleCoordinate> reachableApiBundles = new HashSet<>();

        try (final NarCloseable closeable = NarCloseable.withComponentNarLoader(component.getClass().getClassLoader())) {
            final List<PropertyDescriptor> descriptors = component.getPropertyDescriptors();
            if (descriptors != null && !descriptors.isEmpty()) {
                for (final PropertyDescriptor descriptor : descriptors) {
                    final Class<? extends ControllerService> serviceApi = descriptor.getControllerServiceDefinition();
                    if (serviceApi != null && !component.getClass().getClassLoader().equals(serviceApi.getClassLoader())) {
                        final Bundle apiBundle = classLoaderBundleLookup.get(serviceApi.getClassLoader());
                        reachableApiBundles.add(apiBundle.getBundleDetails().getCoordinate());
                    }
                }
            }
        }

        return reachableApiBundles;
    }

    /**
     * Retrieves the InstanceClassLoader for the component with the given identifier.
     *
     * @param instanceIdentifier the identifier of a component
     * @return the instance class loader for the component
     */
    public static InstanceClassLoader getInstanceClassLoader(final String instanceIdentifier) {
        return instanceClassloaderLookup.get(instanceIdentifier);
    }

    /**
     * Removes the InstanceClassLoader for a given component.
     *
     * @param instanceIdentifier the of a component
     */
    public static InstanceClassLoader removeInstanceClassLoader(final String instanceIdentifier) {
        if (instanceIdentifier == null) {
            return null;
        }

        final InstanceClassLoader classLoader = instanceClassloaderLookup.remove(instanceIdentifier);
        closeURLClassLoader(instanceIdentifier, classLoader);
        return classLoader;
    }

    /**
     * Closes the given ClassLoader if it is an instance of URLClassLoader.
     *
     * @param instanceIdentifier the instance id the class loader corresponds to
     * @param classLoader the class loader to close
     */
    public static void closeURLClassLoader(final String instanceIdentifier, final ClassLoader classLoader) {
        if (classLoader != null && (classLoader instanceof URLClassLoader)) {
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            try {
                urlClassLoader.close();
            } catch (IOException e) {
                logger.warn("Unable to close URLClassLoader for " + instanceIdentifier);
            }
        }
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

    public static ConfigurableComponent getTempComponent(final String classType, final BundleCoordinate bundleCoordinate) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }

        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("Bundle Coordinate cannot be null");
        }

        return tempComponentLookup.get(getClassBundleKey(classType, bundleCoordinate));
    }

    private static String getClassBundleKey(final String classType, final BundleCoordinate bundleCoordinate) {
        return classType + "_" + bundleCoordinate.getCoordinate();
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
