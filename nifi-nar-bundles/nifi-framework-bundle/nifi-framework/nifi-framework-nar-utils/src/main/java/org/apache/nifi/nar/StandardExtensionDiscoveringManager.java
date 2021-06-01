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
import org.apache.nifi.controller.status.analytics.StatusAnalyticsModel;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Scans through the classpath to load all FlowFileProcessors, FlowFileComparators, and ReportingTasks using the service provider API and running through all classloaders (root, NARs).
 *
 * @ThreadSafe - is immutable
 */
@SuppressWarnings("rawtypes")
public class StandardExtensionDiscoveringManager implements ExtensionDiscoveringManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardExtensionDiscoveringManager.class);

    // Maps a service definition (interface) to those classes that implement the interface
    private final Map<Class, Set<ExtensionDefinition>> definitionMap = new HashMap<>();

    private final Map<String, List<Bundle>> classNameBundleLookup = new HashMap<>();
    private final Map<BundleCoordinate, Set<ExtensionDefinition>> bundleCoordinateClassesLookup = new HashMap<>();
    private final Map<BundleCoordinate, Bundle> bundleCoordinateBundleLookup = new HashMap<>();
    private final Map<ClassLoader, Bundle> classLoaderBundleLookup = new HashMap<>();
    private final Map<String, ConfigurableComponent> tempComponentLookup = new HashMap<>();

    private final Map<String, InstanceClassLoader> instanceClassloaderLookup = new ConcurrentHashMap<>();

    public StandardExtensionDiscoveringManager() {
        this(Collections.emptyList());
    }

    public StandardExtensionDiscoveringManager(final Collection<Class<? extends ConfigurableComponent>> additionalExtensionTypes) {
        definitionMap.put(Processor.class, new HashSet<>());
        definitionMap.put(FlowFilePrioritizer.class, new HashSet<>());
        definitionMap.put(ReportingTask.class, new HashSet<>());
        definitionMap.put(ControllerService.class, new HashSet<>());
        definitionMap.put(Authorizer.class, new HashSet<>());
        definitionMap.put(UserGroupProvider.class, new HashSet<>());
        definitionMap.put(AccessPolicyProvider.class, new HashSet<>());
        definitionMap.put(LoginIdentityProvider.class, new HashSet<>());
        definitionMap.put(ProvenanceRepository.class, new HashSet<>());
        definitionMap.put(StatusHistoryRepository.class, new HashSet<>());
        definitionMap.put(FlowFileRepository.class, new HashSet<>());
        definitionMap.put(FlowFileSwapManager.class, new HashSet<>());
        definitionMap.put(ContentRepository.class, new HashSet<>());
        definitionMap.put(StateProvider.class, new HashSet<>());
        definitionMap.put(StatusAnalyticsModel.class, new HashSet<>());
        definitionMap.put(NarProvider.class, new HashSet<>());

        additionalExtensionTypes.forEach(type -> definitionMap.putIfAbsent(type, new HashSet<>()));
    }

    @Override
    public Set<Bundle> getAllBundles() {
        return classNameBundleLookup.values().stream()
            .flatMap(List::stream)
            .collect(Collectors.toSet());
    }

    @Override
    public void discoverExtensions(final Bundle systemBundle, final Set<Bundle> narBundles) {
        // load the system bundle first so that any extensions found in JARs directly in lib will be registered as
        // being from the system bundle and not from all the other NARs
        loadExtensions(systemBundle);
        bundleCoordinateBundleLookup.put(systemBundle.getBundleDetails().getCoordinate(), systemBundle);

        discoverExtensions(narBundles);
    }

    @Override
    public void discoverExtensions(final Set<Bundle> narBundles) {
        // get the current context class loader
        ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();

        // consider each nar class loader
        for (final Bundle bundle : narBundles) {
            // Must set the context class loader to the nar classloader itself
            // so that static initialization techniques that depend on the context class loader will work properly
            final ClassLoader ncl = bundle.getClassLoader();
            Thread.currentThread().setContextClassLoader(ncl);

            final long loadStart = System.currentTimeMillis();
            loadExtensions(bundle);
            final long loadMillis = System.currentTimeMillis() - loadStart;
            logger.info("Loaded extensions for {} in {} millis", bundle.getBundleDetails(), loadMillis);

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
    private void loadExtensions(final Bundle bundle) {
        for (final Class extensionType : definitionMap.keySet()) {
            final String serviceType = extensionType.getName();

            try {
                final Set<URL> serviceResourceUrls = getServiceFileURLs(bundle, extensionType);
                logger.debug("Bundle {} has the following Services File URLs for {}: {}", bundle, serviceType, serviceResourceUrls);

                for (final URL serviceResourceUrl : serviceResourceUrls) {
                    final Set<String> implementationClassNames = getServiceFileImplementationClassNames(serviceResourceUrl);
                    logger.debug("Bundle {} defines {} implementations of interface {}", bundle, implementationClassNames.size(), serviceType);

                    for (final String implementationClassName : implementationClassNames) {
                        try {
                            loadExtension(implementationClassName, extensionType, bundle);
                            logger.debug("Successfully loaded {} {} from {}", extensionType.getSimpleName(), implementationClassName, bundle);
                        } catch (final Exception e) {
                            logger.error("Failed to register {} of type {} in bundle {}" , extensionType.getSimpleName(), implementationClassName, bundle, e);
                        }
                    }
                }
            } catch (final IOException e) {
                throw new RuntimeException("Failed to get resources of type " + serviceType + " from bundle " + bundle);
            }
        }

        classLoaderBundleLookup.put(bundle.getClassLoader(), bundle);
    }

    private Set<String> getServiceFileImplementationClassNames(final URL serviceFileUrl) throws IOException {
        final Set<String> implementationClassNames = new HashSet<>();

        try (final InputStream in = serviceFileUrl.openStream();
             final Reader inputStreamReader = new InputStreamReader(in);
             final BufferedReader reader = new BufferedReader(inputStreamReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                // Remove anything after the #
                final int index = line.indexOf("#");
                if (index >= 0) {
                    line = line.substring(0, index);
                }

                // Ignore empty line
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                implementationClassNames.add(line);
            }
        }

        return implementationClassNames;
    }

    /**
     * Returns a Set of URL's for all Service Files (i.e., META-INF/services/&lt;interface name&gt; files)
     * that define the extensions that exist for the given bundle. The returned set will only contain URL's for
     * which the services file live in the given bundle directly and NOT the parent/ancestor bundle.
     *
     * @param bundle the bundle whose extensions are of interest
     * @param extensionType the type of extension (I.e., Processor, ControllerService, ReportingTask, etc.)
     * @return the set of URL's that point to Service Files for the given extension type in the given bundle. An empty set will be
     * returned if no service files exist
     *
     * @throws IOException if unable to read the services file from the given bundle's classloader.
     */
    private Set<URL> getServiceFileURLs(final Bundle bundle, final Class<?> extensionType) throws IOException {
        final String servicesFile = "META-INF/services/" + extensionType.getName();

        final Enumeration<URL> serviceResourceUrlEnum = bundle.getClassLoader().getResources(servicesFile);
        final Set<URL> serviceResourceUrls = new HashSet<>();
        while (serviceResourceUrlEnum.hasMoreElements()) {
            serviceResourceUrls.add(serviceResourceUrlEnum.nextElement());
        }

        final Enumeration<URL> parentResourceUrlEnum = bundle.getClassLoader().getParent().getResources(servicesFile);
        while (parentResourceUrlEnum.hasMoreElements()) {
            serviceResourceUrls.remove(parentResourceUrlEnum.nextElement());
        }

        return serviceResourceUrls;
    }

    protected void loadExtension(final String extensionClassName, final Class<?> extensionType, final Bundle bundle) {
        registerExtensionClass(extensionType, extensionClassName, bundle);
    }

    protected void registerExtensionClass(final Class<?> extensionType, final String implementationClassName, final Bundle bundle) {
        final Set<ExtensionDefinition> registeredClasses = definitionMap.get(extensionType);
        registerServiceClass(implementationClassName, extensionType, classNameBundleLookup, bundleCoordinateClassesLookup, bundle, registeredClasses);
    }


    protected void initializeTempComponent(final ConfigurableComponent configurableComponent) {
        try {
            final ConfigurableComponentInitializer initializer = ConfigurableComponentInitializerFactory.createComponentInitializer(this, configurableComponent.getClass());
            if (initializer != null) {
                initializer.initialize(configurableComponent);
            }
        } catch (final InitializationException e) {
            logger.warn(String.format("Unable to initialize component %s due to %s", configurableComponent.getClass().getName(), e.getMessage()));
        }
    }

    protected void addTempComponent(final ConfigurableComponent instance, final BundleCoordinate coordinate) {
        final String cacheKey = getClassBundleKey(instance.getClass().getCanonicalName(), coordinate);
        tempComponentLookup.put(cacheKey, instance);
    }

    /**
     * Registers extension for the specified type from the specified Bundle.
     *
     * @param className the fully qualified class name of the extension implementation
     * @param classNameBundleMap mapping of classname to Bundle
     * @param bundle the Bundle being mapped to
     * @param classes to map to this classloader but which come from its ancestors
     */
    private void registerServiceClass(final String className, final Class<?> extensionType,
                                             final Map<String, List<Bundle>> classNameBundleMap,
                                             final Map<BundleCoordinate, Set<ExtensionDefinition>> bundleCoordinateClassesMap,
                                             final Bundle bundle, final Set<ExtensionDefinition> classes) {
        final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();

        // get the bundles that have already been registered for the class name
        final List<Bundle> registeredBundles = classNameBundleMap.computeIfAbsent(className, (key) -> new ArrayList<>());
        final Set<ExtensionDefinition> bundleExtensionDefinitions = bundleCoordinateClassesMap.computeIfAbsent(bundleCoordinate, (key) -> new HashSet<>());

        boolean alreadyRegistered = false;
        for (final Bundle registeredBundle : registeredBundles) {
            final BundleCoordinate registeredCoordinate = registeredBundle.getBundleDetails().getCoordinate();

            // if the incoming bundle has the same coordinate as one of the registered bundles then consider it already registered
            if (registeredCoordinate.equals(bundleCoordinate)) {
                alreadyRegistered = true;
                break;
            }

            // if the type wasn't loaded from an ancestor, and the type isn't a processor, cs, or reporting task, then
            // fail registration because we don't support multiple versions of any other types
            if (!multipleVersionsAllowed(extensionType)) {
                logger.debug("Attempt was made to load {} from {} but that class name is already loaded/registered from {} and multiple versions are not supported for this type",
                    className, bundle.getBundleDetails().getCoordinate().getCoordinate(), registeredBundle.getBundleDetails().getCoordinate());
                return;
            }
        }

        // if none of the above was true then register the new bundle
        if (!alreadyRegistered) {
            registeredBundles.add(bundle);

            final ExtensionDefinition extensionDefinition = new ExtensionDefinition(className, bundle, extensionType);
            bundleExtensionDefinitions.add(extensionDefinition);
            classes.add(extensionDefinition);
        }
    }

    @Override
    public Class<?> getClass(final ExtensionDefinition extensionDefinition) {
        final Bundle bundle = extensionDefinition.getBundle();
        final ClassLoader bundleClassLoader = bundle.getClassLoader();

        try (final NarCloseable x = NarCloseable.withComponentNarLoader(bundleClassLoader)) {
            return Class.forName(extensionDefinition.getImplementationClassName(), true, bundleClassLoader);
        } catch (final Exception e) {
            throw new RuntimeException("Could not create Class for " + extensionDefinition, e);
        }
    }

    /**
     * @param type a Class that we found from a service loader
     * @return true if the given class is a processor, controller service, or reporting task
     */
    private static boolean multipleVersionsAllowed(Class<?> type) {
        return Processor.class.isAssignableFrom(type) || ControllerService.class.isAssignableFrom(type) || ReportingTask.class.isAssignableFrom(type);
    }

    protected boolean isInstanceClassLoaderRequired(final String classType, final Bundle bundle) {
        // We require instance Class Loaders if the component has the @RequiresInstanceClassLoader annotation and is loaded from the NAR ClassLoader.
        // So the first check is to see if the bundle's ClassLoader is a NarClassLoader.
        final ClassLoader bundleClassLoader = bundle.getClassLoader();
        if (!(bundleClassLoader instanceof NarClassLoader)) {
            return false;
        }

        final ConfigurableComponent tempComponent = getTempComponent(classType, bundle.getBundleDetails().getCoordinate());
        if (tempComponent == null) {
            return false;
        }

        return tempComponent.getClass().isAnnotationPresent(RequiresInstanceClassLoading.class);
    }

    @Override
    public InstanceClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle, final Set<URL> additionalUrls) {
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

        final boolean requiresInstanceClassLoader = isInstanceClassLoaderRequired(classType, bundle);
        if (requiresInstanceClassLoader) {
            final ConfigurableComponent tempComponent = getTempComponent(classType, bundle.getBundleDetails().getCoordinate());
            final Class<?> type = tempComponent.getClass();

            final RequiresInstanceClassLoading requiresInstanceClassLoading = type.getAnnotation(RequiresInstanceClassLoading.class);

            final NarClassLoader narBundleClassLoader = (NarClassLoader) bundleClassLoader;
            logger.debug("Including ClassLoader resources from {} for component {}", new Object[] {bundle.getBundleDetails(), instanceIdentifier});

            final Set<URL> instanceUrls = new LinkedHashSet<>();
            final Set<File> narNativeLibDirs = new LinkedHashSet<>();

            narNativeLibDirs.add(narBundleClassLoader.getNARNativeLibDir());
            instanceUrls.addAll(Arrays.asList(narBundleClassLoader.getURLs()));

            ClassLoader ancestorClassLoader = narBundleClassLoader.getParent();

            if (requiresInstanceClassLoading.cloneAncestorResources()) {
                final ConfigurableComponent component = getTempComponent(classType, bundle.getBundleDetails().getCoordinate());
                final Set<BundleCoordinate> reachableApiBundles = findReachableApiBundles(component);

                while (ancestorClassLoader instanceof NarClassLoader) {
                    final Bundle ancestorNarBundle = classLoaderBundleLookup.get(ancestorClassLoader);

                    // stop including ancestor resources when we reach one of the APIs, or when we hit the Jetty NAR
                    if (ancestorNarBundle == null || reachableApiBundles.contains(ancestorNarBundle.getBundleDetails().getCoordinate())
                            || ancestorNarBundle.getBundleDetails().getCoordinate().getId().equals(NarClassLoaders.JETTY_NAR_ID)) {
                        break;
                    }

                    final NarClassLoader ancestorNarClassLoader = (NarClassLoader) ancestorClassLoader;

                    narNativeLibDirs.add(ancestorNarClassLoader.getNARNativeLibDir());
                    Collections.addAll(instanceUrls, ancestorNarClassLoader.getURLs());

                    ancestorClassLoader = ancestorNarClassLoader.getParent();
                }
            }

            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, instanceUrls, additionalUrls, narNativeLibDirs, ancestorClassLoader);
        } else {
            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, Collections.emptySet(), additionalUrls, bundleClassLoader);
        }

        if (logger.isTraceEnabled()) {
            for (URL url : instanceClassLoader.getURLs()) {
                logger.trace("URL resource {} for {}...", new Object[] {url.toExternalForm(), instanceIdentifier});
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
    protected Set<BundleCoordinate> findReachableApiBundles(final ConfigurableComponent component) {
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

    @Override
    public InstanceClassLoader getInstanceClassLoader(final String instanceIdentifier) {
        return instanceClassloaderLookup.get(instanceIdentifier);
    }

    @Override
    public InstanceClassLoader removeInstanceClassLoader(final String instanceIdentifier) {
        if (instanceIdentifier == null) {
            return null;
        }

        final InstanceClassLoader classLoader = instanceClassloaderLookup.remove(instanceIdentifier);
        closeURLClassLoader(instanceIdentifier, classLoader);
        return classLoader;
    }

    @Override
    public void registerInstanceClassLoader(final String instanceIdentifier, final InstanceClassLoader instanceClassLoader) {
        instanceClassloaderLookup.putIfAbsent(instanceIdentifier, instanceClassLoader);
    }

    @Override
    public void closeURLClassLoader(final String instanceIdentifier, final ClassLoader classLoader) {
        if ((classLoader instanceof URLClassLoader)) {
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            try {
                urlClassLoader.close();
            } catch (IOException e) {
                logger.warn("Unable to close URLClassLoader for " + instanceIdentifier);
            }
        }
    }

    @Override
    public List<Bundle> getBundles(final String classType) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }

        final List<Bundle> bundles = classNameBundleLookup.get(classType);
        return bundles == null ? Collections.emptyList() : new ArrayList<>(bundles);
    }

    @Override
    public Bundle getBundle(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        return bundleCoordinateBundleLookup.get(bundleCoordinate);
    }

    @Override
    public Set<ExtensionDefinition> getTypes(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        final Set<ExtensionDefinition> types = bundleCoordinateClassesLookup.get(bundleCoordinate);
        return types == null ? Collections.emptySet() : Collections.unmodifiableSet(types);
    }

    @Override
    public Bundle getBundle(final ClassLoader classLoader) {
        if (classLoader == null) {
            throw new IllegalArgumentException("ClassLoader cannot be null");
        }
        return classLoaderBundleLookup.get(classLoader);
    }

    @Override
    public Set<ExtensionDefinition> getExtensions(final Class<?> definition) {
        if (definition == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        final Set<ExtensionDefinition> extensions = definitionMap.get(definition);
        return (extensions == null) ? Collections.emptySet() : extensions;
    }

    @Override
    public synchronized ConfigurableComponent getTempComponent(final String classType, final BundleCoordinate bundleCoordinate) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }

        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("Bundle Coordinate cannot be null");
        }

        final String bundleKey = getClassBundleKey(classType, bundleCoordinate);
        final ConfigurableComponent existing = tempComponentLookup.get(bundleKey);
        if (existing != null) {
            return existing;
        }

        final Bundle bundle = getBundle(bundleCoordinate);
        if (bundle == null) {
            logger.error("Could not instantiate class of type {} using ClassLoader for bundle {} because the bundle could not be found", classType, bundleCoordinate);
            return null;
        }

        final ClassLoader bundleClassLoader = bundle.getClassLoader();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(bundleClassLoader)) {
            final Class<?> componentClass = Class.forName(classType, true, bundleClassLoader);
            final ConfigurableComponent tempComponent = (ConfigurableComponent) componentClass.newInstance();
            initializeTempComponent(tempComponent);
            tempComponentLookup.put(bundleKey, tempComponent);
            return tempComponent;
        } catch (final Exception e) {
            logger.error("Could not instantiate class of type {} using ClassLoader for bundle {}", classType, bundleCoordinate, e);
            return null;
        }
    }

    private static String getClassBundleKey(final String classType, final BundleCoordinate bundleCoordinate) {
        return classType + "_" + bundleCoordinate.getCoordinate();
    }

    @Override
    public void logClassLoaderMapping() {
        final StringBuilder builder = new StringBuilder();

        builder.append("Extension Type Mapping to Bundle:");
        for (final Map.Entry<Class, Set<ExtensionDefinition>> entry : definitionMap.entrySet()) {
            builder.append("\n\t=== ").append(entry.getKey().getSimpleName()).append(" Type ===");

            for (final ExtensionDefinition extensionDefinition : entry.getValue()) {
                final String implementationClassName = extensionDefinition.getImplementationClassName();
                final List<Bundle> bundles = classNameBundleLookup.getOrDefault(implementationClassName, Collections.emptyList());

                builder.append("\n\t").append(implementationClassName);

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

    @Override
    public void logClassLoaderDetails() {
        if (!logger.isDebugEnabled()) {
            return;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("ClassLoader Hierarchy:\n");

        for (final Bundle bundle : classLoaderBundleLookup.values()) {
            buildClassLoaderDetails(bundle, sb, 0);
            sb.append("\n---------------------------------------------------------------------------------------------------------\n");
        }

        final String message = sb.toString();
        logger.debug(message);
    }

    private void buildClassLoaderDetails(final Bundle bundle, final StringBuilder sb, final int indentLevel) {
        final StringBuilder indentBuilder = new StringBuilder();
        indentBuilder.append("\n");

        for (int i=0; i < indentLevel; i++) {
            indentBuilder.append(" ");
        }

        final String prefix = indentBuilder.toString();

        sb.append(prefix).append("Bundle: ").append(bundle);
        sb.append(prefix).append("Working Directory: ").append(bundle.getBundleDetails().getWorkingDirectory().getAbsolutePath());
        sb.append(prefix).append("Coordinates: ").append(bundle.getBundleDetails().getCoordinate().getCoordinate());
        sb.append(prefix).append("Files loaded: ").append(bundle.getBundleDetails().getCoordinate().getCoordinate());

        final ClassLoader classLoader = bundle.getClassLoader();
        if (classLoader instanceof URLClassLoader) {
            final URL[] urls = ((URLClassLoader) bundle.getClassLoader()).getURLs();
            for (final URL url : urls) {
                sb.append(prefix).append("    ").append(url.getFile());
            }
        }

        final BundleCoordinate parentCoordinate = bundle.getBundleDetails().getDependencyCoordinate();
        if (parentCoordinate != null) {
            final Bundle parent = getBundle(parentCoordinate);
            if (parent != null) {
                buildClassLoaderDetails(parent, sb, indentLevel + 4);
            }
        }
    }

}
