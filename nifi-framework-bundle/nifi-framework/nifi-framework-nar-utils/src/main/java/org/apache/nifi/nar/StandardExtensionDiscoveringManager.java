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
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsModel;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.flow.resource.ExternalResourceProvider;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.init.ConfigurableComponentInitializer;
import org.apache.nifi.init.ConfigurableComponentInitializerFactory;
import org.apache.nifi.nar.ExtensionDefinition.ExtensionRuntime;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBundleCoordinate;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.registry.flow.FlowRegistryClient;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Scans through the classpath to load all FlowFileProcessors, FlowFileComparators, and ReportingTasks using the service provider API and running through all classloaders (root, NARs).
 */
@SuppressWarnings("rawtypes")
public class StandardExtensionDiscoveringManager implements ExtensionDiscoveringManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardExtensionDiscoveringManager.class);

    // Maps a service definition (interface) to those classes that implement the interface
    private final Map<Class<?>, Set<ExtensionDefinition>> definitionMap = new HashMap<>();

    private final Map<String, List<Bundle>> classNameBundleLookup = new HashMap<>();
    private final Map<BundleCoordinate, Set<ExtensionDefinition>> bundleCoordinateClassesLookup = new HashMap<>();
    private final Map<BundleCoordinate, Bundle> bundleCoordinateBundleLookup = new HashMap<>();
    private final Map<ClassLoader, Bundle> classLoaderBundleLookup = new HashMap<>();
    private final Map<String, ConfigurableComponent> tempComponentLookup = new HashMap<>();
    private final Map<String, List<PythonProcessorDetails>> pythonProcessorDetails = new HashMap<>();

    private final Map<String, InstanceClassLoader> instanceClassloaderLookup = new ConcurrentHashMap<>();
    private final ConcurrentMap<BaseClassLoaderKey, SharedInstanceClassLoader> sharedBaseClassloaders = new ConcurrentHashMap<>();

    // TODO: Make final and provide via constructor?
    private PythonBridge pythonBridge;

    public StandardExtensionDiscoveringManager() {
        this(Collections.emptyList());
    }

    public StandardExtensionDiscoveringManager(final Collection<Class<? extends ConfigurableComponent>> additionalExtensionTypes) {
        definitionMap.put(Processor.class, new HashSet<>());
        definitionMap.put(FlowFilePrioritizer.class, new HashSet<>());
        definitionMap.put(ReportingTask.class, new HashSet<>());
        definitionMap.put(FlowAnalysisRule.class, new HashSet<>());
        definitionMap.put(ParameterProvider.class, new HashSet<>());
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
        definitionMap.put(ExternalResourceProvider.class, new HashSet<>());
        definitionMap.put(FlowRegistryClient.class, new HashSet<>());
        definitionMap.put(LeaderElectionManager.class, new HashSet<>());
        definitionMap.put(PythonBridge.class, new HashSet<>());
        definitionMap.put(NarPersistenceProvider.class, new HashSet<>());
        definitionMap.put(AssetManager.class, new HashSet<>());

        additionalExtensionTypes.forEach(type -> definitionMap.putIfAbsent(type, new HashSet<>()));
    }

    @Override
    public synchronized Set<Bundle> getAllBundles() {
        return new HashSet<>(bundleCoordinateBundleLookup.values());
    }

    @Override
    public synchronized void discoverExtensions(final Bundle systemBundle, final Set<Bundle> narBundles) {
        // load the system bundle first so that any extensions found in JARs directly in lib will be registered as
        // being from the system bundle and not from all the other NARs
        loadExtensions(systemBundle, definitionMap.keySet());
        bundleCoordinateBundleLookup.put(systemBundle.getBundleDetails().getCoordinate(), systemBundle);

        discoverExtensions(narBundles);
    }

    @Override
    public synchronized void discoverExtensions(final Set<Bundle> narBundles, final boolean logDetails) {
        discoverExtensions(narBundles, definitionMap.keySet(), logDetails);
    }

    @Override
    public synchronized void discoverExtensions(final Set<Bundle> narBundles, final Set<Class<?>> extensionTypes, final boolean logDetails) {
        // get the current context class loader
        ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();

        // consider each nar class loader
        for (final Bundle bundle : narBundles) {
            // Must set the context class loader to the nar classloader itself
            // so that static initialization techniques that depend on the context class loader will work properly
            final ClassLoader ncl = bundle.getClassLoader();
            Thread.currentThread().setContextClassLoader(ncl);

            final long loadStart = System.currentTimeMillis();
            loadExtensions(bundle, extensionTypes);
            final long loadMillis = System.currentTimeMillis() - loadStart;
            if (logDetails) {
                logger.info("Loaded extensions for {} in {} millis", bundle.getBundleDetails(), loadMillis);
            }

            // Create a look-up from coordinate to bundle
            bundleCoordinateBundleLookup.put(bundle.getBundleDetails().getCoordinate(), bundle);
        }

        // restore the current context class loader if appropriate
        if (currentContextClassLoader != null) {
            Thread.currentThread().setContextClassLoader(currentContextClassLoader);
        }
    }

    @Override
    public synchronized void setPythonBridge(final PythonBridge pythonBridge) {
        this.pythonBridge = pythonBridge;
    }

    @Override
    public synchronized void discoverPythonExtensions(final Bundle pythonBundle) {
        discoverPythonExtensions(pythonBundle, true);
    }

    @Override
    public synchronized void discoverNewPythonExtensions(final Bundle pythonBundle) {
        logger.debug("Scanning to discover new Python extensions...");
        discoverPythonExtensions(pythonBundle, false);
    }

    @Override
    public synchronized void discoverPythonExtensions(final Bundle pythonBundle, final Set<Bundle> bundles) {
        logger.debug("Scanning to discover which Python extensions are available and importing any necessary dependencies. If new components are discovered, this may take a few minutes. " +
                "See python logs for more details.");
        final long start = System.currentTimeMillis();
        final List<File> bundleWorkingDirectories = bundles.stream()
                .map(Bundle::getBundleDetails)
                .map(BundleDetails::getWorkingDirectory)
                .toList();
        pythonBridge.discoverExtensions(bundleWorkingDirectories);
        loadPythonExtensions(pythonBundle, start);
    }

    private void discoverPythonExtensions(final Bundle pythonBundle, final boolean includeNarBundles) {
        logger.debug("Scanning to discover which Python extensions are available and importing any necessary dependencies. If new components are discovered, this may take a few minutes. " +
            "See python logs for more details.");
        final long start = System.currentTimeMillis();
        pythonBridge.discoverExtensions(includeNarBundles);
        loadPythonExtensions(pythonBundle, start);
    }

    private void loadPythonExtensions(final Bundle pythonBundle, final long startTime) {
        bundleCoordinateBundleLookup.putIfAbsent(pythonBundle.getBundleDetails().getCoordinate(), pythonBundle);

        final Set<ExtensionDefinition> processorDefinitions = definitionMap.get(Processor.class);
        final List<PythonProcessorDetails> pythonProcessorDetails = pythonBridge.getProcessorTypes();

        int processorsFound = 0;
        for (final PythonProcessorDetails details : pythonProcessorDetails) {
            final BundleDetails bundleDetails = createBundleDetailsWithOverriddenVersion(pythonBundle.getBundleDetails(), details.getProcessorVersion());
            final Bundle bundle = new Bundle(bundleDetails, pythonBundle.getClassLoader());

            final String className = details.getProcessorType();
            final ExtensionDefinition extensionDefinition = new ExtensionDefinition.Builder()
                    .implementationClassName(className)
                    .runtime(ExtensionRuntime.PYTHON)
                    .bundle(bundle)
                    .extensionType(Processor.class)
                    .description(details.getCapabilityDescription())
                    .tags(details.getTags())
                    .version(details.getProcessorVersion())
                    .build();

            final boolean added = processorDefinitions.add(extensionDefinition);
            if (added) {
                processorsFound++;
                final List<Bundle> bundlesForClass = classNameBundleLookup.computeIfAbsent(className, key -> new ArrayList<>());
                bundlesForClass.add(bundle);
                bundleCoordinateBundleLookup.putIfAbsent(bundleDetails.getCoordinate(), bundle);

                final Set<ExtensionDefinition> bundleExtensionDefinitions = bundleCoordinateClassesLookup.computeIfAbsent(bundleDetails.getCoordinate(), (key) -> new HashSet<>());
                bundleExtensionDefinitions.add(extensionDefinition);

                final List<PythonProcessorDetails> detailsList = this.pythonProcessorDetails.computeIfAbsent(details.getProcessorType(), key -> new ArrayList<>());
                detailsList.add(details);

                logger.info("Discovered Python Processor {}", details.getProcessorType());
            } else {
                logger.debug("Python Processor {} is already known", details.getProcessorType());
            }
        }

        if (processorsFound == 0) {
            logger.debug("Discovered no new or updated Python Processors. Process took in {} millis", System.currentTimeMillis() - startTime);
        } else {
            logger.info("Discovered or updated {} Python Processors in {} millis", processorsFound, System.currentTimeMillis() - startTime);
        }
    }

    @Override
    public synchronized PythonProcessorDetails getPythonProcessorDetails(final String processorType, final String version) {
        final List<PythonProcessorDetails> detailsList = this.pythonProcessorDetails.get(processorType);
        if (detailsList == null) {
            return null;
        }

        for (final PythonProcessorDetails processorDetails : detailsList) {
            if (processorDetails.getProcessorVersion().equals(version)) {
                return processorDetails;
            }
        }

        return null;
    }

    @Override
    public synchronized Set<ExtensionDefinition> getPythonExtensions(final BundleCoordinate originalBundleCoordinate) {
        final Set<ExtensionDefinition> pythonProcessorDefinitions = new HashSet<>();
        for (final ExtensionDefinition processorDefinition : definitionMap.get(Processor.class)) {
            final PythonProcessorDetails processorDetails = getPythonProcessorDetails(processorDefinition.getImplementationClassName(), processorDefinition.getVersion());
            if (processorDetails != null) {
                final PythonBundleCoordinate pythonBundleCoordinate = processorDetails.getBundleCoordinate();
                if (originalBundleCoordinate.getGroup().equals(pythonBundleCoordinate.getGroup())
                        && originalBundleCoordinate.getId().equals(pythonBundleCoordinate.getId())
                        && originalBundleCoordinate.getVersion().equals(pythonBundleCoordinate.getVersion())) {
                    pythonProcessorDefinitions.add(processorDefinition);
                }
            }
        }
        logger.trace("Found {} Python Processor definitions loaded from [{}]", pythonProcessorDefinitions.size(), originalBundleCoordinate);
        return pythonProcessorDefinitions;
    }

    private BundleDetails createBundleDetailsWithOverriddenVersion(final BundleDetails details, final String version) {
        final BundleCoordinate overriddenCoordinate = new BundleCoordinate(details.getCoordinate().getGroup(), details.getCoordinate().getId(), version);

        return new BundleDetails.Builder()
            .buildBranch(details.getBuildBranch())
            .buildJdk(details.getBuildJdk())
            .buildRevision(details.getBuildRevision())
            .buildTag(details.getBuildTag())
            .buildTimestamp(details.getBuildTimestamp())
            .builtBy(details.getBuiltBy())
            .dependencyCoordinate(details.getDependencyCoordinate())
            .coordinate(overriddenCoordinate)
            .workingDir(details.getWorkingDirectory())
            .build();
    }


    /**
     * Loads extensions from the specified bundle.
     *
     * @param bundle from which to load extensions
     * @param extensionTypes the types of extensions to load
     */
    private void loadExtensions(final Bundle bundle, final Set<Class<?>> extensionTypes) {
        for (final Class extensionType : extensionTypes) {
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
                            logger.error("Failed to register {} of type {} in bundle {}", extensionType.getSimpleName(), implementationClassName, bundle, e);
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
     * @param extensionType the type of extension (I.e., Processor, ControllerService, ParameterProvider, ReportingTask, etc.)
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
            logger.warn("Unable to initialize component {} due to {}", configurableComponent.getClass().getName(), e.getMessage());
        }
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

            final ExtensionDefinition extensionDefinition = new ExtensionDefinition.Builder()
                .implementationClassName(className)
                .bundle(bundle)
                .extensionType(extensionType)
                .runtime(ExtensionRuntime.JAVA)
                .build();

            bundleExtensionDefinitions.add(extensionDefinition);
            classes.add(extensionDefinition);
        }
    }

    @Override
    public Class<?> getClass(final ExtensionDefinition extensionDefinition) {
        final Bundle bundle = extensionDefinition.getBundle();
        final ClassLoader bundleClassLoader = bundle.getClassLoader();

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(bundleClassLoader)) {
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
        return Processor.class.isAssignableFrom(type) || ControllerService.class.isAssignableFrom(type) || ReportingTask.class.isAssignableFrom(type)
                || FlowAnalysisRule.class.isAssignableFrom(type) || ParameterProvider.class.isAssignableFrom(type) || FlowRegistryClient.class.isAssignableFrom(type);
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
    public synchronized InstanceClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle, final Set<URL> additionalUrls,
                                                                      final boolean register, final String classloaderIsolationKey) {
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

            final boolean allowsSharedClassloader = tempComponent instanceof ClassloaderIsolationKeyProvider;
            if (allowsSharedClassloader && classloaderIsolationKey == null) {
                instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, Collections.emptySet(), additionalUrls, bundleClassLoader);
            } else {
                final BaseClassLoaderKey baseClassLoaderKey = classloaderIsolationKey == null ? null : new BaseClassLoaderKey(bundle, classloaderIsolationKey);
                final NarClassLoader narBundleClassLoader = (NarClassLoader) bundleClassLoader;
                logger.debug("Including ClassLoader resources from {} for component {}", bundle.getBundleDetails(), instanceIdentifier);

                final Set<URL> instanceUrls = new LinkedHashSet<>(Arrays.asList(narBundleClassLoader.getURLs()));
                final Set<File> narNativeLibDirs = new LinkedHashSet<>();
                narNativeLibDirs.add(narBundleClassLoader.getNARNativeLibDir());

                ClassLoader ancestorClassLoader = narBundleClassLoader.getParent();

                boolean resolvedSharedClassLoader = false;
                final RequiresInstanceClassLoading requiresInstanceClassLoading = type.getAnnotation(RequiresInstanceClassLoading.class);
                if (requiresInstanceClassLoading.cloneAncestorResources()) {
                    // Check to see if there's already a shared ClassLoader that can be used as the parent/base classloader
                    if (baseClassLoaderKey != null) {
                        final SharedInstanceClassLoader sharedBaseClassloader = sharedBaseClassloaders.get(baseClassLoaderKey);
                        if (sharedBaseClassloader != null && sharedBaseClassloader.incrementReferenceCount()) {
                            resolvedSharedClassLoader = true;
                            ancestorClassLoader = sharedBaseClassloader;
                            logger.debug("Creating InstanceClassLoader for type {} using shared Base ClassLoader {} for component {}", type, sharedBaseClassloader, instanceIdentifier);
                        }
                    }

                    // If we didn't find a shared ClassLoader to use, go ahead and clone the bundle's ClassLoader.
                    if (!resolvedSharedClassLoader) {
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
                }

                // register our new InstanceClassLoader as the shared base classloader.
                if (baseClassLoaderKey != null && !resolvedSharedClassLoader) {
                    // Created a shared class loader that is everything we need except for the additional URLs, as the additional URLs are instance-specific.
                    final SharedInstanceClassLoader sharedClassLoader = new SharedInstanceClassLoader(instanceIdentifier, classType, instanceUrls,
                        Collections.emptySet(), narNativeLibDirs, ancestorClassLoader);
                    sharedClassLoader.incrementReferenceCount();

                    instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, Collections.emptySet(), additionalUrls, Collections.emptySet(), sharedClassLoader);

                    logger.debug("Creating InstanceClassLoader for type {} using newly created shared Base ClassLoader {} for component {}", type, sharedClassLoader, instanceIdentifier);
                    if (logger.isTraceEnabled()) {
                        for (URL url : sharedClassLoader.getURLs()) {
                            logger.trace("Shared Base ClassLoader URL resource: {}", url.toExternalForm());
                        }
                    }

                    sharedBaseClassloaders.putIfAbsent(baseClassLoaderKey, sharedClassLoader);
                } else {
                    // If we resolved a shared classloader, the shared classloader already has the instance URLs, so there's no need to provide them for the Instance ClassLoader.
                    // But if we did not resolve to a shared ClassLoader, it's important to pull in the instanceUrls.
                    final Set<URL> resolvedInstanceUrls = resolvedSharedClassLoader ? Collections.emptySet() : instanceUrls;

                    instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, resolvedInstanceUrls, additionalUrls, narNativeLibDirs, ancestorClassLoader);
                }
            }
        } else {
            instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, Collections.emptySet(), additionalUrls, bundleClassLoader);
        }

        if (logger.isTraceEnabled()) {
            for (URL url : instanceClassLoader.getURLs()) {
                logger.trace("URL resource {} for {}...", url.toExternalForm(), instanceIdentifier);
            }
        }

        if (register) {
            instanceClassloaderLookup.put(instanceIdentifier, instanceClassLoader);
        }

        return instanceClassLoader;
    }


    /**
     * Find the bundle coordinates for any service APIs that are referenced by this component and not part of the same bundle.
     *
     * @param component the component being instantiated
     */
    protected Set<BundleCoordinate> findReachableApiBundles(final ConfigurableComponent component) {
        final Set<BundleCoordinate> reachableApiBundles = new HashSet<>();

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(component.getClass().getClassLoader())) {
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
    public synchronized InstanceClassLoader getInstanceClassLoader(final String instanceIdentifier) {
        return instanceClassloaderLookup.get(instanceIdentifier);
    }

    @Override
    public synchronized InstanceClassLoader removeInstanceClassLoader(final String instanceIdentifier) {
        if (instanceIdentifier == null) {
            return null;
        }

        final InstanceClassLoader classLoader = instanceClassloaderLookup.remove(instanceIdentifier);
        closeURLClassLoader(instanceIdentifier, classLoader);
        return classLoader;
    }

    @Override
    public synchronized void registerInstanceClassLoader(final String instanceIdentifier, final InstanceClassLoader instanceClassLoader) {
        instanceClassloaderLookup.putIfAbsent(instanceIdentifier, instanceClassLoader);
    }

    @Override
    public void closeURLClassLoader(final String instanceIdentifier, final ClassLoader classLoader) {
        if ((classLoader instanceof URLClassLoader)) {
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            try {
                urlClassLoader.close();
            } catch (IOException e) {
                logger.warn("Unable to close URLClassLoader for {}", instanceIdentifier);
            }
        }
    }

    @Override
    public synchronized List<Bundle> getBundles(final String classType) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }

        final List<Bundle> bundles = classNameBundleLookup.get(classType);
        return bundles == null ? Collections.emptyList() : new ArrayList<>(bundles);
    }

    @Override
    public synchronized Bundle getBundle(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        return bundleCoordinateBundleLookup.get(bundleCoordinate);
    }

    @Override
    public synchronized Set<Bundle> removeBundles(final Collection<BundleCoordinate> bundleCoordinates) {
        final Set<Bundle> removedBundles = new LinkedHashSet<>();
        for (final BundleCoordinate bundleCoordinate : bundleCoordinates) {
            final Bundle removedBundle = removeBundle(bundleCoordinate);
            if (removedBundle != null) {
                removedBundles.add(removedBundle);
            }
        }
        return removedBundles;
    }

    private Bundle removeBundle(final BundleCoordinate bundleCoordinate) {
        if (PythonBundle.isPythonCoordinate(bundleCoordinate)) {
            throw new IllegalStateException("Python bundle [%s] cannot be removed".formatted(bundleCoordinate));
        }

        final Bundle removedBundle = bundleCoordinateBundleLookup.remove(bundleCoordinate);
        if (removedBundle == null) {
            logger.debug("Bundle not found with coordinate [{}]", bundleCoordinate);
            return null;
        }

        logger.debug("Removing bundle [{}]", bundleCoordinate);
        final ClassLoader removedBundleClassLoader = removedBundle.getClassLoader();
        classLoaderBundleLookup.remove(removedBundleClassLoader);

        if (removedBundleClassLoader instanceof URLClassLoader) {
            try {
                ((URLClassLoader) removedBundleClassLoader).close();
            } catch (final IOException e) {
                logger.warn("Failed to close ClassLoader for {}", bundleCoordinate, e);
            }
        }

        final Set<ExtensionDefinition> extensionDefinitions = new HashSet<>();
        extensionDefinitions.addAll(Optional.ofNullable(bundleCoordinateClassesLookup.remove(bundleCoordinate)).orElse(Collections.emptySet()));
        extensionDefinitions.addAll(getPythonExtensions(bundleCoordinate));
        extensionDefinitions.forEach(this::removeExtensionDefinition);

        return removedBundle;
    }

    private void removeExtensionDefinition(final ExtensionDefinition extensionDefinition) {
        // Use the coordinate from the Bundle of the ExtensionDefinition because Python extension definitions will
        // have a different coordinate from the original bundle being deleted that triggered this method
        final BundleCoordinate extensionDefinitionCoordinate = extensionDefinition.getBundle().getBundleDetails().getCoordinate();
        logger.debug("Removing extension definition [{}] from [{}]", extensionDefinition.getImplementationClassName(), extensionDefinitionCoordinate);

        final Set<ExtensionDefinition> definitions = definitionMap.get(extensionDefinition.getExtensionType());
        if (definitions != null) {
            definitions.remove(extensionDefinition);
        }

        final String removeExtensionClassName = extensionDefinition.getImplementationClassName();
        final List<Bundle> classNameBundles = Optional.ofNullable(classNameBundleLookup.get(removeExtensionClassName)).orElse(Collections.emptyList());
        classNameBundles.removeIf(bundle -> bundle.getBundleDetails().getCoordinate().equals(extensionDefinitionCoordinate));

        final String tempComponentKey = getClassBundleKey(removeExtensionClassName, extensionDefinitionCoordinate);
        final ConfigurableComponent removedTempComponent = tempComponentLookup.remove(tempComponentKey);

        if (PythonBundle.isPythonCoordinate(extensionDefinitionCoordinate)) {
            logger.debug("Removing Python processor type {} - {}", removeExtensionClassName, extensionDefinition.getVersion());

            final List<PythonProcessorDetails> processorDetailsList = Optional.ofNullable(pythonProcessorDetails.get(removeExtensionClassName)).orElse(Collections.emptyList());
            processorDetailsList.removeIf(processorDetails -> processorDetails.getProcessorType().equals(removeExtensionClassName)
                    && processorDetails.getProcessorVersion().equals(extensionDefinition.getVersion()));

            if (removedTempComponent != null) {
                final String pythonTempComponentId = getPythonTempComponentId(removeExtensionClassName);
                pythonBridge.onProcessorRemoved(pythonTempComponentId, removeExtensionClassName, extensionDefinition.getVersion());
            }
            pythonBridge.removeProcessorType(removeExtensionClassName, extensionDefinition.getVersion());
        }
    }

    @Override
    public synchronized Set<Bundle> getDependentBundles(final BundleCoordinate bundleCoordinate) {
        return getAllBundles().stream()
                .filter(bundle -> bundle.getBundleDetails().getDependencyCoordinate() != null
                        && bundle.getBundleDetails().getDependencyCoordinate().equals(bundleCoordinate))
                .collect(Collectors.toSet());
    }

    @Override
    public synchronized Set<ExtensionDefinition> getTypes(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        final Set<ExtensionDefinition> types = bundleCoordinateClassesLookup.get(bundleCoordinate);
        return types == null ? Collections.emptySet() : Collections.unmodifiableSet(types);
    }

    @Override
    public synchronized Bundle getBundle(final ClassLoader classLoader) {
        if (classLoader == null) {
            throw new IllegalArgumentException("ClassLoader cannot be null");
        }
        return classLoaderBundleLookup.get(classLoader);
    }

    @Override
    public synchronized Set<ExtensionDefinition> getExtensions(final Class<?> definition) {
        if (definition == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        final Set<ExtensionDefinition> extensions = definitionMap.get(definition);
        return (extensions == null) ? Collections.emptySet() : new HashSet<>(extensions);
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
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(bundleClassLoader)) {
            final ConfigurableComponent tempComponent;
            if (PythonBundle.isPythonCoordinate(bundle.getBundleDetails().getCoordinate())) {
                final String procId = getPythonTempComponentId(classType);
                tempComponent = pythonBridge.createProcessor(procId, classType, bundleCoordinate.getVersion(), false, false);
            } else {
                final Class<?> componentClass = Class.forName(classType, true, bundleClassLoader);
                tempComponent = (ConfigurableComponent) componentClass.getDeclaredConstructor().newInstance();
            }

            initializeTempComponent(tempComponent);
            tempComponentLookup.put(bundleKey, tempComponent);
            return tempComponent;
        } catch (final Exception e) {
            logger.error("Could not instantiate class of type {} using ClassLoader for bundle {}", classType, bundleCoordinate, e);
            if (logger.isDebugEnabled() && bundleClassLoader instanceof URLClassLoader) {
                final URLClassLoader urlClassLoader = (URLClassLoader) bundleClassLoader;
                final List<URL> availableUrls = Arrays.asList(urlClassLoader.getURLs());
                logger.debug("Available URLs for Bundle ClassLoader {}: {}", bundleCoordinate, availableUrls);
            }

            return null;
        }
    }

    private static String getPythonTempComponentId(final String type) {
        return "temp-component-" + type;
    }

    private static String getClassBundleKey(final String classType, final BundleCoordinate bundleCoordinate) {
        return classType + "_" + bundleCoordinate.getCoordinate();
    }

    @Override
    public synchronized void logClassLoaderMapping() {
        final StringBuilder builder = new StringBuilder();

        builder.append("Extension Type Mapping to Bundle:");
        for (final Map.Entry<Class<?>, Set<ExtensionDefinition>> entry : definitionMap.entrySet()) {
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

        logger.info("{}", builder);
    }

    @Override
    public synchronized void logClassLoaderDetails() {
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

        for (int i = 0; i < indentLevel; i++) {
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


    private static class BaseClassLoaderKey {
        private final Bundle bundle;
        private final String classloaderIsolationKey;

        public BaseClassLoaderKey(final Bundle bundle, final String classloaderIsolationKey) {
            this.bundle = Objects.requireNonNull(bundle);
            this.classloaderIsolationKey = Objects.requireNonNull(classloaderIsolationKey);
        }

        public Bundle getBundle() {
            return bundle;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final BaseClassLoaderKey that = (BaseClassLoaderKey) o;
            return bundle.equals(that.bundle) && classloaderIsolationKey.equals(that.classloaderIsolationKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bundle, classloaderIsolationKey);
        }
    }
}
