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
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceRepository;
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
    }

    /**
     * Loads all FlowFileProcessor, FlowFileComparator, ReportingTask class types that can be found on the bootstrap classloader and by creating classloaders for all NARs found within the classpath.
     * @param bundles the bundles to scan through in search of extensions
     */
    public static void discoverExtensions(final Set<Bundle> bundles) {
        // get the current context class loader
        ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();

        // consider each nar class loader
        for (final Bundle bundle : bundles) {
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

        final BundleCoordinate systemBundleCoordinate = new BundleCoordinate(
                BundleCoordinate.DEFAULT_GROUP, "default", BundleCoordinate.DEFAULT_VERSION);

        final BundleDetails systemBundleDetails = new BundleDetails.Builder()
                .workingDir(new File(narLibraryDirectory))
                .coordinate(systemBundleCoordinate)
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
            final ServiceLoader<?> serviceLoader = ServiceLoader.load(entry.getKey(), bundle.getClassLoader());

            for (final Object o : serviceLoader) {
                registerServiceClass(o.getClass(), classNameBundleLookup, bundle, entry.getValue());
            }
        }
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

        // check if any of the bundles for the given class name have the same coordinate as the current bundle we are processing
        Bundle registeredBundle = null;
        for (Bundle b : registeredBundles) {
            if (b.getBundleDetails().getCoordinate().equals(bundle.getBundleDetails().getCoordinate())) {
                registeredBundle = b;
                break;
            }
        }

        // there was no matching bundle then add the bundle to the registered bundles for the given class
        if (registeredBundle == null) {
            registeredBundles.add(bundle);
            classes.add(type);

            // keep track of which classes require a class loader per component instance
            if (type.isAnnotationPresent(RequiresInstanceClassLoading.class)) {
                requiresInstanceClassLoading.add(className);
            }
        }
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

        // Check if we already have a ClassLoader for this instance
        ClassLoader instanceClassLoader = instanceClassloaderLookup.get(instanceIdentifier);

        // If we don't then we'll create a new ClassLoader for this instance and add it to the map for future lookups
        if (instanceClassLoader == null) {
            final ClassLoader registeredClassLoader = bundle.getClassLoader();

            // If the class is annotated with @RequiresInstanceClassLoading and the registered ClassLoader is a URLClassLoader
            // then make a new InstanceClassLoader that is a full copy of the NAR Class Loader, otherwise create an empty
            // InstanceClassLoader that has the NAR ClassLoader as a parent
            if (requiresInstanceClassLoading.contains(classType) && (registeredClassLoader instanceof URLClassLoader)) {
                final URLClassLoader registeredUrlClassLoader = (URLClassLoader) registeredClassLoader;
                instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, registeredUrlClassLoader.getURLs(), registeredUrlClassLoader.getParent());
            } else {
                instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, new URL[0], registeredClassLoader);
            }

            instanceClassloaderLookup.put(instanceIdentifier, instanceClassLoader);
        }

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
        return requiresInstanceClassLoading.contains(classType);
    }

    /**
     * Retrieves the bundles that have a class with the given name.
     *
     * @param classType the class name of an extension
     * @return the list of bundles that contain an extension with the given class name
     */
    public static List<Bundle> getBundles(final String classType) {
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
        return bundleCoordinateBundleLookup.get(bundleCoordinate);
    }

    public static Set<Class> getExtensions(final Class<?> definition) {
        final Set<Class> extensions = definitionMap.get(definition);
        return (extensions == null) ? Collections.<Class>emptySet() : extensions;
    }

    public static void logClassLoaderMapping() {
        final StringBuilder builder = new StringBuilder();

        builder.append("Extension Type Mapping to Classloader:");
        for (final Map.Entry<Class, Set<Class>> entry : definitionMap.entrySet()) {
            builder.append("\n\t=== ").append(entry.getKey().getSimpleName()).append(" type || Classloader ===");

            for (final Class type : entry.getValue()) {
                builder.append("\n\t").append(type.getName()).append(" || ");//.append(getClassLoader(type.getName()));
            }

            builder.append("\n\t=== End ").append(entry.getKey().getSimpleName()).append(" types ===");
        }

        logger.info(builder.toString());
    }
}
