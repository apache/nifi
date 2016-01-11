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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.nifi.authentication.LoginIdentityProvider;

import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.reporting.ReportingTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Map<String, ClassLoader> extensionClassloaderLookup = new HashMap<>();

    static {
        definitionMap.put(Processor.class, new HashSet<Class>());
        definitionMap.put(FlowFilePrioritizer.class, new HashSet<Class>());
        definitionMap.put(ReportingTask.class, new HashSet<Class>());
        definitionMap.put(ControllerService.class, new HashSet<Class>());
        definitionMap.put(AuthorityProvider.class, new HashSet<Class>());
        definitionMap.put(LoginIdentityProvider.class, new HashSet<Class>());
        definitionMap.put(ProvenanceEventRepository.class, new HashSet<Class>());
        definitionMap.put(ComponentStatusRepository.class, new HashSet<Class>());
        definitionMap.put(FlowFileRepository.class, new HashSet<Class>());
        definitionMap.put(FlowFileSwapManager.class, new HashSet<Class>());
        definitionMap.put(ContentRepository.class, new HashSet<Class>());
    }

    /**
     * Loads all FlowFileProcessor, FlowFileComparator, ReportingTask class types that can be found on the bootstrap classloader and by creating classloaders for all NARs found within the classpath.
     */
    public static void discoverExtensions() {
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

        // get the current context class loader
        ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();

        // consider the system class loader
        loadExtensions(systemClassLoader);

        // consider each nar class loader
        for (final ClassLoader ncl : NarClassLoaders.getExtensionClassLoaders()) {

            // Must set the context class loader to the nar classloader itself
            // so that static initialization techniques that depend on the context class loader will work properly
            Thread.currentThread().setContextClassLoader(ncl);
            loadExtensions(ncl);
        }

        // restore the current context class loader if appropriate
        if (currentContextClassLoader != null) {
            Thread.currentThread().setContextClassLoader(currentContextClassLoader);
        }
    }

    /**
     * Loads extensions from the specified class loader.
     *
     * @param classLoader from which to load extensions
     */
    @SuppressWarnings("unchecked")
    private static void loadExtensions(final ClassLoader classLoader) {
        for (final Map.Entry<Class, Set<Class>> entry : definitionMap.entrySet()) {
            final ServiceLoader<?> serviceLoader = ServiceLoader.load(entry.getKey(), classLoader);

            for (final Object o : serviceLoader) {
                registerServiceClass(o.getClass(), extensionClassloaderLookup, classLoader, entry.getValue());
            }
        }
    }

    /**
     * Registers extension for the specified type from the specified ClassLoader.
     *
     * @param type the extension type
     * @param classloaderMap mapping of classname to classloader
     * @param classLoader the classloader being mapped to
     * @param classes to map to this classloader but which come from its ancestors
     */
    private static void registerServiceClass(final Class<?> type, final Map<String, ClassLoader> classloaderMap, final ClassLoader classLoader, final Set<Class> classes) {
        final String className = type.getName();
        final ClassLoader registeredClassLoader = classloaderMap.get(className);

        // see if this class is already registered (this should happen when the class is loaded by an ancestor of the specified classloader)
        if (registeredClassLoader == null) {
            classloaderMap.put(className, classLoader);
            classes.add(type);
        } else {
            boolean loadedFromAncestor = false;

            // determine if this class was loaded from an ancestor
            ClassLoader ancestorClassLoader = classLoader.getParent();
            while (ancestorClassLoader != null) {
                if (ancestorClassLoader == registeredClassLoader) {
                    loadedFromAncestor = true;
                    break;
                }
                ancestorClassLoader = ancestorClassLoader.getParent();
            }

            // if this class was loaded from a non ancestor class loader, report potential unexpected behavior
            if (!loadedFromAncestor) {
                logger.warn("Attempt was made to load " + className + " from " + classLoader
                        + " but that class name is already loaded/registered from " + registeredClassLoader
                        + ".  This may cause unpredictable behavior.  Order of NARs is not guaranteed.");
            }
        }
    }

    /**
     * Determines the effective classloader for classes of the given type. If returns null it indicates the given type is not known or was not detected.
     *
     * @param classType to lookup the classloader of
     * @return String of fully qualified class name; null if not a detected type
     */
    public static ClassLoader getClassLoader(final String classType) {
        return extensionClassloaderLookup.get(classType);
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
                builder.append("\n\t").append(type.getName()).append(" || ").append(getClassLoader(type.getName()));
            }

            builder.append("\n\t=== End ").append(entry.getKey().getSimpleName()).append(" types ===");
        }

        logger.info(builder.toString());
    }
}
