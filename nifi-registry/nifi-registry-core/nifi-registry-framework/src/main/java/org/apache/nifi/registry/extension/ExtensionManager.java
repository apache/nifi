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
package org.apache.nifi.registry.extension;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.apache.nifi.registry.security.authorization.AccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ExtensionManager {

    static final Logger LOGGER = LoggerFactory.getLogger(ExtensionManager.class);

    private static final List<Class> EXTENSION_CLASSES;
    static {
        final List<Class> classes = new ArrayList<>();
        classes.add(FlowPersistenceProvider.class);
        classes.add(UserGroupProvider.class);
        classes.add(AccessPolicyProvider.class);
        classes.add(Authorizer.class);
        classes.add(IdentityProvider.class);
        classes.add(EventHookProvider.class);
        classes.add(BundlePersistenceProvider.class);
        EXTENSION_CLASSES = Collections.unmodifiableList(classes);
    }

    private final NiFiRegistryProperties properties;
    private final Map<String,ExtensionClassLoader> classLoaderMap = new HashMap<>();
    private final AtomicBoolean loaded = new AtomicBoolean(false);

    @Autowired
    public ExtensionManager(final NiFiRegistryProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public synchronized void discoverExtensions() {
        if (!loaded.get()) {
            // get the list of class loaders to consider
            final List<ExtensionClassLoader> classLoaders = getClassLoaders();

            // for each class loader, attempt to load each extension class using the ServiceLoader
            for (final ExtensionClassLoader extensionClassLoader : classLoaders) {
                for (final Class extensionClass : EXTENSION_CLASSES) {
                    loadExtensions(extensionClass, extensionClassLoader);
                }
            }

            loaded.set(true);
        }
    }

    public ExtensionClassLoader getExtensionClassLoader(final String canonicalClassName) {
        if (StringUtils.isBlank(canonicalClassName)) {
            throw new IllegalArgumentException("Class name can not be null");
        }

        return classLoaderMap.get(canonicalClassName);
    }

    /**
     * Loads implementations of the given extension class from the given class loader.
     *
     * @param extensionClass the extension/service class
     * @param extensionClassLoader the class loader to search
     */
    private void loadExtensions(final Class extensionClass, final ExtensionClassLoader extensionClassLoader) {
        final ServiceLoader<?> serviceLoader = ServiceLoader.load(extensionClass, extensionClassLoader);
        for (final Object o : serviceLoader) {
            final String extensionClassName = o.getClass().getCanonicalName();
            if (classLoaderMap.containsKey(extensionClassName)) {
                final String currDir = extensionClassLoader.getRootDir();
                final String existingDir = classLoaderMap.get(extensionClassName).getRootDir();
                LOGGER.warn("Skipping {} from {} which was already found in {}", new Object[]{extensionClassName, currDir, existingDir});
            } else {
                classLoaderMap.put(o.getClass().getCanonicalName(), extensionClassLoader);
            }
        }
    }

    /**
     * Gets all of the class loaders to consider for loading extensions.
     *
     * Includes the class loader of the web-app running the framework, plus a class loader for each additional
     * directory specified in nifi-registry.properties.
     *
     * @return a list of extension class loaders
     */
    private List<ExtensionClassLoader> getClassLoaders() {
        final List<ExtensionClassLoader> classLoaders = new ArrayList<>();

        // start with the class loader that loaded ExtensionManager, should be WebAppClassLoader for API WAR
        final ExtensionClassLoader frameworkClassLoader = new ExtensionClassLoader("web-api", new URL[0], this.getClass().getClassLoader());
        classLoaders.add(frameworkClassLoader);

        // we want to use the system class loader as the parent of the extension class loaders
        ClassLoader systemClassLoader = FlowPersistenceProvider.class.getClassLoader();

        // add a class loader for each extension dir
        final Set<String> extensionDirs = properties.getExtensionsDirs();
        for (final String dir : extensionDirs) {
            if (!StringUtils.isBlank(dir)) {
                final ExtensionClassLoader classLoader = createClassLoader(dir, systemClassLoader);
                if (classLoader != null) {
                    classLoaders.add(classLoader);
                }
            }
        }

        return classLoaders;
    }

    /**
     * Creates a class loader for the given directory of resources.
     *
     * @param dir the dir of resources to add to the class loader
     * @param parentClassLoader the parent class loader
     * @return a class loader including all of the resources in the given dir, with the specified parent class loader
     */
    private ExtensionClassLoader createClassLoader(final String dir, final ClassLoader parentClassLoader) {
        final File dirFile = new File(dir);

        if (!dirFile.exists()) {
            LOGGER.warn("Skipping extension directory that does not exist: " + dir);
            return null;
        }

        if (!dirFile.canRead()) {
            LOGGER.warn("Skipping extension directory that can not be read: " + dir);
            return null;
        }

        final List<URL> resources = new LinkedList<>();

        try {
            resources.add(dirFile.toURI().toURL());
        } catch (final MalformedURLException mfe) {
            LOGGER.warn("Unable to add {} to classpath due to {}",
                    new Object[]{ dirFile.getAbsolutePath(), mfe.getMessage()}, mfe);
        }

        if (dirFile.isDirectory()) {
            final File[] files = dirFile.listFiles();
            if (files != null) {
                for (final File resource : files) {
                    if (resource.isDirectory()) {
                        LOGGER.warn("Recursive directories are not supported, skipping " + resource.getAbsolutePath());
                    } else {
                        try {
                            resources.add(resource.toURI().toURL());
                        } catch (final MalformedURLException mfe) {
                            LOGGER.warn("Unable to add {} to classpath due to {}",
                                    new Object[]{ resource.getAbsolutePath(), mfe.getMessage()}, mfe);
                        }
                    }
                }
            }
        }

        final URL[] urls = resources.toArray(new URL[resources.size()]);
        return new ExtensionClassLoader(dir, urls, parentClassLoader);
    }

}
