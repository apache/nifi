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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;

import java.net.URL;
import java.util.List;
import java.util.Set;

/**
 * Provides the framework with access to extensions, bundles, and class loaders.
 */
public interface ExtensionManager {

    /**
     * Retrieves all bundles known to this ExtensionManager.
     *
     * @return the set of known bundles
     */
    Set<Bundle> getAllBundles();

    /**
     * Creates the ClassLoader for the instance of the given type.
     *
     * @param classType the type of class to create the ClassLoader for
     * @param instanceIdentifier the identifier of the specific instance of the classType to look up the ClassLoader for
     * @param bundle the bundle where the classType exists
     * @param additionalUrls additional URLs to add to the instance class loader
     * @return the ClassLoader for the given instance of the given type, or null if the type is not a detected extension type
     */
    InstanceClassLoader createInstanceClassLoader(String classType, String instanceIdentifier, Bundle bundle, Set<URL> additionalUrls);

    /**
     * Retrieves the InstanceClassLoader for the component with the given identifier.
     *
     * @param instanceIdentifier the identifier of a component
     * @return the instance class loader for the component
     */
    InstanceClassLoader getInstanceClassLoader(String instanceIdentifier);

    /**
     * Removes the InstanceClassLoader for a given component.
     *
     * @param instanceIdentifier the of a component
     */
    InstanceClassLoader removeInstanceClassLoader(String instanceIdentifier);

    /**
     * Closes the given ClassLoader if it is an instance of URLClassLoader.
     *
     * @param instanceIdentifier the instance id the class loader corresponds to
     * @param classLoader the class loader to close
     */
    void closeURLClassLoader(String instanceIdentifier, ClassLoader classLoader);

    /**
     * Retrieves the bundles that have a class with the given name.
     *
     * @param classType the class name of an extension
     * @return the list of bundles that contain an extension with the given class name
     */
    List<Bundle> getBundles(String classType);

    /**
     * Retrieves the bundle with the given coordinate.
     *
     * @param bundleCoordinate a coordinate to look up
     * @return the bundle with the given coordinate, or null if none exists
     */
    Bundle getBundle(BundleCoordinate bundleCoordinate);

    /**
     * Retrieves the extension classes that were loaded from the bundle with the given coordinate.
     *
     * @param bundleCoordinate the coordinate
     * @return the classes from the bundle with that coordinate
     */
    Set<Class> getTypes(BundleCoordinate bundleCoordinate);

    /**
     * Retrieves the bundle for the given class loader.
     *
     * @param classLoader the class loader to look up the bundle for
     * @return the bundle for the given class loader
     */
    Bundle getBundle(ClassLoader classLoader);

    /**
     * Retrieves the set of classes that have been loaded for the definition.
     *
     * (i.e getExtensions(Processor.class)
     *
     * @param definition the extension definition, such as Processor.class
     * @return the set of extensions implementing the defintion
     */
    Set<Class> getExtensions(Class<?> definition);

    /**
     * Gets the temp component with the given type from the given bundle.
     *
     * @param classType the class name
     * @param bundleCoordinate the coordinate
     * @return the temp component instance
     */
    ConfigurableComponent getTempComponent(String classType, BundleCoordinate bundleCoordinate);

    /**
     * Logs the available class loaders.
     */
    void logClassLoaderMapping();

}
