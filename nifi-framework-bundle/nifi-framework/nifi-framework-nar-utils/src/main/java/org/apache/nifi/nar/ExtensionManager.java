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
import org.apache.nifi.python.PythonProcessorDetails;

import java.net.URL;
import java.util.Collection;
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
    default InstanceClassLoader createInstanceClassLoader(String classType, String instanceIdentifier, Bundle bundle, Set<URL> additionalUrls) {
        return createInstanceClassLoader(classType, instanceIdentifier, bundle, additionalUrls, true, null);
    }

    /**
     * Creates the ClassLoader for the instance of the given type.
     *
     * @param classType the type of class to create the ClassLoader for
     * @param instanceIdentifier the identifier of the specific instance of the classType to look up the ClassLoader for
     * @param bundle the bundle where the classType exists
     * @param additionalUrls additional URLs to add to the instance class loader
     * @param registerClassLoader whether or not to register the class loader as the new classloader for the component with the given ID
     * @param classloaderIsolationKey a classloader key that can be used in order to specify which shared class loader can be used as the instance class loader's parent, or <code>null</code> if the
     * parent class loader should be shared or if cloning ancestors is not necessary
     * @return the ClassLoader for the given instance of the given type, or null if the type is not a detected extension type
     */
    InstanceClassLoader createInstanceClassLoader(String classType, String instanceIdentifier, Bundle bundle, Set<URL> additionalUrls, boolean registerClassLoader, String classloaderIsolationKey);

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
     * Registers the given instance class loader so that it can be later retrieved via {@link #getInstanceClassLoader(String)}
     * @param instanceIdentifier the instance identifier
     * @param instanceClassLoader the class loader
     */
    void registerInstanceClassLoader(String instanceIdentifier, InstanceClassLoader instanceClassLoader);

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
     * Removes the bundles with the given coordinates from the extension manager.
     *
     * @param bundleCoordinates the coordinates
     * @return the removed bundles, or empty if none exists
     */
    Set<Bundle> removeBundles(Collection<BundleCoordinate> bundleCoordinates);

    /**
     * Retrieves the bundles that have a dependency on the bundle with the given coordinate.
     *
     * @param bundleCoordinate the coordinate
     * @return the bundles with a dependency on the coordinate
     */
    Set<Bundle> getDependentBundles(BundleCoordinate bundleCoordinate);

    /**
     * Retrieves the extension classes that were loaded from the bundle with the given coordinate.
     *
     * @param bundleCoordinate the coordinate
     * @return the definitions of the extensions from the bundle with that coordinate
     */
    Set<ExtensionDefinition> getTypes(BundleCoordinate bundleCoordinate);

    /**
     * Returns the Class that is described by the given definition
     * @param extensionDefinition the extension definition
     * @return the extension's class
     */
    Class<?> getClass(ExtensionDefinition extensionDefinition);

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
     * @return the set of extension definitions that describe the the extensions implementing the defintion
     */
    Set<ExtensionDefinition> getExtensions(Class<?> definition);

    /**
     * Gets the temp component with the given type from the given bundle.
     *
     * @param classType the class name
     * @param bundleCoordinate the coordinate
     * @return the temp component instance
     */
    ConfigurableComponent getTempComponent(String classType, BundleCoordinate bundleCoordinate);

    /**
     * Returns the details about the Python Processor with the given type and version
     * @param processorType the type of the Processor
     * @param version the version of the Processor
     * @return the details for the Python Processor, or <code>null</code> if no Processor can be given that match the given type and version
     */
    PythonProcessorDetails getPythonProcessorDetails(String processorType, String version);

    /**
     * Returns the set of Python extension definitions originating from the bundle with the given coordinate.
     *
     * @param originalBundleCoordinate the coordinate of the Python NAR that was loaded
     * @return the set of processors details that come from this bundle
     */
    Set<ExtensionDefinition> getPythonExtensions(BundleCoordinate originalBundleCoordinate);

    /**
     * Logs the available class loaders.
     */
    void logClassLoaderMapping();

    /**
     * Logs details about the files loaded by the class loaders
     */
    void logClassLoaderDetails();

}
