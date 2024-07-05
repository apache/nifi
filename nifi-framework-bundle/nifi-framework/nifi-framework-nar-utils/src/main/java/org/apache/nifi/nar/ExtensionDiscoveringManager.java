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
import org.apache.nifi.python.PythonBridge;

import java.util.Set;

/**
 * ExtensionManage that can discovers and load extensions.
 */
public interface ExtensionDiscoveringManager extends ExtensionManager {

    /**
     * Discovers all extensions available in the provided bundles.
     *
     * This method is intended to be called only once during application start-up.
     *
     * @param systemBundle the system bundle
     * @param narBundles the bundles to use for discovering extensions
     */
    void discoverExtensions(Bundle systemBundle, Set<Bundle> narBundles);

    /**
     * Discovers extensions in the provided bundles.
     *
     * This method is intended to be used to discover additional extensions after the application is running.
     *
     * @param narBundles the bundles to use for discovering extensions
     */
    default void discoverExtensions(Set<Bundle> narBundles) {
        discoverExtensions(narBundles, true);
    }

    /**
     * Discovers extensions in the provided bundles.
     *
     * This method is intended to be used to discover additional extensions after the application is running.
     *
     * @param narBundles the bundles to use for discovering extensions
     * @param logDetails whether or not to log the details about what is loaded
     */
    void discoverExtensions(Set<Bundle> narBundles, boolean logDetails);

    /**
     * Discovers extensions of the given extension types in the provided bundles.
     *
     * This method is intended to be used to discover additional extensions after the application is running, and
     * is used to limit the type of extensions that can be discovered.
     *
     * @param narBundles the bundles to use for discovering extensions
     * @param extensionTypes the types of extensions to discover (i.e. Processor, ControllerService, etc)
     * @param logDetails whether or not to log the details about what is loaded
     */
    void discoverExtensions(Set<Bundle> narBundles, Set<Class<?>> extensionTypes, boolean logDetails);

    /**
     * Provides the Python Bridge that should be used for interacting with the Python Controller
     * @param pythonBridge the python bridge
     */
    void setPythonBridge(PythonBridge pythonBridge);

    /**
     * Discovers any Python based extensions that exist in either the Python extensions directories or NAR bundles that have been expanded.
     * @param pythonBundle the python bundle
     */
    void discoverPythonExtensions(Bundle pythonBundle);

    /**
     * Discovers any new Python based extensions that have been added. This method will scan only the Python extension directories
     * that have been configured and will not include scanning NAR bundles.
     * @param pythonBundle the python bundle
     */
    void discoverNewPythonExtensions(Bundle pythonBundle);

    /**
     * Discovers any new Python based extensions that exist in the given set of bundles.
     *
     * @param pythonBundle the python bundle
     * @param bundles the bundles to scan for Python extensions
     */
    void discoverPythonExtensions(Bundle pythonBundle, Set<Bundle> bundles);
}
