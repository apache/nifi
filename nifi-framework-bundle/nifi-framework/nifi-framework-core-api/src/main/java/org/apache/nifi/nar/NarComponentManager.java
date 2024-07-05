/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;

import java.util.Set;

/**
 * Manages the lifecycle of components for a given NAR.
 */
public interface NarComponentManager {

    /**
     * Determines if any components exist from the given NAR.
     *
     * @param bundleCoordinate the coordinate of the NAR
     * @return true if any components are instantiated from the NAR, false otherwise
     */
    boolean componentsExist(BundleCoordinate bundleCoordinate, Set<ExtensionDefinition> extensionDefinitions);

    /**
     * Loads any components for the given extension definitions that came from the given coordinate that were previously missing (ghosted).
     *
     * @param bundleCoordinate the bundle coordinate
     * @param extensionDefinitions the extension definitions to consider
     * @param stoppedComponents the holder for any components that need to be stopped/disable in order to be reloaded
     */
    void loadMissingComponents(BundleCoordinate bundleCoordinate, Set<ExtensionDefinition> extensionDefinitions, StoppedComponents stoppedComponents);

    /**
     * Unloads (ghosts) any components for the given extension definitions.
     *
     * @param bundleCoordinate the bundle coordinate
     * @param extensionDefinitions the extension definitions to consider
     * @param stoppedComponents the holder for any components that need to be stopped/disable in order to be reloaded
     */
    void unloadComponents(BundleCoordinate bundleCoordinate, Set<ExtensionDefinition> extensionDefinitions, StoppedComponents stoppedComponents);

}
