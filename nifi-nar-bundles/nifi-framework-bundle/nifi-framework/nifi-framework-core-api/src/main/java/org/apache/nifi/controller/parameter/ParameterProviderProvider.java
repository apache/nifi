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
package org.apache.nifi.controller.parameter;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.nar.ExtensionManager;

import java.util.Set;

/**
 * A ParameterProviderProvider is responsible for providing management of, and
 * access to, ParameterProviders.
 */
public interface ParameterProviderProvider {

    /**
     * Creates a new instance of a parameter provider
     *
     * @param type the type (fully qualified class name) of the parameter provider
     * to instantiate
     * @param id the identifier for the Parameter Provider
     * @param bundleCoordinate the bundle coordinate for the type of parameter provider
     * @param firstTimeAdded whether this is the first time that the
     * parameter provider is being added to the flow. I.e., this will be true only
     * when the user adds the parameter provider to the flow, not when the flow is
     * being restored after a restart of the software
     *
     * @return the ParameterProviderNode that is used to manage the parameter provider
     *
     * @throws ParameterProviderInstantiationException if unable to create the
     * Parameter Provider
     */
    ParameterProviderNode createParameterProvider(String type, String id, BundleCoordinate bundleCoordinate,
                                                  boolean firstTimeAdded) throws ParameterProviderInstantiationException;

    /**
     * @param identifier of node
     * @return the parameter provider that has the given identifier, or
     * <code>null</code> if no parameter provider exists with that ID
     */
    ParameterProviderNode getParameterProviderNode(String identifier);

    /**
     * @return a Set of all Parameter Providers that exist for this provider
     */
    Set<ParameterProviderNode> getAllParameterProviders();

    /**
     * Removes the given parameter provider from the flow
     *
     * @param parameterProvider The parameter provider node
     *
     * @throws IllegalStateException if the parameter provider is not known in the
     * flow
     */
    void removeParameterProvider(ParameterProviderNode parameterProvider);

    /**
     * @return the ExtensionManager instance used by this provider
     */
    ExtensionManager getExtensionManager();

}
