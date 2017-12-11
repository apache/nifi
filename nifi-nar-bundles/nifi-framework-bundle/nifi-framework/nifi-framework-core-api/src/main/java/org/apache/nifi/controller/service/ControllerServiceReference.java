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
package org.apache.nifi.controller.service;

import java.util.List;
import java.util.Set;

import org.apache.nifi.controller.ConfiguredComponent;

/**
 * Provides a collection of components that are referencing a Controller Service
 */
public interface ControllerServiceReference {

    /**
     * @return the component that is being referenced
     */
    ControllerServiceNode getReferencedComponent();

    /**
     * @return a {@link Set} of all components that are referencing this
     * Controller Service
     */
    Set<ConfiguredComponent> getReferencingComponents();

    /**
     * @return a {@link Set} of all Processors, Reporting Tasks, and Controller
     * Services that are referencing the Controller Service and are running (in
     * the case of Processors and Reporting Tasks) or enabled (in the case of
     * Controller Services)
     */
    Set<ConfiguredComponent> getActiveReferences();

    /**
     * Returns a List of all components that reference this Controller Service (recursively) that
     * are of the given type
     *
     * @param componentType the type of component that is desirable
     * @return a List of all components that reference this Controller Service that are of the given type
     */
    <T> List<T> findRecursiveReferences(Class<T> componentType);
}
