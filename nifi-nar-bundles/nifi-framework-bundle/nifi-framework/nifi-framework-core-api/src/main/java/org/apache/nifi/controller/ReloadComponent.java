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
package org.apache.nifi.controller;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;

import java.net.URL;
import java.util.Set;

/**
 * A service used to change the type of an underlying component.
 */
public interface ReloadComponent {

    /**
     * Changes the underlying Processor held by the node to an instance of the new type.
     *
     * @param existingNode the node being being updated
     * @param newType the fully qualified class name of the new type
     * @param bundleCoordinate the bundle coordinate of the new type
     * @param additionalUrls additional URLs to be added to the instance class loader of the new component
     * @throws ControllerServiceInstantiationException if unable to create an instance of the new type
     */
    void reload(ProcessorNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls)
            throws ProcessorInstantiationException;

    /**
     * Changes the underlying ControllerService held by the node to an instance of the new type.
     *
     * @param existingNode the node being being updated
     * @param newType the fully qualified class name of the new type
     * @param bundleCoordinate the bundle coordinate of the new type
     * @param additionalUrls additional URLs to be added to the instance class loader of the new component
     * @throws ControllerServiceInstantiationException if unable to create an instance of the new type
     */
    void reload(ControllerServiceNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls)
            throws ControllerServiceInstantiationException;

    /**
     * Changes the underlying ReportingTask held by the node to an instance of the new type.
     *
     * @param existingNode the ReportingTaskNode being updated
     * @param newType the fully qualified class name of the new type
     * @param bundleCoordinate the bundle coordinate of the new type
     * @param additionalUrls additional URLs to be added to the instance class loader of the new component
     * @throws ReportingTaskInstantiationException if unable to create an instance of the new type
     */
    void reload(ReportingTaskNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls)
            throws ReportingTaskInstantiationException;

}
