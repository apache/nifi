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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.logging.ComponentLog;

/**
 * Holder for StandardControllerServiceNode to atomically swap out the component.
 */
public class ControllerServiceDetails {

    private final ControllerService proxiedControllerService;
    private final ControllerService implementation;
    private final ComponentLog componentLog;
    private final BundleCoordinate bundleCoordinate;
    private final ControllerServiceInvocationHandler invocationHandler;

    public ControllerServiceDetails(final LoggableComponent<ControllerService> implementation,
                                    final LoggableComponent<ControllerService> proxiedControllerService,
                                    final ControllerServiceInvocationHandler invocationHandler) {
        this.proxiedControllerService = proxiedControllerService.getComponent();
        this.implementation = implementation.getComponent();
        this.componentLog = implementation.getLogger();
        this.bundleCoordinate = implementation.getBundleCoordinate();
        this.invocationHandler = invocationHandler;
    }

    public ControllerService getProxiedControllerService() {
        return proxiedControllerService;
    }

    public ControllerService getImplementation() {
        return implementation;
    }

    public ComponentLog getComponentLog() {
        return componentLog;
    }

    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }

    public ControllerServiceInvocationHandler getInvocationHandler() {
        return invocationHandler;
    }
}
