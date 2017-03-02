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

package org.apache.nifi.processor;

import java.util.Set;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.service.ControllerServiceProvider;

public class ComponentSpecificControllerServiceLookup implements ControllerServiceLookup {
    private final ControllerServiceProvider serviceProvider;
    private final String componentId;
    private final String groupId;

    public ComponentSpecificControllerServiceLookup(final ControllerServiceProvider serviceProvider, final String componentId, final String groupId) {
        this.serviceProvider = serviceProvider;
        this.componentId = componentId;
        this.groupId = groupId;
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        return serviceProvider.getControllerServiceForComponent(serviceIdentifier, componentId);
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return serviceProvider.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return serviceProvider.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return serviceProvider.isControllerServiceEnabled(service);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) throws IllegalArgumentException {
        return serviceProvider.getControllerServiceIdentifiers(serviceType, groupId);
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        return serviceProvider.getControllerServiceName(serviceIdentifier);
    }
}
