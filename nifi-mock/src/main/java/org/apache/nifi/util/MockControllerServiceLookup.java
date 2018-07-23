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
package org.apache.nifi.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;

public abstract class MockControllerServiceLookup implements ControllerServiceLookup {

    private final Map<String, ControllerServiceConfiguration> controllerServiceMap = new ConcurrentHashMap<>();

    public Map<String, ControllerServiceConfiguration> getControllerServices() {
        return controllerServiceMap;
    }

    public ControllerServiceConfiguration addControllerService(final ControllerService service, final String identifier) {
        final ControllerServiceConfiguration config = new ControllerServiceConfiguration(service);
        controllerServiceMap.put(identifier, config);
        return config;
    }

    public ControllerServiceConfiguration addControllerService(final ControllerService service) {
        return addControllerService(service, service.getIdentifier());
    }

    public void removeControllerService(final ControllerService service) {
        final ControllerService canonical = getControllerService(service.getIdentifier());
        if (canonical == null || canonical != service) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        controllerServiceMap.remove(service.getIdentifier());
    }

    protected void addControllerServices(final MockControllerServiceLookup other) {
        this.controllerServiceMap.putAll(other.controllerServiceMap);
    }

    protected ControllerServiceConfiguration getConfiguration(final String identifier) {
        return controllerServiceMap.get(identifier);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        final ControllerServiceConfiguration status = controllerServiceMap.get(identifier);
        return (status == null) ? null : status.getService();
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final ControllerServiceConfiguration status = controllerServiceMap.get(serviceIdentifier);
        if (status == null) {
            throw new IllegalArgumentException("No ControllerService exists with identifier " + serviceIdentifier);
        }

        return status.isEnabled();
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return false;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        final Set<String> ids = new HashSet<>();
        for (final Map.Entry<String, ControllerServiceConfiguration> entry : controllerServiceMap.entrySet()) {
            if (serviceType.isAssignableFrom(entry.getValue().getService().getClass())) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final ControllerServiceConfiguration status = controllerServiceMap.get(serviceIdentifier);
        return status == null ? null : serviceIdentifier;
    }

    public InputRequirement getInputRequirement() {
        return null;
    }
}
