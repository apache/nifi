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
package org.apache.nifi.fn.core;

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.reporting.InitializationException;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class FnControllerServiceLookup implements ControllerServiceLookup {


    private final Map<String, FnControllerServiceConfiguration> controllerServiceMap = new ConcurrentHashMap<>();
    private final Map<String, SLF4JComponentLog> controllerServiceLoggers = new HashMap<>();
    private final Map<String, FnStateManager> controllerServiceStateManagers = new HashMap<>();


    public Map<String, FnControllerServiceConfiguration> getControllerServices() {
        return controllerServiceMap;
    }

    public void addControllerService(final VersionedControllerService versionedControllerService) throws InitializationException {
        String id = versionedControllerService.getIdentifier();
        ControllerService service = ReflectionUtils.createControllerService(versionedControllerService);
        Map<String, String> properties = versionedControllerService.getProperties();

        addControllerService(id,service,properties);

    }
    public void addControllerService(final String identifier, final ControllerService service, final Map<String, String> properties) throws InitializationException {
        final SLF4JComponentLog logger = new SLF4JComponentLog(service);
        controllerServiceLoggers.put(identifier, logger);

        FnStateManager serviceStateManager = new FnStateManager();
        controllerServiceStateManagers.put(identifier, serviceStateManager);

        final FnProcessContext initContext = new FnProcessContext(requireNonNull(service), this, requireNonNull(identifier), logger, serviceStateManager);
        service.initialize(initContext);

        final Map<PropertyDescriptor, String> resolvedProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            resolvedProps.put(service.getPropertyDescriptor(entry.getKey()), entry.getValue());
        }

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, service);
        } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new InitializationException(e);
        }

        final FnControllerServiceConfiguration config = new FnControllerServiceConfiguration(service);
        controllerServiceMap.put(identifier, config);
    }

    public void removeControllerService(final ControllerService service) throws InvocationTargetException, IllegalAccessException {
        final ControllerService canonical = getControllerService(service.getIdentifier());

        disableControllerService(canonical);

        ReflectionUtils.invokeMethodsWithAnnotation(OnRemoved.class, canonical);

        if (canonical == null || canonical != service) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        controllerServiceMap.remove(service.getIdentifier());
    }


    protected FnControllerServiceConfiguration getConfiguration(final String identifier) {
        return controllerServiceMap.get(identifier);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        final FnControllerServiceConfiguration status = controllerServiceMap.get(identifier);
        return (status == null) ? null : status.getService();
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final FnControllerServiceConfiguration status = controllerServiceMap.get(serviceIdentifier);
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
        for (final Map.Entry<String, FnControllerServiceConfiguration> entry : controllerServiceMap.entrySet()) {
            if (serviceType.isAssignableFrom(entry.getValue().getService().getClass())) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final FnControllerServiceConfiguration status = controllerServiceMap.get(serviceIdentifier);
        return status == null ? null : serviceIdentifier;
    }


    public void disableControllerService(final ControllerService service) throws InvocationTargetException, IllegalAccessException {
        final FnControllerServiceConfiguration configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (!configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be disabled because it is not enabled");
        }

        ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, service);


        configuration.setEnabled(false);
    }
    public void enableControllerService(final ControllerService service, VariableRegistry registry) throws InvocationTargetException, IllegalAccessException {
        final FnControllerServiceConfiguration configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is not disabled");
        }
        final ConfigurationContext configContext = new FnConfigurationContext(service, configuration.getProperties(), this, registry);
        ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service, configContext);


        configuration.setEnabled(true);
    }

    public SLF4JComponentLog getControllerServiceLogger(final String identifier) {
        return controllerServiceLoggers.get(identifier);
    }


    Map<PropertyDescriptor, String> getControllerServiceProperties(final ControllerService controllerService) {
        return this.getConfiguration(controllerService.getIdentifier()).getProperties();
    }
    String getControllerServiceAnnotationData(final ControllerService controllerService) {
        return this.getConfiguration(controllerService.getIdentifier()).getAnnotationData();
    }
    public FnStateManager getStateManager(final ControllerService controllerService) {
        return controllerServiceStateManagers.get(controllerService.getIdentifier());
    }
    public void setControllerServiceAnnotationData(final ControllerService service, final String annotationData) {
        final FnControllerServiceConfiguration configuration = getControllerServiceConfigToUpdate(service);
        configuration.setAnnotationData(annotationData);
    }
    private FnControllerServiceConfiguration getControllerServiceConfigToUpdate(final ControllerService service) {
        final FnControllerServiceConfiguration configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be modified because it is not disabled");
        }

        return configuration;
    }
    public ValidationResult setControllerServiceProperty(final ControllerService service, final PropertyDescriptor property, final FnProcessContext context, final VariableRegistry registry, final String value) {
        final FnStateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller service " + service + " has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new FnValidationContext(context, this, serviceStateManager, registry).getControllerServiceValidationContext(service);
        final ValidationResult validationResult = property.validate(value, validationContext);

        final FnControllerServiceConfiguration configuration = getControllerServiceConfigToUpdate(service);
        final String oldValue = configuration.getProperties().get(property);
        configuration.setProperty(property,value);

        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(property, oldValue, value);
        }

        return validationResult;
    }





}
