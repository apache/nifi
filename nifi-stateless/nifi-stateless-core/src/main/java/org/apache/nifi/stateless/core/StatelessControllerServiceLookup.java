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
package org.apache.nifi.stateless.core;

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.InitializationException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class StatelessControllerServiceLookup implements ControllerServiceLookup {

    private final Map<String, StatelessControllerServiceConfiguration> controllerServiceMap = new ConcurrentHashMap<>();
    private final Map<String, SLF4JComponentLog> controllerServiceLoggers = new HashMap<>();
    private final Map<String, StatelessStateManager> controllerServiceStateManagers = new HashMap<>();

    public Map<String, StatelessControllerServiceConfiguration> getControllerServices() {
        return controllerServiceMap;
    }


    public void addControllerService(final ControllerService service) throws InitializationException {
        final String identifier = service.getIdentifier();
        final SLF4JComponentLog logger = new SLF4JComponentLog(service);
        controllerServiceLoggers.put(identifier, logger);

        StatelessStateManager serviceStateManager = new StatelessStateManager();
        controllerServiceStateManagers.put(identifier, serviceStateManager);

        final StatelessProcessContext initContext = new StatelessProcessContext(requireNonNull(service), this, requireNonNull(identifier), logger, serviceStateManager);
        service.initialize(initContext);

        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, service);
        } catch (final InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
            throw new InitializationException(e);
        }

        final StatelessControllerServiceConfiguration config = new StatelessControllerServiceConfiguration(service);
        controllerServiceMap.put(identifier, config);
    }


    protected StatelessControllerServiceConfiguration getConfiguration(final String identifier) {
        return controllerServiceMap.get(identifier);
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        final StatelessControllerServiceConfiguration status = controllerServiceMap.get(identifier);
        return (status == null) ? null : status.getService();
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final StatelessControllerServiceConfiguration status = controllerServiceMap.get(serviceIdentifier);
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
        for (final Map.Entry<String, StatelessControllerServiceConfiguration> entry : controllerServiceMap.entrySet()) {
            if (serviceType.isAssignableFrom(entry.getValue().getService().getClass())) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final StatelessControllerServiceConfiguration status = controllerServiceMap.get(serviceIdentifier);
        return status == null ? null : serviceIdentifier;
    }

    public void enableControllerService(final ControllerService service, VariableRegistry registry) throws InvocationTargetException, IllegalAccessException {
        final StatelessControllerServiceConfiguration configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Cannot enable Controller Service " + service + " because it is not disabled");
        }
        final ConfigurationContext configContext = new StatelessConfigurationContext(service, configuration.getProperties(), this, registry);
        ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, service, configContext);

        configuration.setEnabled(true);
    }


    public void setControllerServiceAnnotationData(final ControllerService service, final String annotationData) {
        final StatelessControllerServiceConfiguration configuration = getControllerServiceConfigToUpdate(service);
        configuration.setAnnotationData(annotationData);
    }

    private StatelessControllerServiceConfiguration getControllerServiceConfigToUpdate(final ControllerService service) {
        final StatelessControllerServiceConfiguration configuration = getConfiguration(service.getIdentifier());
        if (configuration == null) {
            throw new IllegalArgumentException("Controller Service " + service + " is not known");
        }

        if (configuration.isEnabled()) {
            throw new IllegalStateException("Controller service " + service + " cannot be modified because it is not disabled");
        }

        return configuration;
    }

    public ValidationResult setControllerServiceProperty(final ControllerService service, final PropertyDescriptor property, final StatelessProcessContext context, final VariableRegistry registry, final
    String value) {
        final StatelessStateManager serviceStateManager = controllerServiceStateManagers.get(service.getIdentifier());
        if (serviceStateManager == null) {
            throw new IllegalStateException("Controller service " + service + " has not been added to this TestRunner via the #addControllerService method");
        }

        final ValidationContext validationContext = new StatelessValidationContext(context, this, serviceStateManager, registry).getControllerServiceValidationContext(service);
        final ValidationResult validationResult = property.validate(value, validationContext);

        final StatelessControllerServiceConfiguration configuration = getControllerServiceConfigToUpdate(service);
        final String oldValue = configuration.getProperties().get(property);
        configuration.setProperty(property, value);

        if ((value == null && oldValue != null) || (value != null && !value.equals(oldValue))) {
            service.onPropertyModified(property, oldValue, value);
        }

        return validationResult;
    }

}
