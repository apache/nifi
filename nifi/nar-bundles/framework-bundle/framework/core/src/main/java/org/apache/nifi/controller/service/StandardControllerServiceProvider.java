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

import static java.util.Objects.requireNonNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ControllerServiceAlreadyExistsException;
import org.apache.nifi.controller.exception.ControllerServiceNotFoundException;
import org.apache.nifi.controller.exception.ProcessorLifeCycleException;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StandardControllerServiceProvider implements ControllerServiceProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceProvider.class);

    private final Map<String, ControllerServiceNode> controllerServices;
    private static final Set<Method> validDisabledMethods;

    static {
        // methods that are okay to be called when the service is disabled.
        final Set<Method> validMethods = new HashSet<>();
        for (final Method method : ControllerService.class.getMethods()) {
            validMethods.add(method);
        }
        for (final Method method : Object.class.getMethods()) {
            validMethods.add(method);
        }
        validDisabledMethods = Collections.unmodifiableSet(validMethods);
    }

    public StandardControllerServiceProvider() {
        // the following 2 maps must be updated atomically, but we do not lock around them because they are modified
        // only in the createControllerService method, and both are modified before the method returns
        this.controllerServices = new ConcurrentHashMap<>();
    }

    private Class<?>[] getInterfaces(final Class<?> cls) {
        final List<Class<?>> allIfcs = new ArrayList<>();
        populateInterfaces(cls, allIfcs);
        return allIfcs.toArray(new Class<?>[allIfcs.size()]);
    }

    private void populateInterfaces(final Class<?> cls, final List<Class<?>> interfacesDefinedThusFar) {
        final Class<?>[] ifc = cls.getInterfaces();
        if (ifc != null && ifc.length > 0) {
            for (final Class<?> i : ifc) {
                interfacesDefinedThusFar.add(i);
            }
        }

        final Class<?> superClass = cls.getSuperclass();
        if (superClass != null) {
            populateInterfaces(superClass, interfacesDefinedThusFar);
        }
    }

    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final boolean firstTimeAdded) {
        if (type == null || id == null) {
            throw new NullPointerException();
        }
        if (controllerServices.containsKey(id)) {
            throw new ControllerServiceAlreadyExistsException(id);
        }

        final ClassLoader currentContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader cl = ExtensionManager.getClassLoader(type);
            Thread.currentThread().setContextClassLoader(cl);
            final Class<?> rawClass = Class.forName(type, false, cl);
            final Class<? extends ControllerService> controllerServiceClass = rawClass.asSubclass(ControllerService.class);

            final ControllerService originalService = controllerServiceClass.newInstance();
            final ObjectHolder<ControllerServiceNode> serviceNodeHolder = new ObjectHolder<>(null);
            final InvocationHandler invocationHandler = new InvocationHandler() {
                @Override
                public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                    final ControllerServiceNode node = serviceNodeHolder.get();
                    if (node.isDisabled() && !validDisabledMethods.contains(method)) {
                        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                            throw new IllegalStateException("Cannot invoke method " + method + " on Controller Service " + originalService + " because the Controller Service is disabled");
                        } catch (final Throwable e) {
                            throw new IllegalStateException("Cannot invoke method " + method + " on Controller Service with identifier " + id + " because the Controller Service is disabled");
                        }
                    }

                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return method.invoke(originalService, args);
                    } catch (final InvocationTargetException e) {
                        // If the ControllerService throws an Exception, it'll be wrapped in an InvocationTargetException. We want
                        // to instead re-throw what the ControllerService threw, so we pull it out of the InvocationTargetException.
                        throw e.getCause();
                    }
                }
            };

            final ControllerService proxiedService = (ControllerService) Proxy.newProxyInstance(cl, getInterfaces(controllerServiceClass), invocationHandler);
            logger.info("Loaded service {} as configured.", type);

            originalService.initialize(new StandardControllerServiceInitializationContext(id, this));

            final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(this);

            final ControllerServiceNode serviceNode = new StandardControllerServiceNode(proxiedService, originalService, id, validationContextFactory, this);
            serviceNodeHolder.set(serviceNode);
            serviceNode.setAnnotationData(null);
            serviceNode.setName(id);
            
            if ( firstTimeAdded ) {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, originalService);
                } catch (final Exception e) {
                    throw new ProcessorLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + originalService, e);
                }
            }

            this.controllerServices.put(id, serviceNode);
            return serviceNode;
        } catch (final Throwable t) {
            throw new ControllerServiceNotFoundException(t);
        } finally {
            if (currentContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentContextClassLoader);
            }
        }
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return (node == null) ? null : node.getProxiedControllerService();
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return isControllerServiceEnabled(service.getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        final ControllerServiceNode node = controllerServices.get(serviceIdentifier);
        return (node == null) ? false : !node.isDisabled();
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String serviceIdentifier) {
        return controllerServices.get(serviceIdentifier);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        final Set<String> identifiers = new HashSet<>();
        for (final Map.Entry<String, ControllerServiceNode> entry : controllerServices.entrySet()) {
            if (requireNonNull(serviceType).isAssignableFrom(entry.getValue().getProxiedControllerService().getClass())) {
                identifiers.add(entry.getKey());
            }
        }

        return identifiers;
    }
    
    @Override
    public void removeControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = controllerServices.get(serviceNode.getIdentifier());
        if ( existing == null || existing != serviceNode ) {
            throw new IllegalStateException("Controller Service " + serviceNode + " does not exist in this Flow");
        }
        
        serviceNode.verifyCanDelete();
        
        try (final NarCloseable x = NarCloseable.withNarLoader()) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(serviceNode, this);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, serviceNode.getControllerServiceImplementation(), configurationContext);
        }
        
        controllerServices.remove(serviceNode.getIdentifier());
    }
}
