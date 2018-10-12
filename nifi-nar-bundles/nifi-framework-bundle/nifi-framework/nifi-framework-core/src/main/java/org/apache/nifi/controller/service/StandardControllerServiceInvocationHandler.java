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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class StandardControllerServiceInvocationHandler implements ControllerServiceInvocationHandler {

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

    private final ControllerService originalService;
    private final AtomicReference<ControllerServiceNode> serviceNodeHolder = new AtomicReference<>(null);
    private final ExtensionManager extensionManager;

    /**
     * @param originalService the original service being proxied
     */
    public StandardControllerServiceInvocationHandler(final ExtensionManager extensionManager, final ControllerService originalService) {
        this(extensionManager, originalService, null);
    }

    /**
     * @param originalService the original service being proxied
     * @param serviceNode the node holding the original service which will be used for checking the state (disabled vs running)
     */
    public StandardControllerServiceInvocationHandler(final ExtensionManager extensionManager, final ControllerService originalService, final ControllerServiceNode serviceNode) {
        this.extensionManager = extensionManager;
        this.originalService = originalService;
        this.serviceNodeHolder.set(serviceNode);
    }

    @Override
    public void setServiceNode(final ControllerServiceNode serviceNode) {
        this.serviceNodeHolder.set(serviceNode);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final String methodName = method.getName();
        if ("initialize".equals(methodName) || "onPropertyModified".equals(methodName)) {
            throw new UnsupportedOperationException(method + " may only be invoked by the NiFi framework");
        }

        final ControllerServiceNode node = serviceNodeHolder.get();
        final ControllerServiceState state = node.getState();
        final boolean disabled = state != ControllerServiceState.ENABLED; // only allow method call if service state is ENABLED.
        if (disabled && !validDisabledMethods.contains(method)) {
            throw new ControllerServiceDisabledException(node.getIdentifier(), "Cannot invoke method " + method + " on Controller Service with identifier "
                + serviceNodeHolder.get().getIdentifier() + " because the Controller Service's State is currently " + state);
        }

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, originalService.getClass(), originalService.getIdentifier())) {
            return method.invoke(originalService, args);
        } catch (final InvocationTargetException e) {
            // If the ControllerService throws an Exception, it'll be wrapped in an InvocationTargetException. We want
            // to instead re-throw what the ControllerService threw, so we pull it out of the InvocationTargetException.
            throw e.getCause();
        }
    }

}
