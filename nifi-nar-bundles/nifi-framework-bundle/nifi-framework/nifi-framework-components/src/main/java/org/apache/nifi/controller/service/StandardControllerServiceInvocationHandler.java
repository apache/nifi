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

import org.apache.commons.lang3.ClassUtils;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceProxyWrapper;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class StandardControllerServiceInvocationHandler implements ControllerServiceInvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(StandardControllerServiceInvocationHandler.class);
    private static final Method PROXY_WRAPPER_GET_WRAPPED_METHOD;

    private static final Set<Method> validDisabledMethods;
    static {
        // methods that are okay to be called when the service is disabled.
        final Set<Method> validMethods = new HashSet<>();
        validMethods.addAll(Arrays.asList(ControllerService.class.getMethods()));
        validMethods.addAll(Arrays.asList(Object.class.getMethods()));
        validDisabledMethods = Collections.unmodifiableSet(validMethods);

        try {
            PROXY_WRAPPER_GET_WRAPPED_METHOD = ControllerServiceProxyWrapper.class.getMethod("getWrapped");
        } catch (final NoSuchMethodException e) {
            throw new AssertionError("Could not find getWrapped Method for ProxyWrapper");
        }
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
            // We can blindly throw UnsupportedOperationException because these methods will only ever be invoked by the framework directly
            // on the controller service implementation, not on this proxy object.
            throw new UnsupportedOperationException(method + " may only be invoked by the NiFi framework");
        }

        final ControllerServiceNode node = serviceNodeHolder.get();
        final ControllerServiceState state = node.getState();
        final boolean disabled = state != ControllerServiceState.ENABLED; // only allow method call if service state is ENABLED.
        if (disabled && !validDisabledMethods.contains(method)) {
            throw new ControllerServiceDisabledException(node.getIdentifier(), "Cannot invoke method " + method + " on Controller Service with identifier "
                + serviceNodeHolder.get().getIdentifier() + " because the Controller Service's State is currently " + state);
        }

        final ClassLoader callerClassLoader = Thread.currentThread().getContextClassLoader();
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, originalService.getClass(), originalService.getIdentifier())) {
            // If any objects are proxied, unwrap them so that we provide the unproxied object to the Controller Service.
            ClassLoader serviceClassLoader = Thread.currentThread().getContextClassLoader();

            return invoke(originalService, method, args, serviceClassLoader, callerClassLoader);
        } catch (final InvocationTargetException e) {
            // If the ControllerService throws an Exception, it'll be wrapped in an InvocationTargetException. We want
            // to instead re-throw what the ControllerService threw, so we pull it out of the InvocationTargetException.
            throw e.getCause();
        }
    }

    private boolean isInHierarchy(final ClassLoader objectClassLoader, final ClassLoader classLoaderHierarchy) {
        if (classLoaderHierarchy == null) {
            return false;
        }
        if (objectClassLoader == classLoaderHierarchy) {
            return true;
        }
        return isInHierarchy(objectClassLoader, classLoaderHierarchy.getParent());
    }

    private Object proxy(final Object bareObject, final Class<?> declaredType) {
        if (bareObject == null) {
            return null;
        }

        // We only want to proxy the object if the object is defined by the method that
        // was invoked as being an interface. For example, if a method is expected to return a java.lang.String,
        // we do not want to instead return a proxy because the Proxy won't be a String.
        if (declaredType == null || !declaredType.isInterface()) {
            return bareObject;
        }

        // If the ClassLoader is null, we have a primitive type, which we can't proxy.
        if (bareObject.getClass().getClassLoader() == null) {
            return bareObject;
        }

        // The proxy that is to be returned needs to ensure that it implements all interfaces that are defined by the
        // object. We cannot simply implement the return that that is defined, because the code that receives the object
        // may perform further inspection. For example, consider that a javax.jms.Message is returned. If this method proxies
        // only that method, but the object itself is a javax.jms.BytesMessage, then code such as the following will result in `isBytes == false`
        // when it should be `true`:
        //
        // final javax.jms.Message myMessage = controllerService.getMessage();
        // final boolean isBytes = myMessage instanceof javax.jms.BytesMessage;
        final List<Class<?>> interfaces = ClassUtils.getAllInterfaces(bareObject.getClass());
        if (interfaces == null || interfaces.isEmpty()) {
            return bareObject;
        }

        // Add the ControllerServiceProxyWrapper to the List of interfaces to implement. See javadocs for ControllerServiceProxyWrapper
        // to understand why this is needed.
        if (!interfaces.contains(ControllerServiceProxyWrapper.class)) {
            interfaces.add(ControllerServiceProxyWrapper.class);
        }

        final Class<?>[] interfaceTypes = interfaces.toArray(new Class<?>[0]);
        final InvocationHandler invocationHandler = new ProxiedReturnObjectInvocationHandler(bareObject);
        return Proxy.newProxyInstance(bareObject.getClass().getClassLoader(), interfaceTypes, invocationHandler);
    }

    private Object[] unwrapProxies(final Object[] values, final ClassLoader expectedClassLoader, final Method method) {
        if (!containsWrappedProxy(values)) {
            return values;
        }

        final Object[] unwrappedValues = new Object[values.length];
        for (int i=0; i < values.length; i++) {
            unwrappedValues[i] = unwrap(values[i], expectedClassLoader, method);
        }

        return unwrappedValues;
    }

    private Object unwrap(final Object value, final ClassLoader expectedClassLoader, final Method method) {
        if (!isWrappedProxy(value)) {
            return value;
        }

        final ControllerServiceProxyWrapper<?> wrapper = (ControllerServiceProxyWrapper<?>) value;
        final Object wrapped = wrapper.getWrapped();
        if (wrapped == null) {
            return null;
        }

        final ClassLoader wrappedClassLoader = wrapped.getClass().getClassLoader();
        if (isInHierarchy(wrappedClassLoader, expectedClassLoader)) {
            logger.trace("Unwrapped {} to be used by {} when calling {}", wrapped, originalService, method);
            return wrapped;
        }

        logger.trace("Will not unwrap {} because even though it is a wrapped proxy object, the wrapped object's ClassLoader is {}, not {}", value, wrappedClassLoader, expectedClassLoader);
        return value;
    }

    private boolean containsWrappedProxy(final Object[] values) {
        if (values == null || values.length == 0) {
            return false;
        }

        for (final Object value : values) {
            if (isWrappedProxy(value)) {
                return true;
            }
        }

        return false;
    }

    private boolean isWrappedProxy(final Object value) {
        if (value == null) {
            return false;
        }

        final Class<?> valueClass = value.getClass();
        return ControllerServiceProxyWrapper.class.isAssignableFrom(valueClass) && Proxy.isProxyClass(valueClass);
    }

    private class ProxiedReturnObjectInvocationHandler implements InvocationHandler {
        private final Object bareObject;
        private ClassLoader bareObjectClassLoader;

        public ProxiedReturnObjectInvocationHandler(final Object bareObject) {
            this.bareObject = bareObject;
            this.bareObjectClassLoader = bareObject.getClass().getClassLoader();
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            if (PROXY_WRAPPER_GET_WRAPPED_METHOD.equals(method)) {
                return this.bareObject;
            }

            final ClassLoader callerClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(this.bareObjectClassLoader);

                return StandardControllerServiceInvocationHandler.this.invoke(this.bareObject, method, args, this.bareObjectClassLoader, callerClassLoader);
            } catch (final InvocationTargetException ite) {
                throw ite.getCause();
            } finally {
                Thread.currentThread().setContextClassLoader(callerClassLoader);
            }
        }
    }

    private Object invoke(Object bareObject, Method method, Object[] args, ClassLoader bareObjectClassLoader, ClassLoader callerClassLoader) throws IllegalAccessException, InvocationTargetException {
        // If any objects are proxied, unwrap them so that we provide the unproxied object to the Controller Service.
        final Object[] unwrappedArgs = unwrapProxies(args, bareObjectClassLoader, method);

        // Invoke the method on the underlying implementation
        final Object returnedFromBareObject = method.invoke(bareObject, unwrappedArgs);

        // If the return object is known to the caller, it can be returned directly. Otherwise, proxy the object so that
        // calls into the proxy are called through the appropriate ClassLoader.
        if (returnedFromBareObject == null || isInHierarchy(returnedFromBareObject.getClass().getClassLoader(), callerClassLoader)) {
            return returnedFromBareObject;
        }

        return proxy(returnedFromBareObject, method.getReturnType());
    }
}
