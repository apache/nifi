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
package org.apache.nifi.web.connector.authorization;

import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.ConnectorWebMethod;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * An InvocationHandler that wraps a Connector instance and enforces authorization
 * based on the {@link ConnectorWebMethod} annotation present on the invoked method.
 *
 * <p>Methods must be annotated with {@link ConnectorWebMethod} to be invokable through
 * this handler. The annotation specifies whether READ or WRITE access is required.
 * Methods without the annotation will result in an {@link IllegalStateException}.</p>
 *
 * @param <T> the type of the Connector being proxied
 */
public class AuthorizingConnectorInvocationHandler<T> implements InvocationHandler {

    private final T delegate;
    private final String connectorId;
    private final Authorizer authorizer;
    private final AuthorizableLookup authorizableLookup;

    /**
     * Constructs an AuthorizingConnectorInvocationHandler.
     *
     * @param delegate the actual Connector instance to delegate method calls to
     * @param connectorId the ID of the connector, used for authorization lookups
     * @param authorizer the Authorizer to use for authorization checks
     * @param authorizableLookup the lookup service to obtain the Authorizable for the connector
     */
    public AuthorizingConnectorInvocationHandler(final T delegate, final String connectorId,
                                                  final Authorizer authorizer, final AuthorizableLookup authorizableLookup) {
        this.delegate = delegate;
        this.connectorId = connectorId;
        this.authorizer = authorizer;
        this.authorizableLookup = authorizableLookup;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final ConnectorWebMethod annotation = findConnectorWebMethodAnnotation(method);

        if (annotation == null) {
            throw new IllegalStateException(String.format(
                    "Method [%s] on connector [%s] is not annotated with @ConnectorWebMethod and cannot be invoked through the Connector Web Context",
                    method.getName(), connectorId));
        }

        final RequestAction requiredAction = mapAccessTypeToRequestAction(annotation.value());
        final Authorizable connector = authorizableLookup.getConnector(connectorId);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        connector.authorize(authorizer, requiredAction, user);

        try {
            return method.invoke(delegate, args);
        } catch (final InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Maps the ConnectorWebMethod.AccessType to the corresponding RequestAction.
     *
     * @param accessType the access type from the annotation
     * @return the corresponding RequestAction
     */
    private RequestAction mapAccessTypeToRequestAction(final ConnectorWebMethod.AccessType accessType) {
        return switch (accessType) {
            case READ -> RequestAction.READ;
            case WRITE -> RequestAction.WRITE;
        };
    }

    /**
     * Finds the ConnectorWebMethod annotation on the given method. This method searches
     * the declaring class's interfaces to find the annotation, as the method parameter
     * may be from the proxy class rather than the interface.
     *
     * @param method the method to search for the annotation
     * @return the ConnectorWebMethod annotation, or null if not found
     */
    private ConnectorWebMethod findConnectorWebMethodAnnotation(final Method method) {
        final ConnectorWebMethod directAnnotation = method.getAnnotation(ConnectorWebMethod.class);
        if (directAnnotation != null) {
            return directAnnotation;
        }

        for (final Class<?> iface : delegate.getClass().getInterfaces()) {
            try {
                final Method interfaceMethod = iface.getMethod(method.getName(), method.getParameterTypes());
                final ConnectorWebMethod interfaceAnnotation = interfaceMethod.getAnnotation(ConnectorWebMethod.class);
                if (interfaceAnnotation != null) {
                    return interfaceAnnotation;
                }
            } catch (final NoSuchMethodException ignored) {
                // Method not found on this interface; continue searching other interfaces
                continue;
            }
        }

        return null;
    }
}

