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
package org.apache.nifi.web.connector;

import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.web.NiFiConnectorWebContext;
import org.apache.nifi.web.connector.authorization.AuthorizingConnectorInvocationHandler;
import org.apache.nifi.web.connector.authorization.AuthorizingFlowContext;
import org.apache.nifi.web.connector.authorization.ConnectorAuthorizationContext;
import org.apache.nifi.web.dao.ConnectorDAO;

import java.lang.reflect.Proxy;

/**
 * Implements the NiFiConnectorWebContext interface to provide
 * Connector instances to connector custom UIs.
 *
 * <p>The returned Connector instance is wrapped in an authorization proxy that
 * enforces permissions based on the {@link ConnectorWebMethod} annotation on
 * the connector interface methods. Methods without this annotation cannot be
 * invoked through the proxy.</p>
 *
 * <p>The returned FlowContext instances are also wrapped in authorization wrappers
 * that enforce read/write permissions on all operations.</p>
 */
public class StandardNiFiConnectorWebContext implements NiFiConnectorWebContext {

    private ConnectorDAO connectorDAO;
    private Authorizer authorizer;
    private AuthorizableLookup authorizableLookup;

    @Override
    @SuppressWarnings("unchecked")
    public <T> ConnectorWebContext<T> getConnectorWebContext(final String connectorId) throws IllegalArgumentException {
        final ConnectorNode connectorNode = connectorDAO.getConnector(connectorId);
        if (connectorNode == null) {
            throw new IllegalArgumentException("Unable to find connector with id: " + connectorId);
        }

        final ConnectorAuthorizationContext authContext = new ConnectorAuthorizationContext(connectorId, authorizer, authorizableLookup);

        final T connector = (T) connectorNode.getConnector();
        final T authorizedConnectorProxy = createAuthorizingProxy(connector, connectorId);

        final FlowContext workingFlowContext = new AuthorizingFlowContext(connectorNode.getWorkingFlowContext(), authContext);
        final FlowContext activeFlowContext = new AuthorizingFlowContext(connectorNode.getActiveFlowContext(), authContext);

        return new ConnectorWebContext<>(authorizedConnectorProxy, workingFlowContext, activeFlowContext);
    }

    /**
     * Creates a proxy around the given connector that enforces authorization
     * based on {@link ConnectorWebMethod} annotations.
     *
     * @param <T> the type of the connector
     * @param connector the connector instance to wrap
     * @param connectorId the ID of the connector
     * @return a proxy that enforces authorization on method invocations
     */
    @SuppressWarnings("unchecked")
    private <T> T createAuthorizingProxy(final T connector, final String connectorId) {
        final AuthorizingConnectorInvocationHandler<T> handler = new AuthorizingConnectorInvocationHandler<>(
                connector, connectorId, authorizer, authorizableLookup);

        return (T) Proxy.newProxyInstance(
                connector.getClass().getClassLoader(),
                connector.getClass().getInterfaces(),
                handler);
    }

    public void setConnectorDAO(final ConnectorDAO connectorDAO) {
        this.connectorDAO = connectorDAO;
    }

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setAuthorizableLookup(final AuthorizableLookup authorizableLookup) {
        this.authorizableLookup = authorizableLookup;
    }
}
