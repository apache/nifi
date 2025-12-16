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

/**
 * Holds the authorization context needed to authorize operations on a Connector's FlowContext
 * and its associated facades.
 */
public class ConnectorAuthorizationContext {

    private final String connectorId;
    private final Authorizer authorizer;
    private final AuthorizableLookup authorizableLookup;

    public ConnectorAuthorizationContext(final String connectorId, final Authorizer authorizer, final AuthorizableLookup authorizableLookup) {
        this.connectorId = connectorId;
        this.authorizer = authorizer;
        this.authorizableLookup = authorizableLookup;
    }

    /**
     * Authorizes the current user for read access to the connector.
     */
    public void authorizeRead() {
        authorize(RequestAction.READ);
    }

    /**
     * Authorizes the current user for write access to the connector.
     */
    public void authorizeWrite() {
        authorize(RequestAction.WRITE);
    }

    private void authorize(final RequestAction action) {
        final Authorizable connector = authorizableLookup.getConnector(connectorId);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        connector.authorize(authorizer, action, user);
    }

    public String getConnectorId() {
        return connectorId;
    }

    public Authorizer getAuthorizer() {
        return authorizer;
    }

    public AuthorizableLookup getAuthorizableLookup() {
        return authorizableLookup;
    }
}

