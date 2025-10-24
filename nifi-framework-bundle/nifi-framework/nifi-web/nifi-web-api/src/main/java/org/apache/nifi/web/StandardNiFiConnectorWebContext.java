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
package org.apache.nifi.web;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.web.dao.ConnectorDAO;

/**
 * Implements the NiFiConnectorWebContext interface to provide
 * Connector instances to connector custom UIs.
 */
public class StandardNiFiConnectorWebContext implements NiFiConnectorWebContext {

    private ConnectorDAO connectorDAO;

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConnector(final String connectorId) throws IllegalArgumentException {
        final ConnectorNode connectorNode = connectorDAO.getConnector(connectorId);
        if (connectorNode == null) {
            throw new IllegalArgumentException("Unable to find connector with id: " + connectorId);
        }
        return (T) connectorNode.getConnector();
    }

    public void setConnectorDAO(final ConnectorDAO connectorDAO) {
        this.connectorDAO = connectorDAO;
    }
}

