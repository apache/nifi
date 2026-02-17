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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.web.NiFiConnectorWebContext;

/**
 * Mock implementation of {@link NiFiConnectorWebContext} for the connector test runner.
 * Provides direct access to the connector instance and its flow contexts without
 * authorization proxying, since the mock server uses a permit-all authorizer.
 */
public class MockNiFiConnectorWebContext implements NiFiConnectorWebContext {

    private final ConnectorRepository connectorRepository;

    public MockNiFiConnectorWebContext(final ConnectorRepository connectorRepository) {
        this.connectorRepository = connectorRepository;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ConnectorWebContext<T> getConnectorWebContext(final String connectorId) throws IllegalArgumentException {
        final ConnectorNode connectorNode = connectorRepository.getConnector(connectorId);
        if (connectorNode == null) {
            throw new IllegalArgumentException("Unable to find connector with id: " + connectorId);
        }

        final T connector = (T) connectorNode.getConnector();
        final FlowContext workingFlowContext = connectorNode.getWorkingFlowContext();
        final FlowContext activeFlowContext = connectorNode.getActiveFlowContext();

        return new ConnectorWebContext<>(connector, workingFlowContext, activeFlowContext);
    }
}
