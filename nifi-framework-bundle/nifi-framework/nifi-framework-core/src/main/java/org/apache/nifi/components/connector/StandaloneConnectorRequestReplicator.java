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

package org.apache.nifi.components.connector;

import org.apache.nifi.controller.flow.FlowManager;

public class StandaloneConnectorRequestReplicator implements ConnectorRequestReplicator {

    private FlowManager flowManager;

    @Override
    public ConnectorState getState(final String connectorId) {
        final ConnectorNode connectorNode = flowManager.getConnector(connectorId);
        if (connectorNode == null) {
            throw new IllegalArgumentException("No connector found with id: " + connectorId);
        }

        return connectorNode.getCurrentState();
    }

    @Override
    public void setFlowManager(final FlowManager flowManager) {
        this.flowManager = flowManager;
    }
}
