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

import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.util.NiFiProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Standard implementation of {@link ConnectorRepositoryContext}.
 */
public class StandardConnectorRepositoryContext implements ConnectorRepositoryContext {
    private final Map<String, String> properties;
    private final EventReporter eventReporter;
    private final NodeTypeProvider nodeTypeProvider;

    public StandardConnectorRepositoryContext(final NiFiProperties nifiProperties, final NodeTypeProvider nodeTypeProvider,
                                              final EventReporter eventReporter) {
        this.properties = extractConnectorProperties(nifiProperties);
        this.eventReporter = eventReporter;
        this.nodeTypeProvider = nodeTypeProvider;
    }

    private Map<String, String> extractConnectorProperties(final NiFiProperties nifiProperties) {
        final Map<String, String> connectorProps = new HashMap<>();
        for (final String propertyName : nifiProperties.getPropertyKeys()) {
            if (propertyName.startsWith("nifi.components.connectors.")) {
                connectorProps.put(propertyName, nifiProperties.getProperty(propertyName));
            }
        }
        return connectorProps;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public EventReporter getEventReporter() {
        return eventReporter;
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }

    @Override
    public PersistedConnectorRecord.Builder createRecordBuilder() {
        return new StandardPersistedConnectorRecord.Builder();
    }
}
