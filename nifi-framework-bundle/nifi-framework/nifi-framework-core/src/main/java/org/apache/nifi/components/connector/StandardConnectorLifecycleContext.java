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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Standard implementation of {@link ConnectorLifecycleContext}.
 */
public class StandardConnectorLifecycleContext implements ConnectorLifecycleContext {
    private final Map<String, String> properties;
    private final EventReporter eventReporter;
    private final NodeTypeProvider nodeTypeProvider;
    private final ConnectorClusterCoordinator clusterCoordinator;
    private final ConnectorRepository connectorRepository;
    private final ScheduledExecutorService scheduledExecutorService;

    public StandardConnectorLifecycleContext(final NiFiProperties nifiProperties, final NodeTypeProvider nodeTypeProvider,
                                             final ConnectorRepository connectorRepository, final EventReporter eventReporter) {
        this.properties = extractConnectorProperties(nifiProperties);
        this.eventReporter = eventReporter;
        this.nodeTypeProvider = nodeTypeProvider;
        this.connectorRepository = connectorRepository;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4, r -> {
            final Thread thread = new Thread(r, "Connector Lifecycle Manager Thread");
            thread.setDaemon(true);
            return thread;
        });

        // Use standalone coordinator for now - will be replaced with clustered implementation
        // when cluster coordination is properly initialized
        this.clusterCoordinator = nodeTypeProvider.isClustered()
                ? null  // Will be set by FlowController after cluster coordinator is available
                : ConnectorClusterCoordinator.standalone();
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
    public ConnectorClusterCoordinator getClusterCoordinator() {
        return clusterCoordinator;
    }

    @Override
    public ConnectorRepository getConnectorRepository() {
        return connectorRepository;
    }

    @Override
    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }
}
