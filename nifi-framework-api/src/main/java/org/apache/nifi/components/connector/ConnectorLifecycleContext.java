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

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides context information for initializing a {@link ConnectorLifecycleManager}.
 */
public interface ConnectorLifecycleContext {

    /**
     * Returns the configuration properties for the lifecycle manager.
     * These properties are typically specified in nifi.properties.
     *
     * @return the configuration properties
     */
    Map<String, String> getProperties();

    /**
     * Returns the event reporter that can be used to report events and create bulletins.
     *
     * @return the event reporter
     */
    EventReporter getEventReporter();

    /**
     * Returns the node type provider that provides information about the node's
     * cluster status (e.g., whether it is clustered, primary, etc.).
     *
     * @return the node type provider
     */
    NodeTypeProvider getNodeTypeProvider();

    /**
     * Returns the cluster coordinator that provides cluster coordination capabilities.
     * May be null if running in standalone mode or if cluster coordination is not available.
     *
     * @return the cluster coordinator, or null if not available
     */
    ConnectorClusterCoordinator getClusterCoordinator();

    /**
     * Returns the connector repository for persistence operations.
     *
     * @return the connector repository
     */
    ConnectorRepository getConnectorRepository();

    /**
     * Returns a scheduled executor service that can be used for scheduling
     * lifecycle-related tasks.
     *
     * @return the scheduled executor service
     */
    ScheduledExecutorService getScheduledExecutorService();
}
