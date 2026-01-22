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

/**
 * Provides context information for initializing a {@link ConnectorRepository}.
 */
public interface ConnectorRepositoryContext {

    /**
     * Returns the configuration properties for the connector repository.
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
     * Creates a new builder for {@link PersistedConnectorRecord} instances.
     *
     * @return a new builder
     */
    PersistedConnectorRecord.Builder createRecordBuilder();
}
