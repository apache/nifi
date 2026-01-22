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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;

import java.util.List;

/**
 * Represents a connector's persisted state, including its configuration and metadata.
 * This record contains all information necessary to restore a connector on startup.
 */
public interface PersistedConnectorRecord {

    /**
     * Returns the unique identifier of the connector.
     *
     * @return the connector identifier
     */
    String getIdentifier();

    /**
     * Returns the user-defined name of the connector.
     *
     * @return the connector name
     */
    String getName();

    /**
     * Returns the fully qualified class name of the connector implementation.
     *
     * @return the connector type
     */
    String getType();

    /**
     * Returns the bundle coordinate that identifies the NAR containing this connector's implementation.
     *
     * @return the bundle coordinate
     */
    BundleCoordinate getBundleCoordinate();

    /**
     * Returns the desired state of the connector (e.g., RUNNING, STOPPED).
     *
     * @return the desired connector state
     */
    ConnectorState getDesiredState();

    /**
     * Returns the active flow configuration steps. The active configuration represents
     * the configuration that is currently being used by the running connector.
     *
     * @return the list of active configuration steps, or an empty list if none
     */
    List<VersionedConfigurationStep> getActiveConfiguration();

    /**
     * Returns the working flow configuration steps. The working configuration represents
     * configuration changes that have been made but not yet applied to the active flow.
     *
     * @return the list of working configuration steps, or an empty list if none
     */
    List<VersionedConfigurationStep> getWorkingConfiguration();

    /**
     * Returns the bundle associated with the flow context. This bundle identifies
     * the NAR that was used to create the flow configuration.
     *
     * @return the flow context bundle, or null if not set
     */
    Bundle getFlowContextBundle();

    /**
     * Builder interface for creating PersistedConnectorRecord instances.
     */
    interface Builder {
        Builder identifier(String identifier);

        Builder name(String name);

        Builder type(String type);

        Builder bundleCoordinate(BundleCoordinate bundleCoordinate);

        Builder desiredState(ConnectorState desiredState);

        Builder activeConfiguration(List<VersionedConfigurationStep> activeConfiguration);

        Builder workingConfiguration(List<VersionedConfigurationStep> workingConfiguration);

        Builder flowContextBundle(Bundle flowContextBundle);

        PersistedConnectorRecord build();
    }
}
