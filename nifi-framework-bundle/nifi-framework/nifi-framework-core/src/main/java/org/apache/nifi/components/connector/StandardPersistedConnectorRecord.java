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

import java.util.Collections;
import java.util.List;

/**
 * Standard implementation of {@link PersistedConnectorRecord}.
 */
public class StandardPersistedConnectorRecord implements PersistedConnectorRecord {

    private final String identifier;
    private final String name;
    private final String type;
    private final BundleCoordinate bundleCoordinate;
    private final ConnectorState desiredState;
    private final List<VersionedConfigurationStep> activeConfiguration;
    private final List<VersionedConfigurationStep> workingConfiguration;
    private final Bundle flowContextBundle;

    private StandardPersistedConnectorRecord(final Builder builder) {
        this.identifier = builder.identifier;
        this.name = builder.name;
        this.type = builder.type;
        this.bundleCoordinate = builder.bundleCoordinate;
        this.desiredState = builder.desiredState;
        this.activeConfiguration = builder.activeConfiguration != null ? List.copyOf(builder.activeConfiguration) : Collections.emptyList();
        this.workingConfiguration = builder.workingConfiguration != null ? List.copyOf(builder.workingConfiguration) : Collections.emptyList();
        this.flowContextBundle = builder.flowContextBundle;
    }

    /**
     * Creates a PersistedConnectorRecord from a ConnectorNode.
     *
     * @param connectorNode the connector node
     * @return a PersistedConnectorRecord representing the connector node's state
     */
    public static PersistedConnectorRecord fromConnectorNode(final ConnectorNode connectorNode) {
        return new Builder()
                .identifier(connectorNode.getIdentifier())
                .name(connectorNode.getName())
                .type(connectorNode.getCanonicalClassName())
                .bundleCoordinate(connectorNode.getBundleCoordinate())
                .desiredState(connectorNode.getDesiredState())
                .build();
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }

    @Override
    public ConnectorState getDesiredState() {
        return desiredState;
    }

    @Override
    public List<VersionedConfigurationStep> getActiveConfiguration() {
        return activeConfiguration;
    }

    @Override
    public List<VersionedConfigurationStep> getWorkingConfiguration() {
        return workingConfiguration;
    }

    @Override
    public Bundle getFlowContextBundle() {
        return flowContextBundle;
    }

    public static class Builder implements PersistedConnectorRecord.Builder {
        private String identifier;
        private String name;
        private String type;
        private BundleCoordinate bundleCoordinate;
        private ConnectorState desiredState;
        private List<VersionedConfigurationStep> activeConfiguration;
        private List<VersionedConfigurationStep> workingConfiguration;
        private Bundle flowContextBundle;

        @Override
        public Builder identifier(final String identifier) {
            this.identifier = identifier;
            return this;
        }

        @Override
        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        @Override
        public Builder type(final String type) {
            this.type = type;
            return this;
        }

        @Override
        public Builder bundleCoordinate(final BundleCoordinate bundleCoordinate) {
            this.bundleCoordinate = bundleCoordinate;
            return this;
        }

        @Override
        public Builder desiredState(final ConnectorState desiredState) {
            this.desiredState = desiredState;
            return this;
        }

        @Override
        public Builder activeConfiguration(final List<VersionedConfigurationStep> activeConfiguration) {
            this.activeConfiguration = activeConfiguration;
            return this;
        }

        @Override
        public Builder workingConfiguration(final List<VersionedConfigurationStep> workingConfiguration) {
            this.workingConfiguration = workingConfiguration;
            return this;
        }

        @Override
        public Builder flowContextBundle(final Bundle flowContextBundle) {
            this.flowContextBundle = flowContextBundle;
            return this;
        }

        @Override
        public PersistedConnectorRecord build() {
            return new StandardPersistedConnectorRecord(this);
        }
    }
}
