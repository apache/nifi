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

import java.util.List;

/**
 * Describes a Versioned Process Group on the canvas that is eligible to be used as the source for a Connector
 * migration. Returned by {@link ConnectorMigrationManager#listMigrationSources(String)} and surfaced to clients
 * through the {@code GET /connectors/{id}/migration-sources} endpoint.
 */
public class ConnectorMigrationSource {
    private String processGroupId;
    private String processGroupName;
    private String parentProcessGroupId;
    private String registryClientId;
    private String bucketId;
    private String flowId;
    private String flowName;
    private String version;
    private boolean readyForMigration;
    private List<String> ineligibilityReasons;

    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    public String getProcessGroupName() {
        return processGroupName;
    }

    public void setProcessGroupName(final String processGroupName) {
        this.processGroupName = processGroupName;
    }

    public String getParentProcessGroupId() {
        return parentProcessGroupId;
    }

    public void setParentProcessGroupId(final String parentProcessGroupId) {
        this.parentProcessGroupId = parentProcessGroupId;
    }

    public String getRegistryClientId() {
        return registryClientId;
    }

    public void setRegistryClientId(final String registryClientId) {
        this.registryClientId = registryClientId;
    }

    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(final String bucketId) {
        this.bucketId = bucketId;
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(final String flowId) {
        this.flowId = flowId;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(final String flowName) {
        this.flowName = flowName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    /**
     * Indicates whether the source Process Group is currently in a state from which migration can proceed. When
     * {@code false}, {@link #getIneligibilityReasons()} lists every condition that must be remediated before the
     * source can be migrated.
     *
     * @return whether the source is ready for migration
     */
    public boolean isReadyForMigration() {
        return readyForMigration;
    }

    public void setReadyForMigration(final boolean readyForMigration) {
        this.readyForMigration = readyForMigration;
    }

    /**
     * @return user-facing descriptions of every condition that currently prevents the source Process Group from being
     *         migrated. Empty when {@link #isReadyForMigration()} is {@code true}. The list contains all applicable
     *         conditions so the user can address them together.
     */
    public List<String> getIneligibilityReasons() {
        return ineligibilityReasons;
    }

    public void setIneligibilityReasons(final List<String> ineligibilityReasons) {
        this.ineligibilityReasons = ineligibilityReasons;
    }
}
