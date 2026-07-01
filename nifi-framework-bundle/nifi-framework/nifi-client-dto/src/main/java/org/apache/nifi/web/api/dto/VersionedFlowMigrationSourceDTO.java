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
package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

import java.util.List;

@XmlType(name = "versionedFlowMigrationSource")
public class VersionedFlowMigrationSourceDTO {
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

    @Schema(description = "The identifier of the source Process Group.")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @Schema(description = "The name of the source Process Group.")
    public String getProcessGroupName() {
        return processGroupName;
    }

    public void setProcessGroupName(final String processGroupName) {
        this.processGroupName = processGroupName;
    }

    @Schema(description = "The identifier of the parent Process Group of the source Process Group.")
    public String getParentProcessGroupId() {
        return parentProcessGroupId;
    }

    public void setParentProcessGroupId(final String parentProcessGroupId) {
        this.parentProcessGroupId = parentProcessGroupId;
    }

    @Schema(description = "The identifier of the Flow Registry client backing the source Process Group.")
    public String getRegistryClientId() {
        return registryClientId;
    }

    public void setRegistryClientId(final String registryClientId) {
        this.registryClientId = registryClientId;
    }

    @Schema(description = "The Flow Registry bucket identifier for the source Process Group.")
    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(final String bucketId) {
        this.bucketId = bucketId;
    }

    @Schema(description = "The Flow Registry flow identifier for the source Process Group.")
    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(final String flowId) {
        this.flowId = flowId;
    }

    @Schema(description = "The name of the versioned flow backing the source Process Group, as recorded in the Flow Registry. "
            + "May be null when the registry has not yet supplied a name (for example, while the source's version control state is SYNC_FAILURE).")
    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(final String flowName) {
        this.flowName = flowName;
    }

    @Schema(description = "The published version of the source Process Group.")
    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    @Schema(description = "Whether the source Process Group is currently in a state that allows it to be migrated into the target Connector. "
            + "When false, ineligibilityReasons describes what must change before the migration can proceed.")
    public boolean isReadyForMigration() {
        return readyForMigration;
    }

    public void setReadyForMigration(final boolean readyForMigration) {
        this.readyForMigration = readyForMigration;
    }

    @Schema(description = "User-facing descriptions of all conditions that currently prevent the source Process Group from being migrated. "
            + "Empty when readyForMigration is true. Each entry describes a single remediable condition (running processors, queued FlowFiles, etc.); "
            + "every applicable condition is included so the user can address them together.")
    public List<String> getIneligibilityReasons() {
        return ineligibilityReasons;
    }

    public void setIneligibilityReasons(final List<String> ineligibilityReasons) {
        this.ineligibilityReasons = ineligibilityReasons;
    }
}
