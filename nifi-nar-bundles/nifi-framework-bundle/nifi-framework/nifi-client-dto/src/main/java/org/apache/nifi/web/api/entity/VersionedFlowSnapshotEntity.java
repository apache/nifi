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

package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.api.dto.RevisionDTO;

import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "versionedFlowSnapshotEntity")
public class VersionedFlowSnapshotEntity extends Entity {
    private RegisteredFlowSnapshot versionedFlowSnapshot;
    private RevisionDTO processGroupRevision;
    private String registryId;
    private Boolean updateDescendantVersionedFlows;
    private Boolean disconnectedNodeAcknowledged;

    @Schema(description = "The versioned flow snapshot")
    public RegisteredFlowSnapshot getVersionedFlowSnapshot() {
        return versionedFlowSnapshot;
    }

    public void setVersionedFlow(RegisteredFlowSnapshot versionedFlowSnapshot) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
    }

    @Schema(description = "The Revision of the Process Group under Version Control")
    public RevisionDTO getProcessGroupRevision() {
        return processGroupRevision;
    }

    public void setProcessGroupRevision(final RevisionDTO revision) {
        this.processGroupRevision = revision;
    }

    @Schema(description = "The ID of the Registry that this flow belongs to")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(String registryId) {
        this.registryId = registryId;
    }

    @Schema(description = "If the Process Group to be updated has a child or descendant Process Group that is also under "
        + "Version Control, this specifies whether or not the contents of that child/descendant Process Group should be updated.")
    public Boolean getUpdateDescendantVersionedFlows() {
        return updateDescendantVersionedFlows;
    }

    public void setUpdateDescendantVersionedFlows(Boolean update) {
        this.updateDescendantVersionedFlows = update;
    }

    @Schema(description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
    )
    public Boolean isDisconnectedNodeAcknowledged() {
        return disconnectedNodeAcknowledged;
    }

    public void setDisconnectedNodeAcknowledged(Boolean disconnectedNodeAcknowledged) {
        this.disconnectedNodeAcknowledged = disconnectedNodeAcknowledged;
    }
}
