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
import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.web.api.dto.RevisionDTO;

@XmlRootElement(name = "createFlowBranchRequestEntity")
public class CreateFlowBranchRequestEntity extends Entity {

    private RevisionDTO processGroupRevision;
    private String branch;
    private String sourceBranch;
    private String sourceVersion;
    private Boolean disconnectedNodeAcknowledged;

    @Schema(description = "The Revision of the Process Group under Version Control")
    public RevisionDTO getProcessGroupRevision() {
        return processGroupRevision;
    }

    public void setProcessGroupRevision(final RevisionDTO processGroupRevision) {
        this.processGroupRevision = processGroupRevision;
    }

    @Schema(description = "The name of the new branch to create")
    public String getBranch() {
        return branch;
    }

    public void setBranch(final String branch) {
        this.branch = branch;
    }

    @Schema(description = "The name of the source branch to create the new branch from. Defaults to the branch currently tracking in NiFi.")
    public String getSourceBranch() {
        return sourceBranch;
    }

    public void setSourceBranch(final String sourceBranch) {
        this.sourceBranch = sourceBranch;
    }

    @Schema(description = "The version on the source branch to use when creating the new branch. Defaults to the version currently tracked by NiFi.")
    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(final String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    @Schema(description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.")
    public Boolean isDisconnectedNodeAcknowledged() {
        return disconnectedNodeAcknowledged;
    }

    public void setDisconnectedNodeAcknowledged(final Boolean disconnectedNodeAcknowledged) {
        this.disconnectedNodeAcknowledged = disconnectedNodeAcknowledged;
    }
}
