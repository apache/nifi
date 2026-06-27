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
import org.apache.nifi.web.api.dto.ComponentDifferenceDTO;
import org.apache.nifi.web.api.dto.RebaseChangeDTO;

import java.util.List;
import java.util.Set;

@XmlRootElement(name = "rebaseAnalysisEntity")
public class RebaseAnalysisEntity extends Entity {
    private String processGroupId;
    private String currentVersion;
    private String targetVersion;
    private String analysisFingerprint;
    private Boolean rebaseAllowed;
    private List<RebaseChangeDTO> localChanges;
    private Set<ComponentDifferenceDTO> upstreamChanges;
    private String failureReason;

    @Schema(description = "The ID of the Process Group being rebased")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @Schema(description = "The current version of the flow in the Process Group")
    public String getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(final String currentVersion) {
        this.currentVersion = currentVersion;
    }

    @Schema(description = "The target version to rebase to")
    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(final String targetVersion) {
        this.targetVersion = targetVersion;
    }

    @Schema(description = "A fingerprint representing the state of this analysis, used to verify the analysis is still valid when executing the rebase")
    public String getAnalysisFingerprint() {
        return analysisFingerprint;
    }

    public void setAnalysisFingerprint(final String analysisFingerprint) {
        this.analysisFingerprint = analysisFingerprint;
    }

    @Schema(description = "Whether the rebase is allowed based on the analysis of local and upstream changes")
    public Boolean getRebaseAllowed() {
        return rebaseAllowed;
    }

    public void setRebaseAllowed(final Boolean rebaseAllowed) {
        this.rebaseAllowed = rebaseAllowed;
    }

    @Schema(description = "The list of local changes that were made to the flow since the last version control operation")
    public List<RebaseChangeDTO> getLocalChanges() {
        return localChanges;
    }

    public void setLocalChanges(final List<RebaseChangeDTO> localChanges) {
        this.localChanges = localChanges;
    }

    @Schema(description = "The set of upstream changes between the current version and the target version in the flow registry")
    public Set<ComponentDifferenceDTO> getUpstreamChanges() {
        return upstreamChanges;
    }

    public void setUpstreamChanges(final Set<ComponentDifferenceDTO> upstreamChanges) {
        this.upstreamChanges = upstreamChanges;
    }

    @Schema(description = "The reason the rebase is not allowed, or null if the rebase is allowed")
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(final String failureReason) {
        this.failureReason = failureReason;
    }
}
