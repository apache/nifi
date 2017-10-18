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

package org.apache.nifi.registry.variable;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VariableRegistryUpdateRequest {
    private final String requestId;
    private final String processGroupId;
    private final NiFiUser user;
    private volatile Date submissionTime = new Date();
    private volatile Date lastUpdated = new Date();
    private volatile boolean complete = false;

    private final AtomicReference<String> failureReason = new AtomicReference<>();
    private RevisionDTO processGroupRevision;
    private Map<String, AffectedComponentEntity> affectedComponents;

    private final VariableRegistryUpdateStep identifyComponentsStep = new VariableRegistryUpdateStep("Identifying components affected");
    private final VariableRegistryUpdateStep stopProcessors = new VariableRegistryUpdateStep("Stopping affected Processors");
    private final VariableRegistryUpdateStep disableServices = new VariableRegistryUpdateStep("Disabling affected Controller Services");
    private final VariableRegistryUpdateStep applyUpdates = new VariableRegistryUpdateStep("Applying Updates");
    private final VariableRegistryUpdateStep enableServices = new VariableRegistryUpdateStep("Re-Enabling affected Controller Services");
    private final VariableRegistryUpdateStep startProcessors = new VariableRegistryUpdateStep("Restarting affected Processors");

    public VariableRegistryUpdateRequest(final String requestId, final String processGroupId, final Set<AffectedComponentEntity> affectedComponents, final NiFiUser user) {
        this.requestId = requestId;
        this.processGroupId = processGroupId;
        this.affectedComponents = affectedComponents.stream().collect(Collectors.toMap(AffectedComponentEntity::getId, Function.identity()));
        this.user = user;
    }

    public String getProcessGroupId() {
        return processGroupId;
    }

    public String getRequestId() {
        return requestId;
    }

    public Date getSubmissionTime() {
        return submissionTime;
    }

    public NiFiUser getUser() {
        return user;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public VariableRegistryUpdateStep getIdentifyRelevantComponentsStep() {
        return identifyComponentsStep;
    }

    public VariableRegistryUpdateStep getStopProcessorsStep() {
        return stopProcessors;
    }

    public VariableRegistryUpdateStep getDisableServicesStep() {
        return disableServices;
    }

    public VariableRegistryUpdateStep getApplyUpdatesStep() {
        return applyUpdates;
    }

    public VariableRegistryUpdateStep getEnableServicesStep() {
        return enableServices;
    }

    public VariableRegistryUpdateStep getStartProcessorsStep() {
        return startProcessors;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public String getFailureReason() {
        return failureReason.get();
    }

    public void setFailureReason(String reason) {
        this.failureReason.set(reason);
    }

    public RevisionDTO getProcessGroupRevision() {
        return processGroupRevision;
    }

    public void setProcessGroupRevision(RevisionDTO processGroupRevision) {
        this.processGroupRevision = processGroupRevision;
    }

    public Map<String, AffectedComponentEntity> getAffectedComponents() {
        return affectedComponents;
    }

    public void cancel() {
        this.failureReason.compareAndSet(null, "Update was cancelled");
        this.complete = true;
    }
}
