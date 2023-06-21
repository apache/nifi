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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.util.NumberUtil;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ProcessGroupDTO.
 */
@XmlRootElement(name = "processGroupEntity")
public class ProcessGroupEntity extends ComponentEntity implements Permissible<ProcessGroupDTO> {

    private ProcessGroupDTO component;
    private ProcessGroupStatusDTO status;
    private RegisteredFlowSnapshot versionedFlowSnapshot;

    private Integer runningCount;
    private Integer stoppedCount;
    private Integer invalidCount;
    private Integer disabledCount;
    private Integer activeRemotePortCount;
    private Integer inactiveRemotePortCount;

    private String versionedFlowState;

    private Integer upToDateCount;
    private Integer locallyModifiedCount;
    private Integer staleCount;
    private Integer locallyModifiedAndStaleCount;
    private Integer syncFailureCount;

    private Integer localInputPortCount;
    private Integer localOutputPortCount;
    private Integer publicInputPortCount;
    private Integer publicOutputPortCount;

    private ParameterContextReferenceEntity parameterContext;

    private String processGroupUpdateStrategy;

    /**
     * The ProcessGroupDTO that is being serialized.
     *
     * @return The ProcessGroupDTO object
     */
    @Override
    public ProcessGroupDTO getComponent() {
        return component;
    }

    @Override
    public void setComponent(ProcessGroupDTO component) {
        this.component = component;
    }

    /**
     * @return the process group status
     */
    @ApiModelProperty(
        value = "The status of the process group."
    )
    public ProcessGroupStatusDTO getStatus() {
        return status;
    }

    public void setStatus(ProcessGroupStatusDTO status) {
        this.status = status;
    }

    /**
     * @return number of input ports contained in this process group
     */
    @ApiModelProperty(
        value = "The number of input ports in the process group.",
        accessMode = ApiModelProperty.AccessMode.READ_ONLY
    )
    public Integer getInputPortCount() {
        return NumberUtil.sumNullableIntegers(localInputPortCount, publicInputPortCount);
    }

    public void setInputPortCount(Integer inputPortCount) {
        // See ProcessGroupDTO.setInputPortCount for the reason why this is needed.
    }

    /**
     * @return number of local input ports contained in this process group
     */
    @ApiModelProperty(
        value = "The number of local input ports in the process group."
    )
    public Integer getLocalInputPortCount() {
        return localInputPortCount;
    }

    public void setLocalInputPortCount(Integer localInputPortCount) {
        this.localInputPortCount = localInputPortCount;
    }

    /**
     * @return number of public input ports contained in this process group
     */
    @ApiModelProperty(
        value = "The number of public input ports in the process group."
    )
    public Integer getPublicInputPortCount() {
        return publicInputPortCount;
    }

    public void setPublicInputPortCount(Integer publicInputPortCount) {
        this.publicInputPortCount = publicInputPortCount;
    }

    /**
     * @return number of invalid components in this process group
     */
    @ApiModelProperty(
        value = "The number of invalid components in the process group."
    )
    public Integer getInvalidCount() {
        return invalidCount;
    }

    public void setInvalidCount(Integer invalidCount) {
        this.invalidCount = invalidCount;
    }

    /**
     * @return number of output ports in this process group
     */
    @ApiModelProperty(
        value = "The number of output ports in the process group.",
        accessMode = ApiModelProperty.AccessMode.READ_ONLY
    )
    public Integer getOutputPortCount() {
        return NumberUtil.sumNullableIntegers(localOutputPortCount, publicOutputPortCount);
    }

    public void setOutputPortCount(Integer outputPortCount) {
        // See ProcessGroupDTO.setInputPortCount for the reason why this is needed.
    }

    /**
     * @return number of local output ports in this process group
     */
    @ApiModelProperty(
        value = "The number of local output ports in the process group."
    )
    public Integer getLocalOutputPortCount() {
        return localOutputPortCount;
    }

    public void setLocalOutputPortCount(Integer localOutputPortCount) {
        this.localOutputPortCount = localOutputPortCount;
    }

    /**
     * @return number of public output ports in this process group
     */
    @ApiModelProperty(
        value = "The number of public output ports in the process group."
    )
    public Integer getPublicOutputPortCount() {
        return publicOutputPortCount;
    }

    public void setPublicOutputPortCount(Integer publicOutputPortCount) {
        this.publicOutputPortCount = publicOutputPortCount;
    }

    /**
     * @return number of running component in this process group
     */
    @ApiModelProperty(
        value = "The number of running components in this process group."
    )
    public Integer getRunningCount() {
        return runningCount;
    }

    public void setRunningCount(Integer runningCount) {
        this.runningCount = runningCount;
    }

    /**
     * @return number of stopped components in this process group
     */
    @ApiModelProperty(
        value = "The number of stopped components in the process group."
    )
    public Integer getStoppedCount() {
        return stoppedCount;
    }

    public void setStoppedCount(Integer stoppedCount) {
        this.stoppedCount = stoppedCount;
    }

    /**
     * @return number of disabled components in this process group
     */
    @ApiModelProperty(
        value = "The number of disabled components in the process group."
    )
    public Integer getDisabledCount() {
        return disabledCount;
    }

    public void setDisabledCount(Integer disabledCount) {
        this.disabledCount = disabledCount;
    }

    /**
     * @return number of active remote ports in this process group
     */
    @ApiModelProperty(
        value = "The number of active remote ports in the process group."
    )
    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    /**
     * @return number of inactive remote ports in this process group
     */
    @ApiModelProperty(
        value = "The number of inactive remote ports in the process group."
    )
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    @ApiModelProperty(value = "Returns the Versioned Flow that describes the contents of the Versioned Flow to be imported", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public RegisteredFlowSnapshot getVersionedFlowSnapshot() {
        return versionedFlowSnapshot;
    }

    public void setVersionedFlowSnapshot(RegisteredFlowSnapshot versionedFlowSnapshot) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
    }

    @ApiModelProperty(accessMode = ApiModelProperty.AccessMode.READ_ONLY,
            value = "The current state of the Process Group, as it relates to the Versioned Flow",
            allowableValues = "LOCALLY_MODIFIED, STALE, LOCALLY_MODIFIED_AND_STALE, UP_TO_DATE, SYNC_FAILURE")
    public String getVersionedFlowState() {
        return versionedFlowState;
    }

    public void setVersionedFlowState(String versionedFlowState) {
        this.versionedFlowState = versionedFlowState;
    }

    @ApiModelProperty("The number of up to date versioned process groups in the process group.")
    public Integer getUpToDateCount() {
        return upToDateCount;
    }

    public void setUpToDateCount(Integer upToDateCount) {
        this.upToDateCount = upToDateCount;
    }

    @ApiModelProperty("The number of locally modified versioned process groups in the process group.")
    public Integer getLocallyModifiedCount() {
        return locallyModifiedCount;
    }

    public void setLocallyModifiedCount(Integer locallyModifiedCount) {
        this.locallyModifiedCount = locallyModifiedCount;
    }

    @ApiModelProperty("The number of stale versioned process groups in the process group.")
    public Integer getStaleCount() {
        return staleCount;
    }

    public void setStaleCount(Integer staleCount) {
        this.staleCount = staleCount;
    }

    @ApiModelProperty("The number of locally modified and stale versioned process groups in the process group.")
    public Integer getLocallyModifiedAndStaleCount() {
        return locallyModifiedAndStaleCount;
    }

    public void setLocallyModifiedAndStaleCount(Integer locallyModifiedAndStaleCount) {
        this.locallyModifiedAndStaleCount = locallyModifiedAndStaleCount;
    }

    @ApiModelProperty("The number of versioned process groups in the process group that are unable to sync to a registry.")
    public Integer getSyncFailureCount() {
        return syncFailureCount;
    }

    public void setSyncFailureCount(Integer syncFailureCount) {
        this.syncFailureCount = syncFailureCount;
    }

    @ApiModelProperty("The Parameter Context, or null if no Parameter Context has been bound to the Process Group")
    public ParameterContextReferenceEntity getParameterContext() {
        return parameterContext;
    }

    public void setParameterContext(ParameterContextReferenceEntity parameterContext) {
        this.parameterContext = parameterContext;
    }

    @ApiModelProperty(
            value = "Determines the process group update strategy",
            allowableValues = "UPDATE_PROCESS_GROUP_ONLY, UPDATE_PROCESS_GROUP_WITH_DESCENDANTS"
    )
    public String getProcessGroupUpdateStrategy() {
        return processGroupUpdateStrategy;
    }

    public void setProcessGroupUpdateStrategy(String processGroupUpdateStrategy) {
        this.processGroupUpdateStrategy = processGroupUpdateStrategy;
    }
}
