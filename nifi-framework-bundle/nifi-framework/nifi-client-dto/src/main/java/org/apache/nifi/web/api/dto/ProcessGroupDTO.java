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
import org.apache.nifi.web.api.dto.util.NumberUtil;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;

import jakarta.xml.bind.annotation.XmlType;

/**
 * The details for a process group within this NiFi flow.
 */
@XmlType(name = "processGroup")
public class ProcessGroupDTO extends ComponentDTO {

    private String name;
    private String comments;
    private VersionControlInformationDTO versionControlInformation;
    private ParameterContextReferenceEntity parameterContext;
    private String flowfileConcurrency;
    private String flowfileOutboundPolicy;
    private String defaultFlowFileExpiration;
    private Long defaultBackPressureObjectThreshold;
    private String defaultBackPressureDataSizeThreshold;
    private String logFileSuffix;
    private String executionEngine;
    private Integer maxConcurrentTasks;
    private String statelessFlowTimeout;

    private Integer runningCount;
    private Integer stoppedCount;
    private Integer invalidCount;
    private Integer disabledCount;
    private Integer activeRemotePortCount;
    private Integer inactiveRemotePortCount;

    private Integer upToDateCount;
    private Integer locallyModifiedCount;
    private Integer staleCount;
    private Integer locallyModifiedAndStaleCount;
    private Integer syncFailureCount;

    private Integer localInputPortCount;
    private Integer localOutputPortCount;

    private Integer publicInputPortCount;
    private Integer publicOutputPortCount;

    private String statelessGroupScheduledState;

    private FlowSnippetDTO contents;

    public ProcessGroupDTO() {
        super();
    }

    /**
     * The name of this Process Group.
     *
     * @return The name of this Process Group
     */
    @Schema(description = "The name of the process group."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return comments for this process group
     */
    @Schema(description = "The comments for the process group."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return contents of this process group.
     */
    @Schema(description = "The contents of this process group."
    )
    public FlowSnippetDTO getContents() {
        return contents;
    }

    public void setContents(FlowSnippetDTO contents) {
        this.contents = contents;
    }

    /**
     * @return number of input ports contained in this process group
     */
    @Schema(description = "The number of input ports in the process group.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Integer getInputPortCount() {
        return NumberUtil.sumNullableIntegers(localInputPortCount, publicInputPortCount);
    }

    public void setInputPortCount(Integer inputPortCount) {
        // Without having setter for 'inputPortCount', deserialization fails.
        // If we use Jackson annotation @JsonIgnoreProperties, this empty setter is not needed.
        // Ex. @JsonIgnoreProperties(value={"inputPortCount", "outputPortCount"}, allowGetters=true)
        // But in order to minimize dependencies, we don't use Jackson annotations in this module.
    }

    /**
     * @return number of local input ports contained in this process group
     */
    @Schema(description = "The number of local input ports in the process group."
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
    @Schema(description = "The number of public input ports in the process group."
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
    @Schema(description = "The number of invalid components in the process group."
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
    @Schema(description = "The number of output ports in the process group.",
            accessMode = Schema.AccessMode.READ_ONLY
    )
    public Integer getOutputPortCount() {
        return NumberUtil.sumNullableIntegers(localOutputPortCount, publicOutputPortCount);
    }

    public void setOutputPortCount(Integer outputPortCount) {
        // See setInputPortCount for the reason why this is needed.
    }

    /**
     * @return number of local output ports in this process group
     */
    @Schema(description = "The number of local output ports in the process group."
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
    @Schema(description = "The number of public output ports in the process group."
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
    @Schema(description = "The number of running components in this process group."
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
    @Schema(description = "The number of stopped components in the process group."
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
    @Schema(description = "The number of disabled components in the process group."
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
    @Schema(description = "The number of active remote ports in the process group."
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
    @Schema(description = "The number of inactive remote ports in the process group."
    )
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    @Schema(description = "The number of up to date versioned process groups in the process group.")
    public Integer getUpToDateCount() {
        return upToDateCount;
    }

    public void setUpToDateCount(Integer upToDateCount) {
        this.upToDateCount = upToDateCount;
    }

    @Schema(description = "The number of locally modified versioned process groups in the process group.")
    public Integer getLocallyModifiedCount() {
        return locallyModifiedCount;
    }

    public void setLocallyModifiedCount(Integer locallyModifiedCount) {
        this.locallyModifiedCount = locallyModifiedCount;
    }

    @Schema(description = "The number of stale versioned process groups in the process group.")
    public Integer getStaleCount() {
        return staleCount;
    }

    public void setStaleCount(Integer staleCount) {
        this.staleCount = staleCount;
    }

    @Schema(description = "The number of locally modified and stale versioned process groups in the process group.")
    public Integer getLocallyModifiedAndStaleCount() {
        return locallyModifiedAndStaleCount;
    }

    public void setLocallyModifiedAndStaleCount(Integer locallyModifiedAndStaleCount) {
        this.locallyModifiedAndStaleCount = locallyModifiedAndStaleCount;
    }

    @Schema(description = "The number of versioned process groups in the process group that are unable to sync to a registry.")
    public Integer getSyncFailureCount() {
        return syncFailureCount;
    }

    public void setSyncFailureCount(Integer syncFailureCount) {
        this.syncFailureCount = syncFailureCount;
    }

    @Schema(description = "The Version Control information that indicates which Flow Registry, and where in the Flow Registry, "
        + "this Process Group is tracking to; or null if this Process Group is not under version control")
    public VersionControlInformationDTO getVersionControlInformation() {
        return versionControlInformation;
    }

    public void setVersionControlInformation(final VersionControlInformationDTO versionControlInformation) {
        this.versionControlInformation = versionControlInformation;
    }

    @Schema(description = "The Parameter Context that this Process Group is bound to.")
    public ParameterContextReferenceEntity getParameterContext() {
        return parameterContext;
    }

    public void setParameterContext(final ParameterContextReferenceEntity parameterContext) {
        this.parameterContext = parameterContext;
    }

    @Schema(description = "The FlowFile Concurrency for this Process Group.", allowableValues = {"UNBOUNDED", "SINGLE_FLOWFILE_PER_NODE", "SINGLE_BATCH_PER_NODE"})
    public String getFlowfileConcurrency() {
        return flowfileConcurrency;
    }

    public void setFlowfileConcurrency(final String flowfileConcurrency) {
        this.flowfileConcurrency = flowfileConcurrency;
    }

    @Schema(description = "The Outbound Policy that is used for determining how FlowFiles should be transferred out of the Process Group.",
        allowableValues = {"STREAM_WHEN_AVAILABLE", "BATCH_OUTPUT"})
    public String getFlowfileOutboundPolicy() {
        return flowfileOutboundPolicy;
    }

    public void setFlowfileOutboundPolicy(final String flowfileOutboundPolicy) {
        this.flowfileOutboundPolicy = flowfileOutboundPolicy;
    }

    @Schema(description = "The default FlowFile Expiration for this Process Group.")
    public String getDefaultFlowFileExpiration() {
        return defaultFlowFileExpiration;
    }

    public void setDefaultFlowFileExpiration(String defaultFlowFileExpiration) {
        this.defaultFlowFileExpiration = defaultFlowFileExpiration;
    }

    @Schema(description = "Default value used in this Process Group for the maximum number of objects that can be queued before back pressure is applied.")
    public Long getDefaultBackPressureObjectThreshold() {
        return defaultBackPressureObjectThreshold;
    }

    public void setDefaultBackPressureObjectThreshold(final Long defaultBackPressureObjectThreshold) {
        this.defaultBackPressureObjectThreshold = defaultBackPressureObjectThreshold;
    }

    @Schema(description = "Default value used in this Process Group for the maximum data size of objects that can be queued before back pressure is applied.")
    public String getDefaultBackPressureDataSizeThreshold() {
        return defaultBackPressureDataSizeThreshold;
    }

    public void setDefaultBackPressureDataSizeThreshold(final String defaultBackPressureDataSizeThreshold) {
        this.defaultBackPressureDataSizeThreshold = defaultBackPressureDataSizeThreshold;
    }

    @Schema(description = "The log file suffix for this Process Group for dedicated logging.")
    public String getLogFileSuffix() {
        return logFileSuffix;
    }

    public void setLogFileSuffix(final String logFileSuffix) {
        this.logFileSuffix = logFileSuffix;
    }

    @Schema(description = "The Execution Engine that should be used to run the flow represented by this Process Group.",
        allowableValues = {"STATELESS", "STANDARD", "INHERITED"})
    public String getExecutionEngine() {
        return executionEngine;
    }

    public void setExecutionEngine(final String executionEngine) {
        this.executionEngine = executionEngine;
    }

    @Schema(description = "If the Process Group is configured to run in using the Stateless Engine, represents the current state. Otherwise, will be STOPPED.",
            allowableValues = {"STOPPED", "RUNNING"})
    public String getStatelessGroupScheduledState() {
        return statelessGroupScheduledState;
    }

    public void setStatelessGroupScheduledState(final String state) {
        this.statelessGroupScheduledState = state;
    }

    @Schema(description = "The maximum number of concurrent tasks to use when running the flow using the Stateless Engine")
    public Integer getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public void setMaxConcurrentTasks(final Integer maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    @Schema(description = "The maximum amount of time that the flow can be run using the Stateless Engine before the flow times out")
    public String getStatelessFlowTimeout() {
        return statelessFlowTimeout;
    }

    public void setStatelessFlowTimeout(final String timeout) {
        this.statelessFlowTimeout = timeout;
    }
}
