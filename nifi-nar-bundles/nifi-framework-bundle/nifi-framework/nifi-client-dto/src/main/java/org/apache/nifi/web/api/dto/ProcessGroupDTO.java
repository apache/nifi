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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.util.NumberUtil;

import javax.xml.bind.annotation.XmlType;
import java.util.Map;

/**
 * The details for a process group within this NiFi flow.
 */
@XmlType(name = "processGroup")
public class ProcessGroupDTO extends ComponentDTO {

    private String name;
    private String comments;
    private Map<String, String> variables;
    private VersionControlInformationDTO versionControlInformation;
    private String parameterContextId;

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

    private FlowSnippetDTO contents;

    public ProcessGroupDTO() {
        super();
    }

    /**
     * The name of this Process Group.
     *
     * @return The name of this Process Group
     */
    @ApiModelProperty(
            value = "The name of the process group."
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
    @ApiModelProperty(
            value = "The comments for the process group."
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
    @ApiModelProperty(
            value = "The contents of this process group."
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
    @ApiModelProperty(
            value = "The number of input ports in the process group.",
            readOnly = true
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
            readOnly = true
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

    @ApiModelProperty(value = "The variables that are configured for the Process Group. Note that this map contains only "
        + "those variables that are defined on this Process Group and not any variables that are defined in the parent "
        + "Process Group, etc. I.e., this Map will not contain all variables that are accessible by components in this "
        + "Process Group by rather only the variables that are defined for this Process Group itself.", readOnly = true)
    public Map<String, String> getVariables() {
        return variables;
    }

    public void setVariables(final Map<String, String> variables) {
        this.variables = variables;
    }

    @ApiModelProperty("The Version Control information that indicates which Flow Registry, and where in the Flow Registry, "
        + "this Process Group is tracking to; or null if this Process Group is not under version control")
    public VersionControlInformationDTO getVersionControlInformation() {
        return versionControlInformation;
    }

    public void setVersionControlInformation(final VersionControlInformationDTO versionControlInformation) {
        this.versionControlInformation = versionControlInformation;
    }

    @ApiModelProperty("The ID of the Parameter Context that is assigned to this Process Group.")
    public String getParameterContextId() {
        return parameterContextId;
    }

    public void setParameterContextId(final String parameterContextId) {
        this.parameterContextId = parameterContextId;
    }
}
