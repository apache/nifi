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

import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.Map;

import javax.xml.bind.annotation.XmlType;

/**
 * The details for a process group within this NiFi flow.
 */
@XmlType(name = "processGroup")
public class ProcessGroupDTO extends ComponentDTO {

    private String name;
    private String comments;
    private Map<String, String> variables;

    private Integer runningCount;
    private Integer stoppedCount;
    private Integer invalidCount;
    private Integer disabledCount;
    private Integer activeRemotePortCount;
    private Integer inactiveRemotePortCount;

    private Integer inputPortCount;
    private Integer outputPortCount;

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
            value = "The number of input ports in the process group."
    )
    public Integer getInputPortCount() {
        return inputPortCount;
    }

    public void setInputPortCount(Integer inputPortCount) {
        this.inputPortCount = inputPortCount;
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
            value = "The number of output ports in the process group."
    )
    public Integer getOutputPortCount() {
        return outputPortCount;
    }

    public void setOutputPortCount(Integer outputPortCount) {
        this.outputPortCount = outputPortCount;
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
}
