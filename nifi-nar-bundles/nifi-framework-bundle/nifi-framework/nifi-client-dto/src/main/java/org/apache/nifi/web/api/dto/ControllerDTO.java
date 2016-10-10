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
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * Configuration details for a NiFi controller. Primary use of this DTO is for consumption by a remote NiFi instance to initiate site to site communications.
 */
@XmlType(name = "controller")
public class ControllerDTO {

    private String id;
    private String name;
    private String comments;

    private Integer runningCount;
    private Integer stoppedCount;
    private Integer invalidCount;
    private Integer disabledCount;
    private Integer activeRemotePortCount;
    private Integer inactiveRemotePortCount;

    private Integer inputPortCount;
    private Integer outputPortCount;

    private Integer remoteSiteListeningPort;
    private Integer remoteSiteHttpListeningPort;
    private Boolean siteToSiteSecure;
    private String instanceId;
    private Set<PortDTO> inputPorts;
    private Set<PortDTO> outputPorts;

    /**
     * @return id of this NiFi controller
     */
    @ApiModelProperty(
            value = "The id of the NiFi."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The name of this NiFi controller.
     *
     * @return The name of this controller
     */
    @ApiModelProperty(
            value = "The name of the NiFi."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return comments of this NiFi controller
     */
    @ApiModelProperty(
            value = "The comments for the NiFi."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return input ports available to send data to this NiFi controller
     */
    @ApiModelProperty(
            value = "The input ports available to send data to for the NiFi."
    )
    public Set<PortDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<PortDTO> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return output ports available to received data from this NiFi controller
     */
    @ApiModelProperty(
            value = "The output ports available to received data from the NiFi."
    )
    public Set<PortDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<PortDTO> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * @return Instance ID of the cluster, if this node is connected to a Cluster Manager, or of this individual instance of in standalone mode
     */
    @ApiModelProperty(
            value = "If clustered, the id of the Cluster Manager, otherwise the id of the NiFi."
    )
    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /**
     * The Socket Port on which this instance is listening for Remote Transfers of Flow Files. If this instance is not configured to receive Flow Files from remote instances, this will be null.
     *
     * @return a integer between 1 and 65535, or null, if not configured for remote transfer
     */
    @ApiModelProperty(
            value = "The Socket Port on which this instance is listening for Remote Transfers of Flow Files. If this instance is not configured to receive Flow Files from remote "
                    + "instances, this will be null."
    )
    public Integer getRemoteSiteListeningPort() {
        return remoteSiteListeningPort;
    }

    public void setRemoteSiteListeningPort(final Integer port) {
        this.remoteSiteListeningPort = port;
    }

    /**
     * The HTTP(S) Port on which this instance is listening for Remote Transfers of Flow Files. If this instance is not configured to receive Flow Files from remote instances, this will be null.
     *
     * @return a integer between 1 and 65535, or null, if not configured for remote transfer
     */
    @ApiModelProperty(
            value = "The HTTP(S) Port on which this instance is listening for Remote Transfers of Flow Files. If this instance is not configured to receive Flow Files from remote "
                    + "instances, this will be null."
    )
    public Integer getRemoteSiteHttpListeningPort() {
        return remoteSiteHttpListeningPort;
    }

    public void setRemoteSiteHttpListeningPort(Integer remoteSiteHttpListeningPort) {
        this.remoteSiteHttpListeningPort = remoteSiteHttpListeningPort;
    }

    /**
     * @return Indicates whether or not Site-to-Site communications with this instance is secure (2-way authentication)
     */
    @ApiModelProperty(
            value = "Indicates whether or not Site-to-Site communications with this instance is secure (2-way authentication)."
    )
    public Boolean isSiteToSiteSecure() {
        return siteToSiteSecure;
    }

    public void setSiteToSiteSecure(Boolean siteToSiteSecure) {
        this.siteToSiteSecure = siteToSiteSecure;
    }

    /**
     * @return number of running components in this process group
     */
    @ApiModelProperty(
            value = "The number of running components in the NiFi."
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
            value = "The number of stopped components in the NiFi."
    )
    public Integer getStoppedCount() {
        return stoppedCount;
    }

    public void setStoppedCount(Integer stoppedCount) {
        this.stoppedCount = stoppedCount;
    }

    /**
     * @return number of active remote ports contained in this process group
     */
    @ApiModelProperty(
            value = "The number of active remote ports contained in the NiFi."
    )
    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    /**
     * @return number of inactive remote ports contained in this process group
     */
    @ApiModelProperty(
            value = "The number of inactive remote ports contained in the NiFi."
    )
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    /**
     * @return number of input ports contained in this process group
     */
    @ApiModelProperty(
            value = "The number of input ports contained in the NiFi."
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
            value = "The number of invalid components in the NiFi."
    )
    public Integer getInvalidCount() {
        return invalidCount;
    }

    public void setInvalidCount(Integer invalidCount) {
        this.invalidCount = invalidCount;
    }

    /**
     * @return number of disabled components in this process group
     */
    @ApiModelProperty(
            value = "The number of disabled components in the NiFi."
    )
    public Integer getDisabledCount() {
        return disabledCount;
    }

    public void setDisabledCount(Integer disabledCount) {
        this.disabledCount = disabledCount;
    }

    /**
     * @return number of output ports in this process group
     */
    @ApiModelProperty(
            value = "The number of output ports in the NiFi."
    )
    public Integer getOutputPortCount() {
        return outputPortCount;
    }

    public void setOutputPortCount(Integer outputPortCount) {
        this.outputPortCount = outputPortCount;
    }

}
