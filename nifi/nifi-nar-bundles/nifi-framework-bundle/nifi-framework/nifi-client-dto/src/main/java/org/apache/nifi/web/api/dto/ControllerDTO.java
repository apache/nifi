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

import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * Configuration details for a NiFi controller. Primary use of this DTO is for
 * consumption by a remote NiFi instance to initiate site to site
 * communications.
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
    private Boolean siteToSiteSecure;
    private String instanceId;
    private Set<PortDTO> inputPorts;
    private Set<PortDTO> outputPorts;

    /**
     * The id of this NiFi controller.
     *
     * @return
     */
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
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The comments of this NiFi controller.
     *
     * @return
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * The input ports available to send data to this NiFi controller.
     *
     * @return
     */
    public Set<PortDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<PortDTO> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * The output ports available to received data from this NiFi controller.
     *
     * @return
     */
    public Set<PortDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<PortDTO> outputPorts) {
        this.outputPorts = outputPorts;
    }

    /**
     * The Instance ID of the cluster, if this node is connected to a Cluster
     * Manager, or of this individual instance of in standalone mode
     *
     * @return
     */
    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /**
     * The Socket Port on which this instance is listening for Remote Transfers
     * of Flow Files. If this instance is not configured to receive Flow Files
     * from remote instances, this will be null.
     *
     * @return a integer between 1 and 65535, or null, if not configured for
     * remote transfer
     */
    public Integer getRemoteSiteListeningPort() {
        return remoteSiteListeningPort;
    }

    public void setRemoteSiteListeningPort(final Integer port) {
        this.remoteSiteListeningPort = port;
    }

    /**
     * Indicates whether or not Site-to-Site communications with this instance
     * is secure (2-way authentication)
     *
     * @return
     */
    public Boolean isSiteToSiteSecure() {
        return siteToSiteSecure;
    }

    public void setSiteToSiteSecure(Boolean siteToSiteSecure) {
        this.siteToSiteSecure = siteToSiteSecure;
    }

    /**
     * The number of running components in this process group.
     *
     * @return
     */
    public Integer getRunningCount() {
        return runningCount;
    }

    public void setRunningCount(Integer runningCount) {
        this.runningCount = runningCount;
    }

    /**
     * The number of stopped components in this process group.
     *
     * @return
     */
    public Integer getStoppedCount() {
        return stoppedCount;
    }

    public void setStoppedCount(Integer stoppedCount) {
        this.stoppedCount = stoppedCount;
    }

    /**
     * The number of active remote ports contained in this process group.
     *
     * @return
     */
    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    /**
     * The number of inactive remote ports contained in this process group.
     *
     * @return
     */
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

    /**
     * The number of input ports contained in this process group.
     *
     * @return
     */
    public Integer getInputPortCount() {
        return inputPortCount;
    }

    public void setInputPortCount(Integer inputPortCount) {
        this.inputPortCount = inputPortCount;
    }

    /**
     * The number of invalid components in this process group.
     *
     * @return
     */
    public Integer getInvalidCount() {
        return invalidCount;
    }

    public void setInvalidCount(Integer invalidCount) {
        this.invalidCount = invalidCount;
    }

    /**
     * The number of disabled components in this process group.
     *
     * @return
     */
    public Integer getDisabledCount() {
        return disabledCount;
    }

    public void setDisabledCount(Integer disabledCount) {
        this.disabledCount = disabledCount;
    }

    /**
     * The number of output ports in this process group.
     *
     * @return
     */
    public Integer getOutputPortCount() {
        return outputPortCount;
    }

    public void setOutputPortCount(Integer outputPortCount) {
        this.outputPortCount = outputPortCount;
    }
}
