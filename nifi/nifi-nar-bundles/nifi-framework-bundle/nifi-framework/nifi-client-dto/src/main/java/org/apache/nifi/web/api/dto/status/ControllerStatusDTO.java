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
package org.apache.nifi.web.api.dto.status;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.BulletinDTO;

/**
 * The status of this NiFi controller.
 */
@XmlType(name = "controllerStatus")
public class ControllerStatusDTO {

    private Integer activeThreadCount;
    private String queued;
    private String connectedNodes;
    private Boolean hasPendingAccounts;

    private Integer runningCount;
    private Integer stoppedCount;
    private Integer invalidCount;
    private Integer disabledCount;
    private Integer activeRemotePortCount;
    private Integer inactiveRemotePortCount;

    private List<BulletinDTO> bulletins;
    private List<BulletinDTO> controllerServiceBulletins;
    private List<BulletinDTO> reportingTaskBulletins;

    /**
     * The active thread count.
     *
     * @return The active thread count
     */
    @ApiModelProperty(
            value = "The number of active threads in the NiFi."
    )
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return queue for the controller
     */
    @ApiModelProperty(
            value = "The number of flowfilew queued in the NiFi."
    )
    public String getQueued() {
        return queued;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    /**
     * @return Used in clustering, will report the number of nodes connected vs
     * the number of nodes in the cluster
     */
    @ApiModelProperty(
            value = "When clustered, reports the number of nodes connected vs the number of nodes in the cluster."
    )
    public String getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(String connectedNodes) {
        this.connectedNodes = connectedNodes;
    }

    /**
     * @return System bulletins to be reported to the user
     */
    @ApiModelProperty(
            value = "System level bulletins to be reported to the user."
    )
    public List<BulletinDTO> getBulletins() {
        return bulletins;
    }

    public void setBulletins(List<BulletinDTO> bulletins) {
        this.bulletins = bulletins;
    }

    /**
     * @return Controller service bulletins to be reported to the user
     */
    @ApiModelProperty(
            value = "Controller service bulletins to be reported to the user."
    )
    public List<BulletinDTO> getControllerServiceBulletins() {
        return controllerServiceBulletins;
    }

    public void setControllerServiceBulletins(List<BulletinDTO> controllerServiceBulletins) {
        this.controllerServiceBulletins = controllerServiceBulletins;
    }

    /**
     * @return Reporting task bulletins to be reported to the user
     */
    @ApiModelProperty(
            value = "Reporting task bulletins to be reported to the user."
    )
    public List<BulletinDTO> getReportingTaskBulletins() {
        return reportingTaskBulletins;
    }

    public void setReportingTaskBulletins(List<BulletinDTO> reportingTaskBulletins) {
        this.reportingTaskBulletins = reportingTaskBulletins;
    }

    /**
     * @return whether or not there are pending user requests
     */
    @ApiModelProperty(
            value = "Whether there are any pending user account requests."
    )
    public Boolean getHasPendingAccounts() {
        return hasPendingAccounts;
    }

    public void setHasPendingAccounts(Boolean hasPendingAccounts) {
        this.hasPendingAccounts = hasPendingAccounts;
    }

    /**
     * @return number of running components in this controller
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
     * @return number of stopped components in this controller
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
     * @return number of invalid components in this controller
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
     * @return number of disabled components in this controller
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
     * @return number of active remote ports in this controller
     */
    @ApiModelProperty(
            value = "The number of active remote ports in the NiFi."
    )
    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    /**
     * @return number of inactive remote ports in this controller
     */
    @ApiModelProperty(
            value = "The number of inactive remote ports in the NiFi."
    )
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

}
