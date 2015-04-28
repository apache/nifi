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

    /**
     * The active thread count.
     *
     * @return The active thread count
     */
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return queue for the controller
     */
    public String getQueued() {
        return queued;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    /**
     * @return Used in clustering, will report the number of nodes connected vs the number of nodes in the cluster
     */
    public String getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(String connectedNodes) {
        this.connectedNodes = connectedNodes;
    }

    /**
     * @return System bulletins to be reported to the user
     */
    public List<BulletinDTO> getBulletins() {
        return bulletins;
    }

    public void setBulletins(List<BulletinDTO> bulletins) {
        this.bulletins = bulletins;
    }

    /**
     * @return whether or not there are pending user requests
     */
    public Boolean getHasPendingAccounts() {
        return hasPendingAccounts;
    }

    public void setHasPendingAccounts(Boolean hasPendingAccounts) {
        this.hasPendingAccounts = hasPendingAccounts;
    }

    /**
     * @return number of running components in this controller
     */
    public Integer getRunningCount() {
        return runningCount;
    }

    public void setRunningCount(Integer runningCount) {
        this.runningCount = runningCount;
    }

    /**
     * @return number of stopped components in this controller
     */
    public Integer getStoppedCount() {
        return stoppedCount;
    }

    public void setStoppedCount(Integer stoppedCount) {
        this.stoppedCount = stoppedCount;
    }

    /**
     * @return number of invalid components in this controller
     */
    public Integer getInvalidCount() {
        return invalidCount;
    }

    public void setInvalidCount(Integer invalidCount) {
        this.invalidCount = invalidCount;
    }

    /**
     * @return number of disabled components in this controller
     */
    public Integer getDisabledCount() {
        return disabledCount;
    }

    public void setDisabledCount(Integer disabledCount) {
        this.disabledCount = disabledCount;
    }

    /**
     * @return number of active remote ports in this controller
     */
    public Integer getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public void setActiveRemotePortCount(Integer activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
    }

    /**
     * @return number of inactive remote ports in this controller
     */
    public Integer getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public void setInactiveRemotePortCount(Integer inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
    }

}
