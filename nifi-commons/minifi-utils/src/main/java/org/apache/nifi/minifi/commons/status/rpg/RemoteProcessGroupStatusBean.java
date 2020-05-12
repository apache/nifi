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

package org.apache.nifi.minifi.commons.status.rpg;

import org.apache.nifi.minifi.commons.status.common.BulletinStatus;

import java.io.Serializable;
import java.util.List;

public class RemoteProcessGroupStatusBean implements Serializable {
    private String name;
    private RemoteProcessGroupHealth remoteProcessGroupHealth;
    private List<BulletinStatus> bulletinList;
    private List<PortStatus> inputPortStatusList;
    private List<PortStatus> outputPortStatusList;
    private RemoteProcessGroupStats remoteProcessGroupStats;

    public RemoteProcessGroupStatusBean() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RemoteProcessGroupHealth getRemoteProcessGroupHealth() {
        return remoteProcessGroupHealth;
    }

    public void setRemoteProcessGroupHealth(RemoteProcessGroupHealth remoteProcessGroupHealth) {
        this.remoteProcessGroupHealth = remoteProcessGroupHealth;
    }

    public List<BulletinStatus> getBulletinList() {
        return bulletinList;
    }

    public void setBulletinList(List<BulletinStatus> bulletinList) {
        this.bulletinList = bulletinList;
    }

    public List<PortStatus> getInputPortStatusList() {
        return inputPortStatusList;
    }

    public void setInputPortStatusList(List<PortStatus> inputPortStatusList) {
        this.inputPortStatusList = inputPortStatusList;
    }

    public List<PortStatus> getOutputPortStatusList() {
        return outputPortStatusList;
    }

    public void setOutputPortStatusList(List<PortStatus> outputPortStatusList) {
        this.outputPortStatusList = outputPortStatusList;
    }

    public RemoteProcessGroupStats getRemoteProcessGroupStats() {
        return remoteProcessGroupStats;
    }

    public void setRemoteProcessGroupStats(RemoteProcessGroupStats remoteProcessGroupStats) {
        this.remoteProcessGroupStats = remoteProcessGroupStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteProcessGroupStatusBean that = (RemoteProcessGroupStatusBean) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        if (getRemoteProcessGroupHealth() != null ? !getRemoteProcessGroupHealth().equals(that.getRemoteProcessGroupHealth()) : that.getRemoteProcessGroupHealth() != null) return false;
        if (getBulletinList() != null ? !getBulletinList().equals(that.getBulletinList()) : that.getBulletinList() != null) return false;
        if (getInputPortStatusList() != null ? !getInputPortStatusList().equals(that.getInputPortStatusList()) : that.getInputPortStatusList() != null) return false;
        if (getOutputPortStatusList() != null ? !getOutputPortStatusList().equals(that.getOutputPortStatusList()) : that.getOutputPortStatusList() != null) return false;
        return getRemoteProcessGroupStats() != null ? getRemoteProcessGroupStats().equals(that.getRemoteProcessGroupStats()) : that.getRemoteProcessGroupStats() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getRemoteProcessGroupHealth() != null ? getRemoteProcessGroupHealth().hashCode() : 0);
        result = 31 * result + (getBulletinList() != null ? getBulletinList().hashCode() : 0);
        result = 31 * result + (getInputPortStatusList() != null ? getInputPortStatusList().hashCode() : 0);
        result = 31 * result + (getOutputPortStatusList() != null ? getOutputPortStatusList().hashCode() : 0);
        result = 31 * result + (getRemoteProcessGroupStats() != null ? getRemoteProcessGroupStats().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", remoteProcessGroupHealth=" + remoteProcessGroupHealth +
                ", bulletinList=" + bulletinList +
                ", inputPortStatusList=" + inputPortStatusList +
                ", inputPortStatusList=" + outputPortStatusList +
                ", remoteProcessGroupStats=" + remoteProcessGroupStats +
                '}';
    }
}
