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

package org.apache.nifi.minifi.commons.status.instance;

import org.apache.nifi.minifi.commons.status.common.BulletinStatus;

import java.util.List;

public class InstanceStatus implements java.io.Serializable {

    private InstanceHealth instanceHealth;
    private List<BulletinStatus> bulletinList;
    private InstanceStats instanceStats;

    public InstanceStatus() {
    }

    public InstanceHealth getInstanceHealth() {
        return instanceHealth;
    }

    public void setInstanceHealth(InstanceHealth instanceHealth) {
        this.instanceHealth = instanceHealth;
    }

    public List<BulletinStatus> getBulletinList() {
        return bulletinList;
    }

    public void setBulletinList(List<BulletinStatus> bulletinList) {
        this.bulletinList = bulletinList;
    }

    public InstanceStats getInstanceStats() {
        return instanceStats;
    }

    public void setInstanceStats(InstanceStats instanceStats) {
        this.instanceStats = instanceStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InstanceStatus that = (InstanceStatus) o;

        if (getInstanceHealth() != null ? !getInstanceHealth().equals(that.getInstanceHealth()) : that.getInstanceHealth() != null) return false;
        if (getBulletinList() != null ? !getBulletinList().equals(that.getBulletinList()) : that.getBulletinList() != null) return false;
        return getInstanceStats() != null ? getInstanceStats().equals(that.getInstanceStats()) : that.getInstanceStats() == null;

    }

    @Override
    public int hashCode() {
        int result = getInstanceHealth() != null ? getInstanceHealth().hashCode() : 0;
        result = 31 * result + (getBulletinList() != null ? getBulletinList().hashCode() : 0);
        result = 31 * result + (getInstanceStats() != null ? getInstanceStats().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "instanceHealth=" + instanceHealth +
                ", bulletinList=" + bulletinList +
                ", instanceStats=" + instanceStats +
                '}';
    }
}
