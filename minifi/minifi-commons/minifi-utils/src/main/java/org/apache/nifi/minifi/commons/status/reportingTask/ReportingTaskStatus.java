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

package org.apache.nifi.minifi.commons.status.reportingTask;

import org.apache.nifi.minifi.commons.status.common.BulletinStatus;

import java.util.List;

public class ReportingTaskStatus implements java.io.Serializable {
    private String name;
    private ReportingTaskHealth reportingTaskHealth;
    private List<BulletinStatus> bulletinList;

    public ReportingTaskStatus() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ReportingTaskHealth getReportingTaskHealth() {
        return reportingTaskHealth;
    }

    public void setReportingTaskHealth(ReportingTaskHealth reportingTaskHealth) {
        this.reportingTaskHealth = reportingTaskHealth;
    }

    public List<BulletinStatus> getBulletinList() {
        return bulletinList;
    }

    public void setBulletinList(List<BulletinStatus> bulletinList) {
        this.bulletinList = bulletinList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReportingTaskStatus that = (ReportingTaskStatus) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        if (getReportingTaskHealth() != null ? !getReportingTaskHealth().equals(that.getReportingTaskHealth()) : that.getReportingTaskHealth() != null) return false;
        return getBulletinList() != null ? getBulletinList().equals(that.getBulletinList()) : that.getBulletinList() == null;

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getReportingTaskHealth() != null ? getReportingTaskHealth().hashCode() : 0);
        result = 31 * result + (getBulletinList() != null ? getBulletinList().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", reportingTaskHealth=" + reportingTaskHealth +
                ", bulletinList=" + bulletinList +
                '}';
    }
}
