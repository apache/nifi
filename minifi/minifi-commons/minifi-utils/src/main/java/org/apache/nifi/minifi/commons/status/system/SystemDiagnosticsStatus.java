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

package org.apache.nifi.minifi.commons.status.system;

import java.util.List;

public class SystemDiagnosticsStatus implements java.io.Serializable {
    private List<GarbageCollectionStatus> garbageCollectionStatusList;
    private HeapStatus heapStatus;
    private SystemProcessorStats systemProcessorStats;
    private List<ContentRepositoryUsage> contentRepositoryUsageList;
    private FlowfileRepositoryUsage flowfileRepositoryUsage;

    public SystemDiagnosticsStatus() {
    }

    public List<GarbageCollectionStatus> getGarbageCollectionStatusList() {
        return garbageCollectionStatusList;
    }

    public void setGarbageCollectionStatusList(List<GarbageCollectionStatus> garbageCollectionStatusList) {
        this.garbageCollectionStatusList = garbageCollectionStatusList;
    }

    public HeapStatus getHeapStatus() {
        return heapStatus;
    }

    public void setHeapStatus(HeapStatus heapStatus) {
        this.heapStatus = heapStatus;
    }

    public SystemProcessorStats getProcessorStatus() {
        return systemProcessorStats;
    }

    public void setProcessorStatus(SystemProcessorStats processorStatus) {
        this.systemProcessorStats = processorStatus;
    }

    public List<ContentRepositoryUsage> getContentRepositoryUsageList() {
        return contentRepositoryUsageList;
    }

    public void setContentRepositoryUsageList(List<ContentRepositoryUsage> contentRepositoryUsageList) {
        this.contentRepositoryUsageList = contentRepositoryUsageList;
    }

    public FlowfileRepositoryUsage getFlowfileRepositoryUsage() {
        return flowfileRepositoryUsage;
    }

    public void setFlowfileRepositoryUsage(FlowfileRepositoryUsage flowfileRepositoryUsage) {
        this.flowfileRepositoryUsage = flowfileRepositoryUsage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SystemDiagnosticsStatus that = (SystemDiagnosticsStatus) o;

        if (getGarbageCollectionStatusList() != null ? !getGarbageCollectionStatusList().equals(that.getGarbageCollectionStatusList()) : that.getGarbageCollectionStatusList() != null) return false;
        if (getHeapStatus() != null ? !getHeapStatus().equals(that.getHeapStatus()) : that.getHeapStatus() != null) return false;
        if (systemProcessorStats != null ? !systemProcessorStats.equals(that.systemProcessorStats) : that.systemProcessorStats != null) return false;
        if (getContentRepositoryUsageList() != null ? !getContentRepositoryUsageList().equals(that.getContentRepositoryUsageList()) : that.getContentRepositoryUsageList() != null) return false;
        return getFlowfileRepositoryUsage() != null ? getFlowfileRepositoryUsage().equals(that.getFlowfileRepositoryUsage()) : that.getFlowfileRepositoryUsage() == null;

    }

    @Override
    public int hashCode() {
        int result = getGarbageCollectionStatusList() != null ? getGarbageCollectionStatusList().hashCode() : 0;
        result = 31 * result + (getHeapStatus() != null ? getHeapStatus().hashCode() : 0);
        result = 31 * result + (systemProcessorStats != null ? systemProcessorStats.hashCode() : 0);
        result = 31 * result + (getContentRepositoryUsageList() != null ? getContentRepositoryUsageList().hashCode() : 0);
        result = 31 * result + (getFlowfileRepositoryUsage() != null ? getFlowfileRepositoryUsage().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "garbageCollectionStatusList=" + garbageCollectionStatusList +
                ", heapStatus=" + heapStatus +
                ", systemProcessorStats=" + systemProcessorStats +
                ", contentRepositoryUsageList=" + contentRepositoryUsageList +
                ", flowfileRepositoryUsage=" + flowfileRepositoryUsage +
                '}';
    }
}
