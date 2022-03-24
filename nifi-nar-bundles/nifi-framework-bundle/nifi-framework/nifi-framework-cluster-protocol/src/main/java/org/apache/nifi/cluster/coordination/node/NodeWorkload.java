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

package org.apache.nifi.cluster.coordination.node;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "nodeWorkload")
public class NodeWorkload {

    private long reportedTimestamp;
    private int flowFileCount;
    private long flowFileBytes;
    private int activeThreadCount;
    private long systemStartTime;

    public long getReportedTimestamp() {
        return reportedTimestamp;
    }

    public void setReportedTimestamp(long reportedTimestamp) {
        this.reportedTimestamp = reportedTimestamp;
    }

    public int getFlowFileCount() {
        return flowFileCount;
    }

    public void setFlowFileCount(int flowFileCount) {
        this.flowFileCount = flowFileCount;
    }

    public long getFlowFileBytes() {
        return flowFileBytes;
    }

    public void setFlowFileBytes(long flowFileBytes) {
        this.flowFileBytes = flowFileBytes;
    }

    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(int activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    public long getSystemStartTime() {
        return systemStartTime;
    }

    public void setSystemStartTime(long systemStartTime) {
        this.systemStartTime = systemStartTime;
    }

}
