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
package org.apache.nifi.controller.queue.clustered.dto;

import java.util.Objects;

public class PartitionStatus {
    private final int objectCount;
    private final long totalSizeBytes;
    private final int flowFilesOut;

    public PartitionStatus(final int objectCount, final long totalSizeBytes, final int flowFilesOut) {
        this.objectCount = objectCount;
        this.totalSizeBytes = totalSizeBytes;
        this.flowFilesOut = flowFilesOut;
    }

    public int getObjectCount() {
        return objectCount;
    }

    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public int getFlowFilesOut() {
        return flowFilesOut;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PartitionStatus{");
        sb.append("objectCount=").append(objectCount);
        sb.append(",totalSizeBytes=").append(totalSizeBytes);
        sb.append(",flowFilesOut=").append(flowFilesOut);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionStatus that = (PartitionStatus) o;
        return objectCount == that.objectCount && totalSizeBytes == that.totalSizeBytes && flowFilesOut == that.flowFilesOut;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectCount, totalSizeBytes, flowFilesOut);
    }
}
