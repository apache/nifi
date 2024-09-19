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

package org.apache.nifi.c2.protocol.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;

public class ProcessorStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String groupId;
    private long bytesRead;
    private long bytesWritten;
    private long flowFilesIn;
    private long flowFilesOut;
    private long bytesIn;
    private long bytesOut;
    private int invocations;
    private long processingNanos;
    private int activeThreadCount;
    private int terminatedThreadCount;

    @Schema(description = "The id of the processor")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Schema(description = "The group id of the processor")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The number of bytes read by the processor")
    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    @Schema(description = "The number of bytes written by the processor")
    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    @Schema(description = "The number of accepted flow files")
    public long getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(long flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @Schema(description = "The number of transferred flow files")
    public long getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(long flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @Schema(description = "The size of accepted flow files")
    public long getBytesIn() {
        return bytesIn;
    }

    public void setBytesIn(long bytesIn) {
        this.bytesIn = bytesIn;
    }

    @Schema(description = "The size of transferred flow files")
    public long getBytesOut() {
        return bytesOut;
    }

    public void setBytesOut(long bytesOut) {
        this.bytesOut = bytesOut;
    }

    @Schema(description = "The number of invocations")
    public int getInvocations() {
        return invocations;
    }

    public void setInvocations(int invocations) {
        this.invocations = invocations;
    }

    @Schema(description = "The number of nanoseconds that the processor has spent running")
    public long getProcessingNanos() {
        return processingNanos;
    }

    public void setProcessingNanos(long processingNanos) {
        this.processingNanos = processingNanos;
    }

    @Schema(description = "The number of active threads currently executing")
    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(int activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    @Schema(description = "The number of threads currently terminated")
    public int getTerminatedThreadCount() {
        return terminatedThreadCount;
    }

    public void setTerminatedThreadCount(int terminatedThreadCount) {
        this.terminatedThreadCount = terminatedThreadCount;
    }
}
