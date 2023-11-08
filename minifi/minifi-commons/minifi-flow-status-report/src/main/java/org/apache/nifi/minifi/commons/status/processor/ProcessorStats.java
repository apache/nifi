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

package org.apache.nifi.minifi.commons.status.processor;

public class ProcessorStats implements java.io.Serializable {

    private int activeThreads;
    private int flowfilesReceived;
    private long bytesRead;
    private long bytesWritten;
    private int flowfilesSent;
    private int invocations;
    private long processingNanos;

    public ProcessorStats() {
    }

    public int getActiveThreads() {
        return activeThreads;
    }

    public void setActiveThreads(int activeThreads) {
        this.activeThreads = activeThreads;
    }

    public int getFlowfilesReceived() {
        return flowfilesReceived;
    }

    public void setFlowfilesReceived(int flowfilesReceived) {
        this.flowfilesReceived = flowfilesReceived;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public int getFlowfilesSent() {
        return flowfilesSent;
    }

    public void setFlowfilesSent(int flowfilesSent) {
        this.flowfilesSent = flowfilesSent;
    }

    public int getInvocations() {
        return invocations;
    }

    public void setInvocations(int invocations) {
        this.invocations = invocations;
    }

    public long getProcessingNanos() {
        return processingNanos;
    }

    public void setProcessingNanos(long processingNanos) {
        this.processingNanos = processingNanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorStats that = (ProcessorStats) o;

        if (getActiveThreads() != that.getActiveThreads()) return false;
        if (getFlowfilesReceived() != that.getFlowfilesReceived()) return false;
        if (getBytesRead() != that.getBytesRead()) return false;
        if (getBytesWritten() != that.getBytesWritten()) return false;
        if (getFlowfilesSent() != that.getFlowfilesSent()) return false;
        if (getInvocations() != that.getInvocations()) return false;
        return getProcessingNanos() == that.getProcessingNanos();

    }

    @Override
    public int hashCode() {
        int result = getActiveThreads();
        result = 31 * result + getFlowfilesReceived();
        result = 31 * result + (int) (getBytesRead() ^ (getBytesRead() >>> 32));
        result = 31 * result + (int) (getBytesWritten() ^ (getBytesWritten() >>> 32));
        result = 31 * result + getFlowfilesSent();
        result = 31 * result + getInvocations();
        result = 31 * result + (int) (getProcessingNanos() ^ (getProcessingNanos() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "activeThreads=" + activeThreads +
                ", flowfilesReceived=" + flowfilesReceived +
                ", bytesRead=" + bytesRead +
                ", bytesWritten=" + bytesWritten +
                ", flowfilesSent=" + flowfilesSent +
                ", invocations=" + invocations +
                ", processingNanos=" + processingNanos +
                '}';
    }
}
