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
package org.apache.nifi.controller.status;

public class PerformanceMetrics implements Cloneable {

    private String identifier;
    private long cpuTime;
    private long cpuTimePercentage;
    private long readTime;
    private long readTimePercentage;
    private long writeTime;
    private long writeTimePercentage;
    private long commitTime;
    private long commitTimePercentage;
    private long gcTime;
    private long gcTimePercentage;
    private long bytesRead;
    private long bytesReadPercentage;
    private long bytesWritten;
    private long bytesWrittenPercentage;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public long getCpuTime() {
        return cpuTime;
    }

    public void setCpuTime(long cpuTime) {
        this.cpuTime = cpuTime;
    }

    public long getCpuTimePercentage() {
        return cpuTimePercentage;
    }

    public void setCpuTimePercentage(long cpuTimePercentage) {
        this.cpuTimePercentage = cpuTimePercentage;
    }

    public long getReadTime() {
        return readTime;
    }

    public void setReadTime(long readTime) {
        this.readTime = readTime;
    }

    public long getReadTimePercentage() {
        return readTimePercentage;
    }

    public void setReadTimePercentage(long readTimePercentage) {
        this.readTimePercentage = readTimePercentage;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    public long getWriteTimePercentage() {
        return writeTimePercentage;
    }

    public void setWriteTimePercentage(long writeTimePercentage) {
        this.writeTimePercentage = writeTimePercentage;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(long commitTime) {
        this.commitTime = commitTime;
    }

    public long getCommitTimePercentage() {
        return commitTimePercentage;
    }

    public void setCommitTimePercentage(long commitTimePercentage) {
        this.commitTimePercentage = commitTimePercentage;
    }

    public long getGcTime() {
        return gcTime;
    }

    public void setGcTime(long gcTime) {
        this.gcTime = gcTime;
    }

    public long getGcTimePercentage() {
        return gcTimePercentage;
    }

    public void setGcTimePercentage(long gcTimePercentage) {
        this.gcTimePercentage = gcTimePercentage;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getBytesReadPercentage() {
        return bytesReadPercentage;
    }

    public void setBytesReadPercentage(long bytesReadPercentage) {
        this.bytesReadPercentage = bytesReadPercentage;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public long getBytesWrittenPercentage() {
        return bytesWrittenPercentage;
    }

    public void setBytesWrittenPercentage(long bytesWrittenPercentage) {
        this.bytesWrittenPercentage = bytesWrittenPercentage;
    }

    public static void merge(final PerformanceMetrics target, final PerformanceMetrics toMerge) {
        if (target == null || toMerge == null) {
            return;
        }

        target.setIdentifier(toMerge.getIdentifier());
        target.setCpuTime(target.getCpuTime() + toMerge.getCpuTime());
        target.setCpuTimePercentage(Math.min(100L, target.getCpuTimePercentage() + toMerge.getCpuTimePercentage()));
        target.setReadTime(target.getReadTime() + toMerge.getReadTime());
        target.setReadTimePercentage(Math.min(100L, target.getReadTimePercentage() + toMerge.getReadTimePercentage()));
        target.setWriteTime(target.getWriteTime() + toMerge.getWriteTime());
        target.setWriteTimePercentage(Math.min(100L, target.getWriteTimePercentage() + toMerge.getWriteTimePercentage()));
        target.setCommitTime(target.getCommitTime() + toMerge.getCommitTime());
        target.setCommitTimePercentage(Math.min(100L, target.getCommitTimePercentage() + toMerge.getCommitTimePercentage()));
        target.setGcTime(target.getGcTime() + toMerge.getGcTime());
        target.setGcTimePercentage(Math.min(100L, target.getGcTimePercentage() + toMerge.getGcTimePercentage()));
        target.setBytesRead(target.getBytesRead() + toMerge.getBytesRead());
        target.setBytesReadPercentage(Math.min(100L, target.getBytesReadPercentage() + toMerge.getBytesReadPercentage()));
        target.setBytesWritten(toMerge.getBytesWritten() + toMerge.getBytesWritten());
        target.setBytesWrittenPercentage(Math.min(100L, target.getBytesWrittenPercentage() + toMerge.getBytesWrittenPercentage()));
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        final PerformanceMetrics clonedObj = new PerformanceMetrics();

        clonedObj.identifier = identifier;
        clonedObj.cpuTime = cpuTime;
        clonedObj.cpuTimePercentage = cpuTimePercentage;
        clonedObj.readTime = readTime;
        clonedObj.readTimePercentage = readTimePercentage;
        clonedObj.writeTime = writeTime;
        clonedObj.writeTimePercentage = writeTimePercentage;
        clonedObj.commitTime = commitTime;
        clonedObj.commitTimePercentage = commitTimePercentage;
        clonedObj.gcTime = gcTime;
        clonedObj.gcTimePercentage = gcTimePercentage;
        clonedObj.bytesRead = bytesRead;
        clonedObj.bytesReadPercentage = bytesReadPercentage;
        clonedObj.bytesWritten = bytesWritten;
        clonedObj.bytesWrittenPercentage = bytesWrittenPercentage;

        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProcessorPerformanceStatus [Group ID= ");
        builder.append(identifier);
        builder.append(", cpuTime= ");
        builder.append(cpuTime);
        builder.append(", cpuTimePercentage= ");
        builder.append(cpuTimePercentage);
        builder.append(", readTime= ");
        builder.append(readTime);
        builder.append(", readTimePercentage= ");
        builder.append(readTimePercentage);
        builder.append(", writeTime= ");
        builder.append(writeTime);
        builder.append(", writeTimePercentage= ");
        builder.append(writeTimePercentage);
        builder.append(", commitTime= ");
        builder.append(commitTime);
        builder.append(", commitTimePercentage= ");
        builder.append(commitTimePercentage);
        builder.append(", gcTime= ");
        builder.append(gcTime);
        builder.append(", gcTimePercentage= ");
        builder.append(gcTimePercentage);
        builder.append(", bytesReadPercentage= ");
        builder.append(bytesReadPercentage);
        builder.append(", bytesWrittenPercentage= ");
        builder.append(bytesWrittenPercentage);
        builder.append("]");
        return builder.toString();
    }
}
