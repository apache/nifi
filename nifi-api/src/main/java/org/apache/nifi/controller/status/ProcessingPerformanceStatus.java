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

public class ProcessingPerformanceStatus implements Cloneable {

    private String identifier;
    private long cpuTime;
    private long readTime;
    private long writeTime;
    private long commitTime;
    private long gcTime;
    private long bytesRead;
    private long bytesWritten;

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

    public long getReadTime() {
        return readTime;
    }

    public void setReadTime(long readTime) {
        this.readTime = readTime;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public void setCommitTime(long commitTime) {
        this.commitTime = commitTime;
    }

    public long getGcTime() {
        return gcTime;
    }

    public void setGcTime(long gcTime) {
        this.gcTime = gcTime;
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

    @Override
    protected Object clone() throws CloneNotSupportedException {
        final ProcessingPerformanceStatus clonedObj = new ProcessingPerformanceStatus();

        clonedObj.identifier = identifier;
        clonedObj.cpuTime = cpuTime;
        clonedObj.readTime = readTime;
        clonedObj.writeTime = writeTime;
        clonedObj.commitTime = commitTime;
        clonedObj.gcTime = gcTime;
        clonedObj.bytesRead = bytesRead;
        clonedObj.bytesWritten = bytesWritten;

        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProcessorPerformanceStatus [Group ID= ");
        builder.append(identifier);
        builder.append(", cpuTime= ");
        builder.append(cpuTime);
        builder.append(", readTime= ");
        builder.append(readTime);
        builder.append(", writeTime= ");
        builder.append(writeTime);
        builder.append(", commitTime= ");
        builder.append(commitTime);
        builder.append(", gcTime= ");
        builder.append(gcTime);
        builder.append("]");
        return builder.toString();
    }
}
