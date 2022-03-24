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

package org.apache.nifi.web.api.dto.diagnostics;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "threadDump")
public class ThreadDumpDTO {
    private String nodeId;
    private String nodeAddress;
    private Integer apiPort;
    private String stackTrace;
    private String threadName;
    private long threadActiveMillis;
    private boolean taskTerminated;

    @ApiModelProperty("The ID of the node in the cluster")
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @ApiModelProperty("The address of the node in the cluster")
    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    @ApiModelProperty("The port the node is listening for API requests.")
    public Integer getApiPort() {
        return apiPort;
    }

    public void setApiPort(Integer port) {
        this.apiPort = port;
    }

    @ApiModelProperty("The stack trace for the thread")
    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    @ApiModelProperty("The name of the thread")
    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    @ApiModelProperty("The number of milliseconds that the thread has been executing in the Processor")
    public long getThreadActiveMillis() {
        return threadActiveMillis;
    }

    public void setThreadActiveMillis(long threadActiveMillis) {
        this.threadActiveMillis = threadActiveMillis;
    }

    public void setTaskTerminated(final boolean terminated) {
        this.taskTerminated = terminated;
    }

    @ApiModelProperty("Indicates whether or not the user has requested that the task be terminated. If this is true, it may indicate that "
        + "the thread is in a state where it will continue running indefinitely without returning.")
    public boolean isTaskTerminated() {
        return taskTerminated;
    }
}
