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
package org.apache.nifi.web.api.dto.status;

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the status of a processor.
 */
@XmlType(name = "processorStatusSnapshot")
public class ProcessorStatusSnapshotDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private String type;
    private String runStatus;

    private Long bytesRead = 0L;
    private Long bytesWritten = 0L;
    private String read;
    private String written;

    private Integer flowFilesIn = 0;
    private Long bytesIn = 0L;
    private String input;

    private Integer flowFilesOut = 0;
    private Long bytesOut = 0L;
    private String output;

    private Integer taskCount = 0;
    private Long tasksDurationNanos = 0L;
    private String tasks;
    private String tasksDuration;
    private Integer activeThreadCount = 0;

    /* getters / setters */
    /**
     * @return The processor id
     */
    @ApiModelProperty("The id of the processor.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The processor name
     */
    @ApiModelProperty("The name of the prcessor.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return The processor type
     */
    @ApiModelProperty("The type of the processor.")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * @return run status of this processor
     */
    @ApiModelProperty(
            value = "The state of the processor.",
            allowableValues = "RUNNING, STOPPED, DISABLED, INVALID"
    )
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    /**
     * @return The total count and size of flow files that have been accepted in the last five minutes
     */
    @ApiModelProperty("The count/size of flowfiles that have been accepted in the last 5 minutes.")
    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    /**
     * @return number of bytes read
     */
    @ApiModelProperty("The number of bytes read in the last 5 minutes.")
    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    /**
     * @return number of bytes written
     */
    @ApiModelProperty("The number of bytes written in the last 5 minutes.")
    public String getWritten() {
        return written;
    }

    public void setWritten(String written) {
        this.written = written;
    }

    /**
     * @return the ID of the Process Group to which this processor belongs.
     */
    @ApiModelProperty("The id of the parent process group to which the processor belongs.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return The total count and size of flow files that have been processed in the last five minutes
     */
    @ApiModelProperty("The count/size of flowfiles that have been processed in the last 5 minutes.")
    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    /**
     * @return number of threads currently running for this Processor
     */
    @ApiModelProperty("The number of threads currently executing in the processor.")
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer threadCount) {
        this.activeThreadCount = threadCount;
    }

    /**
     * @return number of task this connectable has had over the last 5 minutes
     */
    @ApiModelProperty("The total number of task this connectable has completed over the last 5 minutes.")
    public String getTasks() {
        return tasks;
    }

    public void setTasks(String tasks) {
        this.tasks = tasks;
    }

    /**
     * @return total duration of all tasks for this connectable over the last 5 minutes
     */
    @ApiModelProperty("The total duration of all tasks for this connectable over the last 5 minutes.")
    public String getTasksDuration() {
        return tasksDuration;
    }

    public void setTasksDuration(String tasksDuration) {
        this.tasksDuration = tasksDuration;
    }

    @ApiModelProperty("The number of bytes read by this Processor in the last 5 mintues")
    public Long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(Long bytesRead) {
        this.bytesRead = bytesRead;
    }

    @ApiModelProperty("The number of bytes written by this Processor in the last 5 minutes")
    public Long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(Long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    @ApiModelProperty("The number of FlowFiles that have been accepted in the last 5 minutes")
    public Integer getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(Integer flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @ApiModelProperty("The size of the FlowFiles that have been accepted in the last 5 minutes")
    public Long getBytesIn() {
        return bytesIn;
    }

    public void setBytesIn(Long bytesIn) {
        this.bytesIn = bytesIn;
    }

    @ApiModelProperty("The number of FlowFiles transferred to a Connection in the last 5 minutes")
    public Integer getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(Integer flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @ApiModelProperty("The size of the FlowFiles transferred to a Connection in the last 5 minutes")
    public Long getBytesOut() {
        return bytesOut;
    }

    public void setBytesOut(Long bytesOut) {
        this.bytesOut = bytesOut;
    }

    @ApiModelProperty("The number of times this Processor has run in the last 5 minutes")
    public Integer getTaskCount() {
        return taskCount;
    }

    public void setTaskCount(Integer taskCount) {
        this.taskCount = taskCount;
    }

    @ApiModelProperty("The number of nanoseconds that this Processor has spent running in the last 5 minutes")
    public Long getTasksDurationNanos() {
        return tasksDurationNanos;
    }

    public void setTasksDurationNanos(Long taskNanos) {
        this.tasksDurationNanos = taskNanos;
    }

    @Override
    public ProcessorStatusSnapshotDTO clone() {
        final ProcessorStatusSnapshotDTO other = new ProcessorStatusSnapshotDTO();
        other.setId(getId());
        other.setGroupId(getGroupId());
        other.setName(getName());
        other.setType(getType());

        other.setRunStatus(getRunStatus());
        other.setBytesRead(getBytesRead());
        other.setBytesWritten(getBytesWritten());
        other.setFlowFilesIn(getFlowFilesIn());
        other.setBytesIn(getBytesIn());
        other.setFlowFilesOut(getFlowFilesOut());
        other.setBytesOut(getBytesOut());
        other.setTaskCount(getTaskCount());
        other.setTasksDuration(getTasksDuration());
        other.setTasksDurationNanos(getTasksDurationNanos());
        other.setActiveThreadCount(getActiveThreadCount());
        other.setInput(getInput());
        other.setOutput(getOutput());
        other.setRead(getRead());
        other.setWritten(getWritten());
        other.setTasks(getTasks());

        return other;
    }
}
