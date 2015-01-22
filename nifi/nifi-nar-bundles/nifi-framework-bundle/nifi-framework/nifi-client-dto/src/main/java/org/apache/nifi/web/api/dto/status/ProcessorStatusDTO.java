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

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the status of a processor.
 */
@XmlType(name = "processorStatus")
public class ProcessorStatusDTO extends StatusDTO {

    private String id;
    private String groupId;
    private String name;
    private String type;
    private String runStatus;

    private String read;
    private String written;

    private String input;
    private String output;

    private String tasks;
    private String tasksDuration;
    private Integer activeThreadCount;

    /* getters / setters */
    /**
     * The id for the processor.
     *
     * @return The processor id
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The name of the processor.
     *
     * @return The processor name
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The type of the processor.
     *
     * @return The processor type
     */
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * The run status of this processor.
     *
     * @return
     */
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    /**
     * The total count and size of flow files that have been accepted in the
     * last five minutes.
     *
     * @return The total processed
     */
    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    /**
     * The number of bytes read.
     *
     * @return
     */
    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    /**
     * The number of bytes written.
     *
     * @return
     */
    public String getWritten() {
        return written;
    }

    public void setWritten(String written) {
        this.written = written;
    }

    /**
     * The ID of the Process Group to which this processor belongs.
     *
     * @return the ID of the Process Group to which this processor belongs.
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * The total count and size of flow files that have been processed in the
     * last five minutes.
     *
     * @return The total output
     */
    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    /**
     * The number of threads currently running for this Processor
     *
     * @return
     */
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer threadCount) {
        this.activeThreadCount = threadCount;
    }

    /**
     * The number of task this connectable has had over the last 5 minutes.
     *
     * @return
     */
    public String getTasks() {
        return tasks;
    }

    public void setTasks(String tasks) {
        this.tasks = tasks;
    }

    /**
     * The total duration of all tasks for this connectable over the last 5
     * minutes.
     *
     * @return
     */
    public String getTasksDuration() {
        return tasksDuration;
    }

    public void setTasksDuration(String tasksDuration) {
        this.tasksDuration = tasksDuration;
    }

}
