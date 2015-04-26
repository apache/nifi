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
 * The status for a port in this NiFi.
 */
@XmlType(name = "portStatus")
public class PortStatusDTO extends StatusDTO {

    private String id;
    private String groupId;
    private String name;
    private Integer activeThreadCount;
    private String input;
    private String output;
    private Boolean transmitting;
    private String runStatus;

    /**
     * @return whether this port has incoming or outgoing connections to a remote NiFi
     */
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * @return the active thread count for this port
     */
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * @return id of this port
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return id of the group this port resides in
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of this port
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return run status of this port
     */
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    /**
     * @return The total count and size of flow files that have been accepted in the
     * last five minutes
     */
    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    /**
     * @return The total count and size of flow files that have been processed in the
     * last five minutes
     */
    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

}
