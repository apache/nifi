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
 * DTO for serializing the status of a connection.
 */
@XmlType(name = "connectionStatus")
public class ConnectionStatusDTO {

    private String id;
    private String groupId;
    private String name;
    private String input;
    private String queuedCount;
    private String queuedSize;
    private String queued;
    private String output;

    private String sourceId;
    private String sourceName;
    private String destinationId;
    private String destinationName;

    /* getters / setters */
    /**
     * @return The connection id
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the ID of the Process Group to which this processor belongs.
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of this connection
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return total count of flow files that are queued
     */
    public String getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(String queuedCount) {
        this.queuedCount = queuedCount;
    }

    /**
     * @return total size of flow files that are queued
     */
    public String getQueuedSize() {
        return queuedSize;
    }

    public void setQueuedSize(String queuedSize) {
        this.queuedSize = queuedSize;
    }

    /**
     * @return The total count and size of queued flow files
     */
    public String getQueued() {
        return queued;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    /**
     * @return id of the source of this connection
     */
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return name of the source of this connection
     */
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * @return id of the destination of this connection
     */
    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    /**
     * @return name of the destination of this connection
     */
    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    /**
     * @return input for this connection
     */
    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    /**
     * @return output for this connection
     */
    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

}
