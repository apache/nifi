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
package org.apache.nifi.web.api.dto;

import javax.xml.bind.annotation.XmlType;

/**
 * Details about a connectable component.
 */
@XmlType(name = "connectable")
public class ConnectableDTO {

    private String id;
    private String type;
    private String groupId;
    private String name;
    private Boolean running;
    private Boolean transmitting;
    private Boolean exists;
    private String comments;

    /**
     * The id of this connectable component.
     *
     * @return
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The type of this connectable component.
     *
     * @return
     */
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * The id of the group that this connectable component resides in.
     *
     * @return
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * The name of this connectable component.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Used to reflect the current state of this Connectable.
     *
     * @return
     */
    public Boolean isRunning() {
        return running;
    }

    public void setRunning(Boolean running) {
        this.running = running;
    }

    /**
     * If this represents a remote port it is used to indicate whether the
     * target exists.
     *
     * @return
     */
    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    /**
     * If this represents a remote port it is used to indicate whether is it
     * configured to transmit.
     *
     * @return
     */
    public Boolean getTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * The comments from this Connectable.
     *
     * @return
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public String toString() {
        return "ConnectableDTO [Type=" + type + ", Name=" + name + ", Id=" + id + "]";
    }
}
