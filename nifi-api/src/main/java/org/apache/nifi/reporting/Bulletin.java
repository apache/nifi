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
package org.apache.nifi.reporting;

import java.util.Date;

/**
 * A Bulletin is a construct that represents a message that is to be displayed
 * to users to notify of a specific (usually fleeting) event.
 */
public abstract class Bulletin implements Comparable<Bulletin> {

    private final Date timestamp;
    private final long id;
    private String nodeAddress;
    private String level;
    private String category;
    private String message;

    private String groupId;
    private String sourceId;
    private String sourceName;
    private ComponentType sourceType;

    protected Bulletin(final long id) {
        this.timestamp = new Date();
        this.id = id;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public long getId() {
        return id;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public ComponentType getSourceType() {
        return sourceType;
    }

    public void setSourceType(ComponentType sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public String toString() {
        return "Bulletin{" + "id=" + id + ", message=" + message + ", sourceName=" + sourceName + ", sourceType=" + sourceType + '}';
    }

    @Override
    public int compareTo(Bulletin b) {
        if (b == null) {
            return -1;
        }

        return -Long.compare(getId(), b.getId());
    }
}
