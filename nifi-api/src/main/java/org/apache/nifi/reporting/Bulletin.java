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

import org.apache.nifi.flowfile.FlowFile;

import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Bulletin is a construct that represents a message that is to be displayed
 * to users to notify of a specific (usually fleeting) event.
 */
public class Bulletin implements Comparable<Bulletin> {

    private static final AtomicLong bulletinId = new AtomicLong(-1);

    private final Date timestamp;
    private final long id;
    private final String nodeAddress;
    private final String level;
    private final String category;
    private final String message;

    private final String groupId;
    private final String groupName;
    private final String groupPath;
    private final String sourceId;
    private final String sourceName;
    private final ComponentType sourceType;
    private final FlowFile flowFile;

    private Bulletin(Builder builder) {
        timestamp = builder.timestamp;
        id = builder.id;
        nodeAddress = builder.nodeAddress;
        level = builder.level;
        category = builder.category;
        message = builder.message;

        groupId = builder.groupId;
        groupName = builder.groupName;
        groupPath = builder.groupPath;
        sourceId = builder.sourceId;
        sourceName = builder.sourceName;
        sourceType = builder.sourceType;
        flowFile = builder.flowFile;
    }

    public static class Builder {

        private long id;
        private final Date timestamp;
        private String nodeAddress;
        private String level;
        private String category;
        private String message;
        private String groupId;
        private String groupName;
        private String groupPath;
        private String sourceId;
        private String sourceName;
        private ComponentType sourceType;
        private FlowFile flowFile;

        public Builder() {
            timestamp = new Date();
        }

        public Builder nodeAddress(String nodeAddress) {
            this.nodeAddress = nodeAddress;
            return this;
        }

        public Builder level(String level) {
            this.level = level;
            return this;
        }

        public Builder category(String category) {
            this.category = category;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

        public Builder groupPath(String groupPath) {
            this.groupPath = groupPath;
            return this;
        }

        public Builder sourceId(String sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        public Builder sourceName(String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public Builder sourceType(ComponentType sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder flowfile(FlowFile flowFile) {
            this.flowFile = flowFile;
            return this;
        }

        public Bulletin build() {
            this.id = bulletinId.incrementAndGet();
            return new Bulletin(this);
        }
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public long getId() {
        return id;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public String getLevel() {
        return level;
    }

    public String getCategory() {
        return category;
    }

    public String getMessage() {
        return message;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getGroupPath() {
        return groupPath;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public ComponentType getSourceType() {
        return sourceType;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    @Override
    public String toString() {
        return "Bulletin{" +
                "timestamp=" + timestamp +
                ", id=" + id +
                ", nodeAddress='" + nodeAddress + '\'' +
                ", level='" + level + '\'' +
                ", category='" + category + '\'' +
                ", message='" + message + '\'' +
                ", groupId='" + groupId + '\'' +
                ", groupName='" + groupName + '\'' +
                ", groupPath='" + groupPath + '\'' +
                ", sourceId='" + sourceId + '\'' +
                ", sourceName='" + sourceName + '\'' +
                ", sourceType=" + sourceType +
                ", flowFile=" + flowFile +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bulletin bulletin = (Bulletin) o;
        return id == bulletin.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public int compareTo(Bulletin b) {
        if (b == null) {
            return -1;
        }

        return -Long.compare(getId(), b.getId());
    }
}
