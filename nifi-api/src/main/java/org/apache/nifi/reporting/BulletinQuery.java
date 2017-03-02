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

import java.util.regex.Pattern;

/**
 *
 */
public class BulletinQuery {

    private final ComponentType sourceType;
    private final Pattern sourceIdPattern;
    private final Pattern groupIdPattern;
    private final Pattern namePattern;
    private final Pattern messagePattern;
    private final Long after;
    private final Integer limit;

    private BulletinQuery(final Builder builder) {
        this.sourceType = builder.sourceType;
        this.sourceIdPattern = builder.sourceIdPattern == null ? null : Pattern.compile(builder.sourceIdPattern);
        this.groupIdPattern = builder.groupIdPattern == null ? null : Pattern.compile(builder.groupIdPattern);
        this.namePattern = builder.namePattern == null ? null : Pattern.compile(builder.namePattern);
        this.messagePattern = builder.messagePattern == null ? null : Pattern.compile(builder.messagePattern);
        this.after = builder.after;
        this.limit = builder.limit;
    }

    public ComponentType getSourceType() {
        return sourceType;
    }

    public Pattern getSourceIdPattern() {
        return sourceIdPattern;
    }

    public Pattern getGroupIdPattern() {
        return groupIdPattern;
    }

    public Pattern getNamePattern() {
        return namePattern;
    }

    public Pattern getMessagePattern() {
        return messagePattern;
    }

    public Long getAfter() {
        return after;
    }

    public Integer getLimit() {
        return limit;
    }

    public static class Builder {

        private ComponentType sourceType;
        private String sourceIdPattern;
        private String groupIdPattern;
        private String namePattern;
        private String messagePattern;
        private Long after;
        private Integer limit;

        public Builder after(Long after) {
            this.after = after;
            return this;
        }

        public Builder groupIdMatches(String groupId) {
            this.groupIdPattern = groupId;
            return this;
        }

        public Builder sourceIdMatches(String sourceId) {
            this.sourceIdPattern = sourceId;
            return this;
        }

        public Builder sourceType(ComponentType sourceType) {
            this.sourceType = sourceType;
            return this;
        }

        public Builder nameMatches(String name) {
            this.namePattern = name;
            return this;
        }

        public Builder messageMatches(String message) {
            this.messagePattern = message;
            return this;
        }

        public Builder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        public BulletinQuery build() {
            return new BulletinQuery(this);
        }
    }
}
