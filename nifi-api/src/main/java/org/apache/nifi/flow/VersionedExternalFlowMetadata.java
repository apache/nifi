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

package org.apache.nifi.flow;

public class VersionedExternalFlowMetadata {
    private String bucketId;
    private String flowId;
    private String version;
    private String flowName;
    private String author;
    private String comments;
    private long timestamp;

    public String getBucketIdentifier() {
        return bucketId;
    }

    public void setBucketIdentifier(final String bucketIdentifier) {
        this.bucketId = bucketIdentifier;
    }

    public String getFlowIdentifier() {
        return flowId;
    }

    public void setFlowIdentifier(final String flowId) {
        this.flowId = flowId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(final String flowName) {
        this.flowName = flowName;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(final String author) {
        this.author = author;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(final String comments) {
        this.comments = comments;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }
}
