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
package org.apache.nifi.registry.flow;

public class RegisteredFlowSnapshotMetadata {
    private String branch;
    private String bucketIdentifier;
    private String flowIdentifier;
    private String version;
    private long timestamp;
    private String author;
    private String comments;

    public String getBranch() {
        return branch;
    }

    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    public String getFlowIdentifier() {
        return flowIdentifier;
    }

    public String getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getAuthor() {
        return author;
    }

    public String getComments() {
        return comments;
    }

    public void setBranch(final String branch) {
        this.branch = branch;
    }

    public void setBucketIdentifier(String bucketIdentifier) {
        this.bucketIdentifier = bucketIdentifier;
    }

    public void setFlowIdentifier(String flowIdentifier) {
        this.flowIdentifier = flowIdentifier;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }
}
