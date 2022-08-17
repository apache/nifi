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
package org.apache.nifi.registry.provider.flow.git;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class Flow {
    /**
     * The ID of a Flow. It never changes.
     */
    private final String flowId;

    /**
     * A version to a Flow pointer.
     */
    private final Map<Integer, FlowPointer> versions = new HashMap<>();

    public Flow(String flowId) {
        this.flowId = flowId;
    }

    public boolean hasVersion(int version) {
        return versions.containsKey(version);
    }

    public FlowPointer getFlowVersion(int version) {
        return versions.get(version);
    }

    public void putVersion(int version, FlowPointer pointer) {
        versions.put(version, pointer);
    }

    Map<Integer, FlowPointer> getVersions() {
        return versions;
    }

    public static class FlowPointer {
        private String gitRev;
        private String objectId;
        private final String fileName;

        // May not be populated pre-0.3.0
        private String flowName;
        private String flowDescription;
        private String author;
        private String comment;
        private Long created;

        /**
         * Create new FlowPointer instance.
         * @param fileName The filename must be sanitized, use {@link org.apache.nifi.registry.util.FileUtils#sanitizeFilename(String)} to do so.
         */
        public FlowPointer(String fileName) {
            this.fileName = fileName;
        }

        public void setGitRev(String gitRev) {
            this.gitRev = gitRev;
        }

        public String getGitRev() {
            return gitRev;
        }

        public String getFileName() {
            return fileName;
        }

        public String getObjectId() {
            return objectId;
        }

        public void setObjectId(String objectId) {
            this.objectId = objectId;
        }

        public String getFlowName() {
            return flowName;
        }

        public void setFlowName(String flowName) {
            this.flowName = flowName;
        }

        public String getFlowDescription() {
            return flowDescription;
        }

        public void setFlowDescription(String flowDescription) {
            this.flowDescription = flowDescription;
        }

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public Long getCreated() {
            return created;
        }

        public void setCreated(Long created) {
            this.created = created;
        }
    }

    /**
     * Serialize the latest version of this Flow meta data.
     * @return serialized flow
     */
    Map<String, Object> serialize() {
        final Map<String, Object> map = new HashMap<>();
        final Optional<Integer> latestVerOpt = getLatestVersion();
        if (!latestVerOpt.isPresent()) {
            throw new IllegalStateException("Flow version is not added yet, can not be serialized.");
        }

        final Integer latestVer = latestVerOpt.get();
        final Flow.FlowPointer latestFlowPointer = versions.get(latestVer);

        map.put(GitFlowMetaData.VER, latestVer);
        map.put(GitFlowMetaData.FILE, latestFlowPointer.fileName);

        if (latestFlowPointer.flowName != null) {
            map.put(GitFlowMetaData.FLOW_NAME, latestFlowPointer.flowName);
        }
        if (latestFlowPointer.flowDescription != null) {
            map.put(GitFlowMetaData.FLOW_DESC, latestFlowPointer.flowDescription);
        }
        if (latestFlowPointer.author != null) {
            map.put(GitFlowMetaData.AUTHOR, latestFlowPointer.author);
        }
        if (latestFlowPointer.comment != null) {
            map.put(GitFlowMetaData.COMMENTS, latestFlowPointer.comment);
        }
        if (latestFlowPointer.created != null) {
            map.put(GitFlowMetaData.CREATED, latestFlowPointer.created);
        }

        return map;
    }

    Optional<Integer> getLatestVersion() {
        return versions.keySet().stream().reduce(Integer::max);
    }

}
