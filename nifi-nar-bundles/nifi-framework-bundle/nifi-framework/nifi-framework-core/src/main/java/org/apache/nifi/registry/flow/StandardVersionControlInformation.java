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

import java.util.Objects;

import org.apache.nifi.web.api.dto.VersionControlInformationDTO;

public class StandardVersionControlInformation implements VersionControlInformation {

    private final String registryIdentifier;
    private volatile String registryName;
    private final String bucketIdentifier;
    private volatile String bucketName;
    private final String flowIdentifier;
    private volatile String flowName;
    private volatile String flowDescription;
    private final int version;
    private volatile VersionedProcessGroup flowSnapshot;
    private final VersionedFlowStatus status;

    public static class Builder {
        private String registryIdentifier;
        private String registryName;
        private String bucketIdentifier;
        private String bucketName;
        private String flowIdentifier;
        private String flowName;
        private String flowDescription;
        private int version;
        private VersionedProcessGroup flowSnapshot;
        private VersionedFlowStatus status;

        public Builder registryId(String registryId) {
            this.registryIdentifier = registryId;
            return this;
        }

        public Builder registryName(String registryName) {
            this.registryName = registryName;
            return this;
        }

        public Builder bucketId(String bucketId) {
            this.bucketIdentifier = bucketId;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder flowId(String flowId) {
            this.flowIdentifier = flowId;
            return this;
        }

        public Builder flowName(String flowName) {
            this.flowName = flowName;
            return this;
        }

        public Builder flowDescription(String flowDescription) {
            this.flowDescription = flowDescription;
            return this;
        }

        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder flowSnapshot(VersionedProcessGroup snapshot) {
            this.flowSnapshot = snapshot;
            return this;
        }

        public Builder status(final VersionedFlowStatus status) {
            this.status = status;
            return this;
        }

        public static Builder fromDto(VersionControlInformationDTO dto) {
            Builder builder = new Builder();
            builder.registryId(dto.getRegistryId())
                .registryName(dto.getRegistryName())
                .bucketId(dto.getBucketId())
                .bucketName(dto.getBucketName())
                .flowId(dto.getFlowId())
                .flowName(dto.getFlowName())
                .flowDescription(dto.getFlowDescription())
                .status(new VersionedFlowStatus() {
                    @Override
                    public VersionedFlowState getState() {
                        return VersionedFlowState.valueOf(dto.getState());
                    }

                    @Override
                    public String getStateExplanation() {
                        return dto.getStateExplanation();
                    }
                })
                .version(dto.getVersion());

            return builder;
        }

        public StandardVersionControlInformation build() {
            Objects.requireNonNull(registryIdentifier, "Registry ID must be specified");
            Objects.requireNonNull(bucketIdentifier, "Bucket ID must be specified");
            Objects.requireNonNull(flowIdentifier, "Flow ID must be specified");
            Objects.requireNonNull(version, "Version must be specified");

            final StandardVersionControlInformation svci = new StandardVersionControlInformation(registryIdentifier, registryName,
                bucketIdentifier, flowIdentifier, version, flowSnapshot, status);

            svci.setBucketName(bucketName);
            svci.setFlowName(flowName);
            svci.setFlowDescription(flowDescription);

            return svci;
        }
    }


    public StandardVersionControlInformation(final String registryId, final String registryName, final String bucketId, final String flowId, final int version,
        final VersionedProcessGroup snapshot, final VersionedFlowStatus status) {
        this.registryIdentifier = registryId;
        this.registryName = registryName;
        this.bucketIdentifier = bucketId;
        this.flowIdentifier = flowId;
        this.version = version;
        this.flowSnapshot = snapshot;
        this.status = status;
    }


    @Override
    public String getRegistryIdentifier() {
        return registryIdentifier;
    }

    @Override
    public String getRegistryName() {
        return registryName;
    }

    public void setRegistryName(final String registryName) {
        this.registryName = registryName;
    }

    @Override
    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(final String bucketName) {
        this.bucketName = bucketName;
    }

    @Override
    public String getFlowIdentifier() {
        return flowIdentifier;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @Override
    public String getFlowName() {
        return flowName;
    }

    public void setFlowDescription(String flowDescription) {
        this.flowDescription = flowDescription;
    }

    @Override
    public String getFlowDescription() {
        return flowDescription;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public VersionedProcessGroup getFlowSnapshot() {
        return flowSnapshot;
    }

    public void setFlowSnapshot(final VersionedProcessGroup flowSnapshot) {
        this.flowSnapshot = flowSnapshot;
    }

    @Override
    public VersionedFlowStatus getStatus() {
        return status;
    }
}
