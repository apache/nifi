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
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.repository.claim.ContentClaim;

public class ReconstitutedSerializedRepositoryRecord implements SerializedRepositoryRecord {
    private final String queueIdentifier;
    private final RepositoryRecordType type;
    private final FlowFileRecord flowFile;
    private final String swapLocation;

    private ReconstitutedSerializedRepositoryRecord(final Builder builder) {
        this.queueIdentifier = builder.queueIdentifier;
        this.type = builder.type;
        this.flowFile = builder.flowFile;
        this.swapLocation = builder.swapLocation;
    }

    @Override
    public String getQueueIdentifier() {
        return queueIdentifier;
    }

    @Override
    public RepositoryRecordType getType() {
        return type;
    }

    @Override
    public ContentClaim getContentClaim() {
        return flowFile.getContentClaim();
    }

    @Override
    public long getClaimOffset() {
        return flowFile.getContentClaimOffset();
    }

    @Override
    public FlowFileRecord getFlowFileRecord() {
        return flowFile;
    }

    @Override
    public boolean isMarkedForAbort() {
        return false;
    }

    @Override
    public boolean isAttributesChanged() {
        return false;
    }

    @Override
    public String getSwapLocation() {
        return swapLocation;
    }

    public static class Builder {
        private String queueIdentifier;
        private RepositoryRecordType type;
        private FlowFileRecord flowFile;
        private String swapLocation;

        public Builder queueIdentifier(final String queueIdentifier) {
            this.queueIdentifier = queueIdentifier;
            return this;
        }

        public Builder type(final RepositoryRecordType type) {
            this.type = type;
            return this;
        }

        public Builder flowFileRecord(final FlowFileRecord flowFileRecord) {
            this.flowFile = flowFileRecord;
            return this;
        }

        public Builder swapLocation(final String swapLocation) {
            this.swapLocation = swapLocation;
            return this;
        }

        public ReconstitutedSerializedRepositoryRecord build() {
            return new ReconstitutedSerializedRepositoryRecord(this);
        }
    }
}
