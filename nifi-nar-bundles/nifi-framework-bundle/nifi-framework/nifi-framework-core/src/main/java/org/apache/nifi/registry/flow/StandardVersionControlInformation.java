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

import java.util.Optional;

public class StandardVersionControlInformation implements VersionControlInformation {

    private final String registryIdentifier;
    private final String bucketIdentifier;
    private final String flowIdentifier;
    private final int version;
    private volatile VersionedProcessGroup flowSnapshot;
    private volatile Boolean modified = null;
    private volatile Boolean current = null;

    public StandardVersionControlInformation(final String registryId, final String bucketId, final String flowId, final int version,
        final VersionedProcessGroup snapshot, final Boolean modified, final Boolean current) {
        this.registryIdentifier = registryId;
        this.bucketIdentifier = bucketId;
        this.flowIdentifier = flowId;
        this.version = version;
        this.flowSnapshot = snapshot;
        this.modified = modified;
        this.current = current;
    }

    @Override
    public String getRegistryIdentifier() {
        return registryIdentifier;
    }

    @Override
    public String getBucketIdentifier() {
        return bucketIdentifier;
    }

    @Override
    public String getFlowIdentifier() {
        return flowIdentifier;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public Optional<Boolean> getModified() {
        return Optional.ofNullable(modified);
    }

    @Override
    public Optional<Boolean> getCurrent() {
        return Optional.ofNullable(current);
    }

    @Override
    public VersionedProcessGroup getFlowSnapshot() {
        return flowSnapshot;
    }

    public void setModified(final boolean modified) {
        this.modified = modified;
    }

    public void setCurrent(final boolean current) {
        this.current = current;
    }

    public void setFlowSnapshot(final VersionedProcessGroup flowSnapshot) {
        this.flowSnapshot = flowSnapshot;
    }
}
