/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.registry.flow;

import java.util.Objects;

/**
 * Information for locating a flow version in a flow registry.
 */
public class FlowVersionLocation extends FlowLocation {

    private String version;

    public FlowVersionLocation() {

    }

    public FlowVersionLocation(final String branch, final String bucketId, final String flowId, final String version) {
        super(branch, bucketId, flowId);
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FlowVersionLocation that = (FlowVersionLocation) o;
        return Objects.equals(getBranch(), that.getBranch())
                && Objects.equals(getBucketId(), that.getBucketId())
                && Objects.equals(getFlowId(), that.getFlowId())
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBranch(), getBucketId(), getFlowId(), version);
    }
}
