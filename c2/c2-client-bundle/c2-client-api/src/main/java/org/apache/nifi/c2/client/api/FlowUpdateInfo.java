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
package org.apache.nifi.c2.client.api;

import java.net.URI;
import java.util.Objects;

public class FlowUpdateInfo {
    private final String flowUpdateUrl;
    private final String requestId;

    public FlowUpdateInfo(final String flowUpdateUrl, final String requestId) {
        this.flowUpdateUrl = flowUpdateUrl;
        this.requestId = requestId;
    }

    public String getFlowUpdateUrl() {
        return flowUpdateUrl;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getFlowId() {
        try {
            final URI flowUri = new URI(flowUpdateUrl);
            final String flowUriPath = flowUri.getPath();
            final String[] split = flowUriPath.split("/");
            final String flowId = split[4];
            return flowId;
        } catch (Exception e) {
            throw new IllegalStateException("Could not get flow id from the provided URL");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowUpdateInfo that = (FlowUpdateInfo) o;
        return Objects.equals(flowUpdateUrl, that.flowUpdateUrl)
            && Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowUpdateUrl, requestId);
    }
}