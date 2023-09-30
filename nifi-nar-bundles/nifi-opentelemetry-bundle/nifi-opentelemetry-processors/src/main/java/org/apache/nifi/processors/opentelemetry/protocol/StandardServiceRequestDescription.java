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
package org.apache.nifi.processors.opentelemetry.protocol;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Standard implementation of Service Request Description
 */
public class StandardServiceRequestDescription implements ServiceRequestDescription {
    private final TelemetryContentEncoding contentEncoding;

    private final TelemetryContentType contentType;

    private final TelemetryRequestType requestType;

    private final InetSocketAddress remoteAddress;

    public StandardServiceRequestDescription(
            final TelemetryContentEncoding contentEncoding,
            final TelemetryContentType contentType,
            final TelemetryRequestType requestType,
            final InetSocketAddress remoteAddress
    ) {
        this.contentEncoding = Objects.requireNonNull(contentEncoding, "Content Encoding required");
        this.contentType = Objects.requireNonNull(contentType, "Content Type required");
        this.requestType = Objects.requireNonNull(requestType, "Request Type required");
        this.remoteAddress = Objects.requireNonNull(remoteAddress, "Remote Address required");
    }

    @Override
    public TelemetryContentEncoding getContentEncoding() {
        return contentEncoding;
    }

    @Override
    public TelemetryContentType getContentType() {
        return contentType;
    }

    @Override
    public TelemetryRequestType getRequestType() {
        return requestType;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }
}
