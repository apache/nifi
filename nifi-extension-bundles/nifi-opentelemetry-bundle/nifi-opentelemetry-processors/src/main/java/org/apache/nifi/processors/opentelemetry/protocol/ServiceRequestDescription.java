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

/**
 * Service Request Description from Headers
 */
public interface ServiceRequestDescription {
    /**
     * Get Request Content-Encoding
     *
     * @return Request Content-Encoding indicating compression
     */
    TelemetryContentEncoding getContentEncoding();

    /**
     * Get Request Content-Type
     *
     * @return Request Content-Type
     */
    TelemetryContentType getContentType();

    /**
     * Get Export Service Request Type
     *
     * @return Export Service Request Type
     */
    TelemetryRequestType getRequestType();

    /**
     * Get Remote Address
     *
     * @return Remote Address
     */
    InetSocketAddress getRemoteAddress();
}
