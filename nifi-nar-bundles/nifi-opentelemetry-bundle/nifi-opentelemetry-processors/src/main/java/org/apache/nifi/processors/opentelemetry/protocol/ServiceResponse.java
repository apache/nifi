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

import java.util.Objects;

/**
 * Service Processing Response with status and number of rejected elements
 */
public class ServiceResponse {
    private final ServiceResponseStatus serviceResponseStatus;

    private final int rejected;

    public ServiceResponse(final ServiceResponseStatus serviceResponseStatus, final int rejected) {
        this.serviceResponseStatus = Objects.requireNonNull(serviceResponseStatus, "Status required");
        this.rejected = rejected;
    }

    /**
     * Get Service Response Status including HTTP Status
     *
     * @return Service Response Status
     */
    public ServiceResponseStatus getServiceResponseStatus() {
        return serviceResponseStatus;
    }

    /**
     * Get rejected resource elements
     *
     * @return Rejected resource elements
     */
    public int getRejected() {
        return rejected;
    }
}
