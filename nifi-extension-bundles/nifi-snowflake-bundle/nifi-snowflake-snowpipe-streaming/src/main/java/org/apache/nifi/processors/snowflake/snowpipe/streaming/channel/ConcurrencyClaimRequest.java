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
package org.apache.nifi.processors.snowflake.snowpipe.streaming.channel;

import java.util.Objects;

public class ConcurrencyClaimRequest {
    private final String groupRequested;
    private final ConcurrencyClaim existingClaim;
    private boolean processed = false;

    public ConcurrencyClaimRequest(final String groupRequested, final ConcurrencyClaim existingClaim) {
        this.groupRequested = Objects.requireNonNull(groupRequested);
        this.existingClaim = Objects.requireNonNull(existingClaim);
    }

    public String getGroupRequested() {
        return groupRequested;
    }

    public ConcurrencyClaim getExistingClaim() {
        return existingClaim;
    }

    void accept() {
        if (processed) {
            throw new IllegalStateException("ConcurrencyClaimRequest has already been accepted or rejected");
        }

        processed = true;
        existingClaim.addAcquiredConcurrencyGroup(groupRequested);
    }

    void reject() {
        if (processed) {
            throw new IllegalStateException("ConcurrencyClaimRequest has already been accepted or rejected");
        }

        processed = true;
    }
}
