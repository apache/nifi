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

/**
 * Abstraction for managing claims for channel concurrency groups
 */
public interface ConcurrencyManager {
    /**
     * Attempt to claim one of the available concurrency slots for the given concurrency group.
     *
     * @param request Concurrency Claim Request
     * @return Concurrency Claim Result
     */
    ConcurrencyClaimResult tryClaim(ConcurrencyClaimRequest request);

    /**
     * Release the claim on the concurrency slot for the given concurrency group.
     *
     * @param concurrencyGroup Concurrency Group of the claim to be released
     */
    void releaseClaim(String concurrencyGroup);

    /**
     * @return true if there are no claims for any concurrency groups, false otherwise
     */
    boolean isEmpty();
}
