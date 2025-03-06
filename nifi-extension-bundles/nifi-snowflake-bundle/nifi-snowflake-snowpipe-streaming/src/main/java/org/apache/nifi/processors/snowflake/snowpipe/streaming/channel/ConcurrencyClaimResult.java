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

public enum ConcurrencyClaimResult {
    /**
     * The Concurrency Claim was rejected by the Concurrency Manager.
     * No allocation has been made for the Concurrency Claim.
     */
    REJECTED,

    /**
     * The Concurrency Claim has been accepted by the Concurrency Manager, and the associated channel
     * is now responsible for releasing the Concurrency Claim when it is no longer needed.
     */
    OWNED,

    /**
     * The Concurrency Claim has been accepted by the Concurrency Manager because the Concurrency Group has
     * already been acquired by the Claim. The associated channel should not release the Concurrency Claim.
     */
    INHERITED;
}
