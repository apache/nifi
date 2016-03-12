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

package org.apache.nifi.controller.swap;

import java.util.Collections;
import java.util.List;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaim;

public class StandardSwapSummary implements SwapSummary {
    public static final SwapSummary EMPTY_SUMMARY = new StandardSwapSummary(new QueueSize(0, 0L), null, Collections.<ResourceClaim> emptyList());

    private final QueueSize queueSize;
    private final Long maxFlowFileId;
    private final List<ResourceClaim> resourceClaims;

    public StandardSwapSummary(final QueueSize queueSize, final Long maxFlowFileId, final List<ResourceClaim> resourceClaims) {
        this.queueSize = queueSize;
        this.maxFlowFileId = maxFlowFileId;
        this.resourceClaims = Collections.unmodifiableList(resourceClaims);
    }

    @Override
    public QueueSize getQueueSize() {
        return queueSize;
    }

    @Override
    public Long getMaxFlowFileId() {
        return maxFlowFileId;
    }

    @Override
    public List<ResourceClaim> getResourceClaims() {
        return resourceClaims;
    }
}
