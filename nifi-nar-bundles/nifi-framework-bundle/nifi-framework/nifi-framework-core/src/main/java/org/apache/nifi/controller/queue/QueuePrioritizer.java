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

package org.apache.nifi.controller.queue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFilePrioritizer;

public class QueuePrioritizer implements Comparator<FlowFileRecord>, Serializable {
    private static final long serialVersionUID = 1L;
    private final transient List<FlowFilePrioritizer> prioritizers = new ArrayList<>();

    public QueuePrioritizer(final List<FlowFilePrioritizer> priorities) {
        if (null != priorities) {
            prioritizers.addAll(priorities);
        }
    }

    @Override
    public int compare(final FlowFileRecord f1, final FlowFileRecord f2) {
        int returnVal = 0;
        final boolean f1Penalized = f1.isPenalized();
        final boolean f2Penalized = f2.isPenalized();

        if (f1Penalized && !f2Penalized) {
            return 1;
        } else if (!f1Penalized && f2Penalized) {
            return -1;
        }

        if (f1Penalized && f2Penalized) {
            if (f1.getPenaltyExpirationMillis() < f2.getPenaltyExpirationMillis()) {
                return -1;
            } else if (f1.getPenaltyExpirationMillis() > f2.getPenaltyExpirationMillis()) {
                return 1;
            }
        }

        if (!prioritizers.isEmpty()) {
            for (final FlowFilePrioritizer prioritizer : prioritizers) {
                returnVal = prioritizer.compare(f1, f2);
                if (returnVal != 0) {
                    return returnVal;
                }
            }
        }

        final ContentClaim claim1 = f1.getContentClaim();
        final ContentClaim claim2 = f2.getContentClaim();

        // put the one without a claim first
        if (claim1 == null && claim2 != null) {
            return -1;
        } else if (claim1 != null && claim2 == null) {
            return 1;
        } else if (claim1 != null && claim2 != null) {
            final int claimComparison = claim1.compareTo(claim2);
            if (claimComparison != 0) {
                return claimComparison;
            }

            final int claimOffsetComparison = Long.compare(f1.getContentClaimOffset(), f2.getContentClaimOffset());
            if (claimOffsetComparison != 0) {
                return claimOffsetComparison;
            }
        }

        return Long.compare(f1.getId(), f2.getId());
    }
}
