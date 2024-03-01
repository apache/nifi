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

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flowfile.FlowFilePrioritizer;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class QueuePrioritizer implements Comparator<FlowFileRecord>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Comparator<FlowFileRecord> penaltyComparator = Comparator
            .comparing(FlowFileRecord::isPenalized)
            .thenComparingLong(record -> record.isPenalized() ? record.getPenaltyExpirationMillis() : 0);
    private static final Comparator<FlowFileRecord> claimComparator = Comparator
            .comparing(FlowFileRecord::getContentClaim, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparingLong(FlowFileRecord::getContentClaimOffset);
    private static final Comparator<FlowFileRecord> idComparator = Comparator.comparingLong(FlowFileRecord::getId);

    private final transient List<FlowFilePrioritizer> prioritizers;

    public QueuePrioritizer(final List<FlowFilePrioritizer> priorities) {
        prioritizers = priorities == null ? List.of() : List.copyOf(priorities);
    }

    @Override
    public int compare(final FlowFileRecord f1, final FlowFileRecord f2) {
        final int penaltyComparisonResult = penaltyComparator.compare(f1, f2);
        if (penaltyComparisonResult != 0) {
            return penaltyComparisonResult;
        }

        for (FlowFilePrioritizer comparator : prioritizers) {
            final int prioritizerComparisonResult = comparator.compare(f1, f2);
            if (prioritizerComparisonResult != 0) {
                return prioritizerComparisonResult;
            }
        }

        final int claimComparisionResult = claimComparator.compare(f1, f2);
        if (claimComparisionResult != 0) {
            return claimComparisionResult;
        }

        return idComparator.compare(f1, f2);
    }
}