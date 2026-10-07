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

package org.apache.nifi.components.connector;

import java.util.Collection;

/**
 * Reports local progress through a pending Connector flow evolution.
 */
public enum FlowEvolutionStatus {
    INACTIVE,
    PHASE_ONE_PENDING,
    PHASE_ONE_COMPLETE,
    PHASE_TWO_COMPLETE,
    FAILED;

    /**
     * Merges statuses from every cluster participant. The lowest ordinal wins so the cluster advances only when
     * every node has reached a phase. {@link #FAILED} wins over every other status.
     *
     * @param statuses the statuses to merge; a null or empty collection merges to {@link #INACTIVE}
     * @return the merged status
     */
    public static FlowEvolutionStatus merge(final Collection<FlowEvolutionStatus> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            return INACTIVE;
        }

        FlowEvolutionStatus mergedStatus = PHASE_TWO_COMPLETE;
        for (final FlowEvolutionStatus status : statuses) {
            final FlowEvolutionStatus effective = status == null ? INACTIVE : status;
            if (effective == FAILED) {
                return FAILED;
            }

            if (effective.ordinal() < mergedStatus.ordinal()) {
                mergedStatus = effective;
            }
        }

        return mergedStatus;
    }
}
