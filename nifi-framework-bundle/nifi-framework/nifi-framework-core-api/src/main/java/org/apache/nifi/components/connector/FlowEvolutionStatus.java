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
 * Reports local progress through the evolve-drain phase of a pending Connector flow evolution. Applying the upgraded flow
 * after the evolve-drain phase is tracked through {@link ConnectorState}, as for a configuration update.
 *
 * <p>
 * A node reports {@link #INACTIVE} until an upgrade is pending, and {@link #EVOLVE_DRAIN_PENDING} from then on, even before
 * the first START. When the local drain finishes, the status becomes {@link #EVOLVE_DRAIN_COMPLETE} and stays there while the
 * upgraded flow is applied. Once the upgraded flow is applied and its snapshot captured, the status returns to {@link #INACTIVE}.
 * There is no separate completed value, because {@link #INACTIVE} means that nothing is pending.
 * </p>
 */
public enum FlowEvolutionStatus {
    /**
     * No evolution is pending, so the node no longer needs the outgoing flow.
     */
    INACTIVE,

    /**
     * The node still has the outgoing flow and has not finished draining it.
     */
    EVOLVE_DRAIN_PENDING,

    /**
     * The node has drained the outgoing flow and has not yet applied the upgraded flow.
     */
    EVOLVE_DRAIN_COMPLETE,

    /**
     * The current evolution attempt failed or was cancelled on this node. It is reported instead of the drain status until
     * the next START prepares a new attempt, which then reports the drain status again.
     */
    FAILED;

    /**
     * Merges statuses from every cluster participant. {@link #INACTIVE} nodes have nothing to drain and are ignored.
     * Of the rest, the lowest ordinal wins so the cluster passes the drain barrier only when every node that still has
     * the outgoing flow has drained it. {@link #FAILED} wins over every other status.
     *
     * @param statuses the statuses to merge; a null or empty collection merges to {@link #INACTIVE}
     * @return the merged status, or {@link #INACTIVE} when every participant is inactive
     */
    public static FlowEvolutionStatus merge(final Collection<FlowEvolutionStatus> statuses) {
        if (statuses == null || statuses.isEmpty()) {
            return INACTIVE;
        }

        FlowEvolutionStatus mergedStatus = INACTIVE;
        for (final FlowEvolutionStatus status : statuses) {
            if (status == FAILED) {
                return FAILED;
            }

            if (status == null || status == INACTIVE) {
                continue;
            }

            if (mergedStatus == INACTIVE || status.ordinal() < mergedStatus.ordinal()) {
                mergedStatus = status;
            }
        }

        return mergedStatus;
    }
}
