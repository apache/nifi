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

package org.apache.nifi.cluster.coordination.http.replication;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

public class ResponseUtils {

    /**
     * Finds the Node Identifier for all nodes that had a 'slow' response time. A 'slow' response time
     * is defined as being more than X standard deviations from the mean response time, where X is the
     * given Standard Deviation Multiple
     *
     * @param response the AsyncClusterResponse to base the calculations off of
     * @param stdDeviationMultiple the number of standard deviations that a response time must be away from the mean in order
     *            to be considered 'slow'
     *
     * @return a Set of all Node Identifiers that took a long time to respond
     */
    public static Set<NodeIdentifier> findLongResponseTimes(final AsyncClusterResponse response, final double stdDeviationMultiple) {
        final Set<NodeIdentifier> slowResponses = new HashSet<>();

        if (response.isOlderThan(1, TimeUnit.SECONDS)) {
            // If the response is older than 2 seconds, determines if any node took a long time to respond.
            final Set<NodeIdentifier> completedIds = response.getCompletedNodeIdentifiers();

            if (completedIds.size() < 2) {
                return slowResponses;
            }

            long requestMillisSum = 0L;
            int numNodes = 0;
            for (final NodeIdentifier nodeId : completedIds) {
                final long requestMillis = response.getNodeResponse(nodeId).getRequestDuration(TimeUnit.NANOSECONDS);
                if (requestMillis < 0) {
                    continue;
                }

                requestMillisSum += requestMillis;
                numNodes++;
            }

            if (numNodes < 2) {
                return slowResponses;
            }

            final double mean = requestMillisSum / numNodes;
            double differenceSquaredSum = 0D;
            for (final NodeIdentifier nodeId : completedIds) {
                final long requestMillis = response.getNodeResponse(nodeId).getRequestDuration(TimeUnit.NANOSECONDS);
                final double differenceSquared = Math.pow(mean - requestMillis, 2);
                differenceSquaredSum += differenceSquared;
            }

            final double meanOfDifferenceSquared = differenceSquaredSum / numNodes;
            final double stdDev = Math.pow(meanOfDifferenceSquared, 0.5D);
            final double longTimeThreshold = mean + stdDev * stdDeviationMultiple;

            for (final NodeIdentifier nodeId : completedIds) {
                final long requestMillis = response.getNodeResponse(nodeId).getRequestDuration(TimeUnit.NANOSECONDS);
                if (requestMillis > longTimeThreshold) {
                    slowResponses.add(nodeId);
                }
            }
        }

        return slowResponses;
    }
}
