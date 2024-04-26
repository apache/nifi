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

package org.apache.nifi.cluster.coordination.flow;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * <p>
 * A FlowElection is responsible for examining multiple versions of a dataflow and determining which of
 * the versions is the "correct" version of the flow.
 * </p>
 */
public interface FlowElection {

    /**
     * Checks if the election has completed or not.
     *
     * @return <code>true</code> if the election has completed, <code>false</code> otherwise.
     */
    boolean isElectionComplete();

    /**
     * Returns <code>true</code> if a vote has already been counted for the given Node Identifier, <code>false</code> otherwise.
     *
     * @param nodeIdentifier the identifier of the node
     * @return <code>true</code> if a vote has already been counted for the given Node Identifier, <code>false</code> otherwise.
     */
    boolean isVoteCounted(NodeIdentifier nodeIdentifier);

    /**
     * If the election has not yet completed, adds the given DataFlow to the list of candidates
     * (if it is not already in the running) and increments the number of votes for this DataFlow by 1.
     * If the election has completed, the given candidate is ignored, and the already-elected DataFlow
     * will be returned. If the election has not yet completed, a vote will be cast for the given
     * candidate and <code>null</code> will be returned, signifying that no candidate has yet been chosen.
     *
     * @param candidate the DataFlow to vote for and add to the pool of candidates if not already present
     * @param nodeIdentifier the identifier of the node casting the vote
     *
     * @return the elected {@link DataFlow}, or <code>null</code> if no DataFlow has yet been elected
     */
    DataFlow castVote(DataFlow candidate, NodeIdentifier nodeIdentifier);

    /**
     * Returns the DataFlow that has been elected as the "correct" version of the flow, or <code>null</code>
     * if the election has not yet completed.
     *
     * @return the DataFlow that has been elected as the "correct" version of the flow, or <code>null</code>
     *         if the election has not yet completed.
     */
    DataFlow getElectedDataFlow();

    /**
     * Returns a human-readable description of the status of the election
     *
     * @return a human-readable description of the status of the election
     */
    String getStatusDescription();
}
