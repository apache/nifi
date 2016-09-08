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

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.StandardFlowSynchronizer;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An implementation of {@link FlowElection} that waits until either a maximum amount of time has elapsed
 * or a maximum number of Data Flows have entered the running to be elected, and then elects the 'winner'
 * based on the number of 'votes' that a particular DataFlow has received. This implementation considers
 * two Flows with the same fingerprint to be the same Flow. If there is a tie in the number of votes for
 * a particular DataFlow, one will be chosen in a non-deterministic manner. If multiple DataFlows are
 * presented with the same fingerprint but different Flows (for instance, the position of a component has
 * changed), one of the Flows with that fingerprint will be chosen in a non-deterministic manner.
 * </p>
 */
public class PopularVoteFlowElection implements FlowElection {
    private static final Logger logger = LoggerFactory.getLogger(PopularVoteFlowElection.class);

    private final long maxWaitNanos;
    private final Integer maxNodes;
    private final FingerprintFactory fingerprintFactory;

    private volatile Long startNanos = null;
    private volatile DataFlow electedDataFlow = null;

    private final Map<String, FlowCandidate> candidateByFingerprint = new HashMap<>();

    public PopularVoteFlowElection(final long maxWait, final TimeUnit maxWaitPeriod, final Integer maxNodes, final FingerprintFactory fingerprintFactory) {
        this.maxWaitNanos = maxWaitPeriod.toNanos(maxWait);
        if (maxWaitNanos < 1) {
            throw new IllegalArgumentException("Maximum wait time to elect Cluster Flow cannot be less than 1 nanosecond");
        }

        this.maxNodes = maxNodes;
        if (maxNodes != null && maxNodes < 1) {
            throw new IllegalArgumentException("Maximum number of nodes to wait on before electing Cluster Flow cannot be less than 1");
        }

        this.fingerprintFactory = requireNonNull(fingerprintFactory);
    }

    @Override
    public synchronized boolean isElectionComplete() {
        if (electedDataFlow != null) {
            return true;
        }

        if (startNanos == null) {
            return false;
        }

        final long nanosSinceStart = System.nanoTime() - startNanos;
        if (nanosSinceStart > maxWaitNanos) {
            final FlowCandidate elected = performElection();
            logger.info("Election is complete because the maximum allowed time has elapsed. "
                + "The elected dataflow is held by the following nodes: {}", elected.getNodes());

            return true;
        } else if (maxNodes != null) {
            final int numVotes = getVoteCount();
            if (numVotes >= maxNodes) {
                final FlowCandidate elected = performElection();
                logger.info("Election is complete because the required number of nodes ({}) have voted. "
                    + "The elected dataflow is held by the following nodes: {}", maxNodes, elected.getNodes());

                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isVoteCounted(final NodeIdentifier nodeIdentifier) {
        return candidateByFingerprint.values().stream()
            .anyMatch(candidate -> candidate.getNodes().contains(nodeIdentifier));
    }

    private synchronized int getVoteCount() {
        return candidateByFingerprint.values().stream().mapToInt(candidate -> candidate.getVotes()).sum();
    }

    @Override
    public synchronized DataFlow castVote(final DataFlow candidate, final NodeIdentifier nodeId) {
        if (candidate == null || isElectionComplete()) {
            return getElectedDataFlow();
        }

        final String fingerprint = fingerprint(candidate);
        final FlowCandidate flowCandidate = candidateByFingerprint.computeIfAbsent(fingerprint, key -> new FlowCandidate(candidate));
        final boolean voteCast = flowCandidate.vote(nodeId);

        if (startNanos == null) {
            startNanos = System.nanoTime();
        }

        if (voteCast) {
            logger.info("Vote cast by {}; this flow now has {} votes", nodeId, flowCandidate.getVotes());
        }

        if (isElectionComplete()) {
            return getElectedDataFlow();
        }

        return null; // no elected candidate so return null
    }

    private String fingerprint(final DataFlow dataFlow) {
        final String flowFingerprint = fingerprintFactory.createFingerprint(dataFlow.getFlow());
        final String authFingerprint = dataFlow.getAuthorizerFingerprint() == null ? "" : new String(dataFlow.getAuthorizerFingerprint(), StandardCharsets.UTF_8);
        final String candidateFingerprint = flowFingerprint + authFingerprint;

        return candidateFingerprint;
    }

    @Override
    public DataFlow getElectedDataFlow() {
        return electedDataFlow;
    }

    private FlowCandidate performElection() {
        if (candidateByFingerprint.isEmpty()) {
            return null;
        }

        final List<FlowCandidate> nonEmptyCandidates = candidateByFingerprint.values().stream()
            .filter(candidate -> !candidate.isFlowEmpty())
            .collect(Collectors.toList());

        if (nonEmptyCandidates.isEmpty()) {
            // All flow candidates are empty flows. Just use one of them.
            final FlowCandidate electedCandidate = candidateByFingerprint.values().iterator().next();
            this.electedDataFlow = electedCandidate.getDataFlow();
            return electedCandidate;
        }

        final FlowCandidate elected;
        if (nonEmptyCandidates.size() == 1) {
            // Only one flow is non-empty. Use that one.
            elected = nonEmptyCandidates.iterator().next();
        } else {
            // Choose the non-empty flow that got the most votes.
            elected = nonEmptyCandidates.stream()
                .max((candidate1, candidate2) -> Integer.compare(candidate1.getVotes(), candidate2.getVotes()))
                .get();
        }

        this.electedDataFlow = elected.getDataFlow();
        return elected;
    }

    @Override
    public synchronized String getStatusDescription() {
        if (startNanos == null) {
            return "No votes have yet been cast.";
        }

        final StringBuilder descriptionBuilder = new StringBuilder("Election will complete in ");
        final long nanosElapsed = System.nanoTime() - startNanos;
        final long nanosLeft = maxWaitNanos - nanosElapsed;
        final long secsLeft = TimeUnit.NANOSECONDS.toSeconds(nanosLeft);
        if (secsLeft < 1) {
            descriptionBuilder.append("less than 1 second");
        } else {
            descriptionBuilder.append(secsLeft).append(" seconds");
        }

        if (maxNodes != null) {
            final int votesNeeded = maxNodes.intValue() - getVoteCount();
            descriptionBuilder.append(" or after ").append(votesNeeded).append(" more vote");
            descriptionBuilder.append(votesNeeded == 1 ? " is " : "s are ");
            descriptionBuilder.append("cast, whichever occurs first.");
        }

        return descriptionBuilder.toString();
    }

    private static class FlowCandidate {
        private final DataFlow dataFlow;
        private final AtomicInteger voteCount = new AtomicInteger(0);
        private final Set<NodeIdentifier> nodeIds = Collections.synchronizedSet(new HashSet<>());

        public FlowCandidate(final DataFlow dataFlow) {
            this.dataFlow = dataFlow;
        }

        /**
         * Casts a vote for this candidate for the given node identifier, if a vote has not already
         * been cast for this node identifier
         *
         * @param nodeId the node id that is casting the vote
         * @return <code>true</code> if the vote was case, <code>false</code> if this node id has already cast its vote
         */
        public boolean vote(final NodeIdentifier nodeId) {
            if (nodeIds.add(nodeId)) {
                voteCount.incrementAndGet();
                return true;
            }

            return false;
        }

        public int getVotes() {
            return voteCount.get();
        }

        public DataFlow getDataFlow() {
            return dataFlow;
        }

        public boolean isFlowEmpty() {
            return StandardFlowSynchronizer.isEmpty(dataFlow);
        }

        public Set<NodeIdentifier> getNodes() {
            return nodeIds;
        }
    }
}
