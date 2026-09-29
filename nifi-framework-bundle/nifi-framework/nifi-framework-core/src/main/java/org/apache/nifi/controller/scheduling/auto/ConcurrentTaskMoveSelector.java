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
package org.apache.nifi.controller.scheduling.auto;

import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingSnapshot.RecentDemand;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * While the global concurrent task limit is fully used, chooses when one concurrent task should move from a Processor with little
 * queued work to a Processor with much more. A Processor's demand score is its queued FlowFiles divided by its recent rate of committed
 * input FlowFiles: roughly how many seconds its backlog would take to clear. A move is proposed only after the same pair has shown a
 * clear difference in score for several evaluations in a row. Not thread-safe.
 */
public class ConcurrentTaskMoveSelector {
    static final long MIN_MEASUREMENT_NANOS = TimeUnit.SECONDS.toNanos(6);
    static final double MAX_SCORE = 60D;
    static final double MIN_RECEIVER_SCORE = 10D;
    static final double SOURCE_SCORE = MIN_RECEIVER_SCORE;
    static final int SUSTAINED_EVALUATIONS = 3;
    private static final double MIN_SCORE_RATIO = 2D;
    private static final double MIN_SCORE_DIFFERENCE = 5D;
    private static final double AVERAGING_SECONDS = RecentDemand.AVERAGING_NANOS / (double) TimeUnit.SECONDS.toNanos(1);

    private TaskMove candidateMove;
    private int consecutiveEvaluations;

    public Optional<TaskMove> selectMove(final List<ProcessorDemand> demands, final boolean globalCapacityFull) {
        if (!globalCapacityFull) {
            candidateMove = null;
            consecutiveEvaluations = 0;
            return Optional.empty();
        }

        ProcessorDemand receiver = null;
        double receiverScore = 0D;
        for (final ProcessorDemand demand : demands) {
            final double score = calculateScore(demand);
            if (isEligibleReceiver(demand, score) && (receiver == null || score > receiverScore)) {
                receiver = demand;
                receiverScore = score;
            }
        }

        ProcessorDemand donor = null;
        double donorScore = 0D;
        for (final ProcessorDemand demand : demands) {
            if (demand == receiver || demand.source() || demand.concurrentTasks() <= 1 || demand.concurrencyComparisonActive()) {
                continue;
            }

            final double score = calculateScore(demand);
            if (donor == null || score < donorScore || score == donorScore && demand.concurrentTasks() > donor.concurrentTasks()) {
                donor = demand;
                donorScore = score;
            }
        }

        if (receiver == null || donor == null || receiverScore < donorScore * MIN_SCORE_RATIO || receiverScore < donorScore + MIN_SCORE_DIFFERENCE) {
            candidateMove = null;
            consecutiveEvaluations = 0;
            return Optional.empty();
        }

        final TaskMove move = new TaskMove(receiver.identifier(), donor.identifier());
        consecutiveEvaluations = move.equals(candidateMove) ? consecutiveEvaluations + 1 : 1;
        candidateMove = move;
        if (consecutiveEvaluations < SUSTAINED_EVALUATIONS) {
            return Optional.empty();
        }

        // The difference must be sustained again before the next move.
        candidateMove = null;
        consecutiveEvaluations = 0;
        return Optional.of(move);
    }

    public static double calculateScore(final ProcessorDemand demand) {
        final ProcessorSchedulingSnapshot snapshot = demand.snapshot();
        if (demand.source()) {
            final boolean activeAndBusy = snapshot.processorReady() && snapshot.sourceProcessorRecentlyReportedActivity()
                    && snapshot.concurrentTaskUtilization() >= StandardAutoSchedulingController.MIN_UTILIZATION_FOR_INCREASE;
            return activeAndBusy ? SOURCE_SCORE : 0D;
        }

        if (snapshot.localInputQueueCount() == 0L) {
            return 0D;
        }

        final RecentDemand recent = snapshot.recentDemand();
        final boolean committedInput = isRecent(recent.committedInputFlowFilesPerSecond());
        final boolean notProgressing = isRecent(recent.completedInvocationsPerSecond()) && !committedInput;
        if (recent.measuredNanos() >= MIN_MEASUREMENT_NANOS && (isFailing(recent) || notProgressing)) {
            // A Processor that fails or commits nothing gains nothing from its extra tasks, so it is the first to give one up.
            return 0D;
        }

        return committedInput ? Math.min(MAX_SCORE, snapshot.localInputQueueCount() / recent.committedInputFlowFilesPerSecond()) : MAX_SCORE;
    }

    private static boolean isEligibleReceiver(final ProcessorDemand demand, final double score) {
        final RecentDemand recent = demand.snapshot().recentDemand();
        return score >= MIN_RECEIVER_SCORE
                && demand.snapshot().processorReady()
                && demand.concurrentTasks() < demand.maxConcurrentTasks()
                && !isFailing(recent)
                && recent.measuredNanos() >= MIN_MEASUREMENT_NANOS
                && !demand.concurrencyComparisonActive()
                && !demand.increaseCoolingDown()
                && (demand.source() || isRecent(recent.committedInputFlowFilesPerSecond()));
    }

    private static boolean isFailing(final RecentDemand recent) {
        return isRecent(recent.failedInvocationsPerSecond())
                && recent.failedInvocationsPerSecond() >= recent.completedInvocationsPerSecond() * StandardAutoSchedulingController.MAX_FAILED_INVOCATION_FRACTION;
    }

    /**
     * @return whether a rate amounts to at least one occurrence during the averaging period
     */
    private static boolean isRecent(final double perSecond) {
        return perSecond * AVERAGING_SECONDS >= 1D;
    }

    /**
     * @param source whether the Processor has no input queue of its own
     * @param maxConcurrentTasks the most concurrent tasks the Processor may use
     * @param increaseCoolingDown whether the Processor must wait before testing another increase
     */
    public record ProcessorDemand(String identifier, boolean source, int concurrentTasks, int maxConcurrentTasks, boolean concurrencyComparisonActive,
                                  boolean increaseCoolingDown, ProcessorSchedulingSnapshot snapshot) {
    }

    public record TaskMove(String receiverIdentifier, String donorIdentifier) {
    }
}
