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

import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveSelector.ProcessorDemand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Moves one concurrent task at a time from a donor to a receiver chosen by a {@link ConcurrentTaskMoveSelector}. The donor gives up its task
 * before the receiver may test one more, so a move never adds concurrent tasks. The receiver keeps the task only when its test ends with
 * higher throughput. Any other ending, including a reset during the test, rolls the receiver back and then gives the donor its task back.
 * Must be updated from a single thread, but its read methods may be called from any thread.
 */
public class ConcurrentTaskMoveCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentTaskMoveCoordinator.class);

    private final ConcurrentTaskMoveSelector selector = new ConcurrentTaskMoveSelector();
    private volatile Move move;

    /**
     * Ends a move whose receiver stopped or was reset mid-test, or selects a new move while global capacity is full.
     *
     * @param participants the participants for each of the demands, by identifier
     */
    public void balance(final List<ProcessorDemand> demands, final Map<String, ? extends Participant> participants, final boolean globalCapacityFull) {
        final Move current = move;
        if (current != null) {
            if (!current.receiver.isActive() || current.testedConcurrentTasks > 0 && !current.receiver.isConcurrencyComparisonActive()) {
                finish(current, false);
            }

            return;
        }

        selector.selectMove(demands, globalCapacityFull).ifPresent(selected -> {
            final Participant receiver = participants.get(selected.receiverIdentifier());
            final Participant donor = participants.get(selected.donorIdentifier());
            if (donor.changeConcurrentTasks(-1)) {
                move = new Move(receiver, donor);
                logger.info("Moving one concurrent task from {} to {} because its queued work is much larger", donor, receiver);
            }
        });
    }

    /**
     * @return whether the participant may test one more concurrent task even though global capacity is full
     */
    public boolean isTestAllowed(final Participant participant) {
        final Move current = move;
        return current != null && current.receiver == participant && current.testedConcurrentTasks == 0;
    }

    /**
     * Records the setting the receiver tests with the moved task, and ends the move once the receiver is not testing it.
     */
    public void recordDecision(final Participant participant, final ProcessorSchedulingDecisionReason reason) {
        final Move current = move;
        if (current == null || current.receiver != participant) {
            return;
        }

        if (current.testedConcurrentTasks == 0 && reason == ProcessorSchedulingDecisionReason.TESTING_HIGHER_CONCURRENCY) {
            current.testedConcurrentTasks = participant.getConcurrentTasks();
        } else if (!participant.isConcurrencyComparisonActive()) {
            finish(current, current.testedConcurrentTasks > 0 && reason == ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_INCREASED_THROUGHPUT);
        }
    }

    /**
     * @return {@code RECEIVER} or {@code DONOR} while the participant takes part in a move, otherwise {@code null}
     */
    public String getRole(final Participant participant) {
        final Move current = move;
        if (current != null && current.receiver == participant) {
            return "RECEIVER";
        }

        return current != null && current.donor == participant ? "DONOR" : null;
    }

    private void finish(final Move current, final boolean throughputIncreased) {
        move = null;
        if (throughputIncreased) {
            return;
        }

        // A test that ended without higher throughput has already lowered the receiver, but a reset leaves it on the tested setting.
        // Lowering the receiver before raising the donor keeps the total from ever exceeding what it was before the move.
        if (current.testedConcurrentTasks > 0 && current.receiver.isActive() && current.receiver.getConcurrentTasks() >= current.testedConcurrentTasks) {
            current.receiver.changeConcurrentTasks(-1);
        }

        current.donor.changeConcurrentTasks(1);
    }

    /**
     * A Processor that can give or receive a concurrent task.
     */
    public interface Participant {
        /**
         * @return whether the Processor is still scheduled in the same run
         */
        boolean isActive();

        int getConcurrentTasks();

        boolean isConcurrencyComparisonActive();

        /**
         * Changes the concurrent task setting by the given amount if the Processor is active and the result is within its limits.
         *
         * @return whether the setting was changed
         */
        boolean changeConcurrentTasks(int change);
    }

    private static class Move {
        private final Participant receiver;
        private final Participant donor;
        private volatile int testedConcurrentTasks;

        private Move(final Participant receiver, final Participant donor) {
            this.receiver = receiver;
            this.donor = donor;
        }
    }
}
