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

import org.apache.nifi.controller.scheduling.SchedulingSettings;
import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveSelector.ProcessorDemand;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingSnapshot.RecentDemand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentTaskMoveCoordinatorTest {
    private final ConcurrentTaskMoveCoordinator coordinator = new ConcurrentTaskMoveCoordinator();
    private final TestParticipant receiver = new TestParticipant(1);
    private final TestParticipant donor = new TestParticipant(6);
    private final Map<String, TestParticipant> participants = Map.of("receiver", receiver, "donor", donor);
    private int maxTotalConcurrentTasks;

    @ParameterizedTest
    @EnumSource(MoveEnding.class)
    void testMoveNeverAddsConcurrentTasks(final MoveEnding moveEnding) {
        selectMove();
        // The donor has already given up its task when the receiver is first allowed to test one more.
        assertEquals(5, donor.concurrentTasks);
        assertTrue(coordinator.isTestAllowed(receiver));
        assertFalse(coordinator.isTestAllowed(donor));
        assertEquals("RECEIVER", coordinator.getRole(receiver));
        assertEquals("DONOR", coordinator.getRole(donor));

        if (moveEnding == MoveEnding.RECEIVER_DECLINES) {
            coordinator.recordDecision(receiver, ProcessorSchedulingDecisionReason.GLOBAL_CAPACITY_FULL);
        } else {
            startReceiverTest();
            assertFalse(coordinator.isTestAllowed(receiver));
            // A further selection is ignored while the move is in progress.
            balance();
            assertEquals(5, donor.concurrentTasks);

            switch (moveEnding) {
                case THROUGHPUT_INCREASED -> endReceiverTest(2, ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_INCREASED_THROUGHPUT);
                case THROUGHPUT_NOT_INCREASED -> endReceiverTest(1, ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_DID_NOT_INCREASE_THROUGHPUT);
                // A reset ends the test without undoing the receiver's extra task, and either the next balance or the receiver's next
                // evaluation may be the first to notice.
                case RESET_NOTICED_BY_BALANCE -> {
                    receiver.comparisonActive = false;
                    balance();
                }
                case RESET_NOTICED_BY_EVALUATION -> endReceiverTest(2, ProcessorSchedulingDecisionReason.PERFORMANCE_MEASUREMENTS_RESET);
                case RECEIVER_STOPPED -> {
                    receiver.active = false;
                    balance();
                }
                default -> throw new IllegalStateException(moveEnding.name());
            }
        }

        // Only a test that proved higher throughput lets the receiver keep the task.
        final boolean receiverKeepsTask = moveEnding == MoveEnding.THROUGHPUT_INCREASED;
        if (moveEnding != MoveEnding.RECEIVER_STOPPED) {
            assertEquals(receiverKeepsTask ? 2 : 1, receiver.concurrentTasks);
        }

        assertEquals(receiverKeepsTask ? 5 : 6, donor.concurrentTasks);
        assertTrue(maxTotalConcurrentTasks <= 7, "Total concurrent tasks reached " + maxTotalConcurrentTasks);
        assertNull(coordinator.getRole(receiver));
        assertNull(coordinator.getRole(donor));
        assertFalse(coordinator.isTestAllowed(receiver));
    }

    @Test
    void testNoMoveWhenDonorCannotGiveUpTask() {
        donor.active = false;
        selectMove();
        assertEquals(6, donor.concurrentTasks);
        assertFalse(coordinator.isTestAllowed(receiver));
        assertNull(coordinator.getRole(receiver));

        // A donor that stops during a move does not get the task back.
        donor.active = true;
        selectMove();
        assertEquals(5, donor.concurrentTasks);
        donor.active = false;
        coordinator.recordDecision(receiver, ProcessorSchedulingDecisionReason.GLOBAL_CAPACITY_FULL);
        assertEquals(5, donor.concurrentTasks);
        assertNull(coordinator.getRole(donor));
    }

    private void selectMove() {
        for (int evaluation = 0; evaluation < ConcurrentTaskMoveSelector.SUSTAINED_EVALUATIONS; evaluation++) {
            balance();
        }
    }

    private void balance() {
        coordinator.balance(List.of(demand("receiver", receiver, 10_000), demand("donor", donor, 1)), participants, true);
    }

    private void startReceiverTest() {
        receiver.concurrentTasks = 2;
        receiver.comparisonActive = true;
        coordinator.recordDecision(receiver, ProcessorSchedulingDecisionReason.TESTING_HIGHER_CONCURRENCY);
    }

    private void endReceiverTest(final int concurrentTasks, final ProcessorSchedulingDecisionReason reason) {
        receiver.concurrentTasks = concurrentTasks;
        receiver.comparisonActive = false;
        coordinator.recordDecision(receiver, reason);
    }

    private static ProcessorDemand demand(final String identifier, final TestParticipant participant, final long queuedFlowFiles) {
        final ProcessorSchedulingSnapshot snapshot = ProcessorSchedulingSnapshot.createBuilder()
                .setAppliedSettings(new SchedulingSettings(participant.concurrentTasks, 0L))
                .setProcessorReady(true)
                .setConcurrentTaskUtilization(1D)
                .setLocalInputQueueCount(queuedFlowFiles)
                .setRecentDemand(new RecentDemand(100, 10, 0, TimeUnit.SECONDS.toNanos(10)))
                .build();
        return new ProcessorDemand(identifier, false, participant.concurrentTasks, 12, participant.comparisonActive, false, snapshot);
    }

    private enum MoveEnding {
        THROUGHPUT_INCREASED,
        THROUGHPUT_NOT_INCREASED,
        RESET_NOTICED_BY_BALANCE,
        RESET_NOTICED_BY_EVALUATION,
        RECEIVER_STOPPED,
        RECEIVER_DECLINES
    }

    /**
     * Records the largest total of the active participants' settings reached by any change the coordinator makes.
     */
    private class TestParticipant implements ConcurrentTaskMoveCoordinator.Participant {
        private int concurrentTasks;
        private boolean active = true;
        private boolean comparisonActive;

        private TestParticipant(final int concurrentTasks) {
            this.concurrentTasks = concurrentTasks;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public int getConcurrentTasks() {
            return concurrentTasks;
        }

        @Override
        public boolean isConcurrencyComparisonActive() {
            return comparisonActive;
        }

        @Override
        public boolean changeConcurrentTasks(final int change) {
            if (!active || concurrentTasks + change < 1) {
                return false;
            }

            concurrentTasks += change;
            maxTotalConcurrentTasks = Math.max(maxTotalConcurrentTasks, receiver.getActiveConcurrentTasks() + donor.getActiveConcurrentTasks());
            return true;
        }

        private int getActiveConcurrentTasks() {
            return active ? concurrentTasks : 0;
        }
    }
}
