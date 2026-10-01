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
import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveSelector.TaskMove;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingSnapshot.RecentDemand;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentTaskMoveSelectorTest {
    private static final long MEASURED_NANOS = TimeUnit.SECONDS.toNanos(10);

    @Test
    void testMoveRequiresSustainedMarginBetweenHighestReceiverAndLowestDonor() {
        // At 100 committed FlowFiles per second: 10,000 queued is capped at 60 seconds, 50 queued is 0.5 seconds, and 200 queued is 2 seconds.
        final List<ProcessorDemand> demands = List.of(queueConsumer("other-donor", 6, 200, 100), queueConsumer("receiver", 1, 10_000, 100),
                queueConsumer("lowest-donor", 6, 50, 100));
        final ConcurrentTaskMoveSelector selector = new ConcurrentTaskMoveSelector();
        assertTrue(selectSustainedMove(selector, demands, false).isEmpty());
        assertTrue(selector.selectMove(demands, true).isEmpty());
        assertTrue(selector.selectMove(demands, true).isEmpty());
        assertEquals(Optional.of(new TaskMove("receiver", "lowest-donor")), selector.selectMove(demands, true));

        // After a move is returned, the margin must be sustained again before the next move.
        assertTrue(selector.selectMove(demands, true).isEmpty());
        assertTrue(selector.selectMove(demands, true).isEmpty());
        assertTrue(selector.selectMove(demands, true).isPresent());

        // 12 seconds against 7 seconds meets the minimum difference but not twice the donor's score.
        assertTrue(selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(queueConsumer("receiver", 1, 1_200, 100), queueConsumer("donor", 6, 700, 100)), true)
                .isEmpty());
        // Nearly empty queues never reach the minimum receiver score, so they do not pass tasks between themselves.
        assertTrue(selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(queueConsumer("receiver", 1, 90, 100), queueConsumer("donor", 6, 1, 100)), true)
                .isEmpty());
        // Among donors with the same score, the donor with the most tasks gives one up.
        final List<ProcessorDemand> equalDonors = List.of(queueConsumer("receiver", 1, 10_000, 100), queueConsumer("fewer", 3, 0, 100),
                queueConsumer("more", 8, 0, 100));
        assertEquals(Optional.of(new TaskMove("receiver", "more")), selectSustainedMove(new ConcurrentTaskMoveSelector(), equalDonors, true));
    }

    @Test
    void testSourceTakesTasksOnlyFromProcessorsWithoutRealBacklog() {
        final ProcessorDemand activeSource = source(true, true, 1D);
        assertEquals(ConcurrentTaskMoveSelector.SOURCE_SCORE, ConcurrentTaskMoveSelector.calculateScore(activeSource));
        assertEquals(0D, ConcurrentTaskMoveSelector.calculateScore(source(false, true, 1D)));
        assertEquals(0D, ConcurrentTaskMoveSelector.calculateScore(source(true, false, 1D)));
        assertEquals(0D, ConcurrentTaskMoveSelector.calculateScore(source(true, true, 0.5D)));

        assertEquals(Optional.of(new TaskMove("source", "donor")),
                selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(activeSource, queueConsumer("donor", 6, 10, 100)), true));
        // 20 queued at 1 per second is 20 seconds of work, a real backlog.
        assertTrue(selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(activeSource, queueConsumer("donor", 6, 20, 1)), true).isEmpty());
        // A source is never a donor.
        assertTrue(selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(queueConsumer("receiver", 1, 10_000, 100), activeSource), true).isEmpty());
    }

    @Test
    void testIneligibleReceiversAndDonors() {
        final ProcessorDemand donor = queueConsumer("donor", 6, 1, 100);
        final List<ProcessorDemand> ineligibleReceivers = List.of(
                demand("not-ready", 1, false, false, false, 10_000, new RecentDemand(100, 10, 0, MEASURED_NANOS)),
                demand("at-maximum", 12, true, false, false, 10_000, new RecentDemand(100, 10, 0, MEASURED_NANOS)),
                demand("failing", 1, true, false, false, 10_000, new RecentDemand(100, 10, 1, MEASURED_NANOS)),
                demand("comparing", 1, true, true, false, 10_000, new RecentDemand(100, 10, 0, MEASURED_NANOS)),
                demand("cooling-down", 1, true, false, true, 10_000, new RecentDemand(100, 10, 0, MEASURED_NANOS)),
                demand("newly-started", 1, true, false, false, 10_000, new RecentDemand(100, 10, 0, TimeUnit.SECONDS.toNanos(5))),
                demand("not-progressing", 1, true, false, false, 10_000, new RecentDemand(0, 10, 0, MEASURED_NANOS)));
        for (final ProcessorDemand ineligibleReceiver : ineligibleReceivers) {
            assertTrue(selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(ineligibleReceiver, donor), true).isEmpty(), ineligibleReceiver.identifier());
        }

        // Failing and non-progressing Processors score 0, so they give up extra tasks first.
        final ProcessorDemand receiver = queueConsumer("receiver", 1, 10_000, 100);
        final ProcessorDemand busyDonor = queueConsumer("busy-donor", 6, 50, 100);
        final ProcessorDemand failingDonor = demand("failing-donor", 6, true, false, false, 10_000, new RecentDemand(100, 10, 1, MEASURED_NANOS));
        final ProcessorDemand stalledDonor = demand("stalled-donor", 6, true, false, false, 10_000, new RecentDemand(0, 10, 0, MEASURED_NANOS));
        assertEquals(Optional.of(new TaskMove("receiver", "failing-donor")), selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(receiver, busyDonor, failingDonor), true));
        assertEquals(Optional.of(new TaskMove("receiver", "stalled-donor")), selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(receiver, busyDonor, stalledDonor), true));

        // A newly started Processor with no commits yet has the maximum score, so it does not give up tasks.
        assertEquals(ConcurrentTaskMoveSelector.MAX_SCORE,
                ConcurrentTaskMoveSelector.calculateScore(demand("new", 6, true, false, false, 10_000, new RecentDemand(0, 0, 0, TimeUnit.SECONDS.toNanos(2)))));
        // A donor keeps at least one task, and a donor measuring its own comparison is left alone.
        final ProcessorDemand comparingDonor = demand("comparing", 6, true, true, false, 1, new RecentDemand(100, 10, 0, MEASURED_NANOS));
        assertTrue(selectSustainedMove(new ConcurrentTaskMoveSelector(), List.of(receiver, queueConsumer("single-task", 1, 1, 100), comparingDonor), true).isEmpty());
    }

    private static Optional<TaskMove> selectSustainedMove(final ConcurrentTaskMoveSelector selector, final List<ProcessorDemand> demands, final boolean globalCapacityFull) {
        Optional<TaskMove> move = Optional.empty();
        for (int evaluation = 0; evaluation < ConcurrentTaskMoveSelector.SUSTAINED_EVALUATIONS; evaluation++) {
            move = selector.selectMove(demands, globalCapacityFull);
        }

        return move;
    }

    private static ProcessorDemand queueConsumer(final String identifier, final int concurrentTasks, final long queuedFlowFiles, final double committedPerSecond) {
        return demand(identifier, concurrentTasks, true, false, false, queuedFlowFiles, new RecentDemand(committedPerSecond, 10, 0, MEASURED_NANOS));
    }

    private static ProcessorDemand source(final boolean ready, final boolean recentlyActive, final double concurrentTaskUtilization) {
        final ProcessorSchedulingSnapshot snapshot = ProcessorSchedulingSnapshot.createBuilder()
                .setAppliedSettings(new SchedulingSettings(1, 0L))
                .setProcessorReady(ready)
                .setSourceProcessorRecentlyReportedActivity(recentlyActive)
                .setConcurrentTaskUtilization(concurrentTaskUtilization)
                .setRecentDemand(new RecentDemand(0, 10, 0, MEASURED_NANOS))
                .build();
        return new ProcessorDemand("source", true, 6, 12, false, false, snapshot);
    }

    private static ProcessorDemand demand(final String identifier, final int concurrentTasks, final boolean ready, final boolean comparisonActive,
                                          final boolean coolingDown, final long queuedFlowFiles, final RecentDemand recentDemand) {
        final ProcessorSchedulingSnapshot snapshot = ProcessorSchedulingSnapshot.createBuilder()
                .setAppliedSettings(new SchedulingSettings(1, 0L))
                .setProcessorReady(ready)
                .setConcurrentTaskUtilization(1D)
                .setLocalInputQueueCount(queuedFlowFiles)
                .setRecentDemand(recentDemand)
                .build();
        return new ProcessorDemand(identifier, false, concurrentTasks, 12, comparisonActive, coolingDown, snapshot);
    }
}
