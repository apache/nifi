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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchedulingChangedRebaseHandlerTest {

    private static final String ROOT = "root";
    private static final String PROC_A = "proc-a";
    private static final String PROC_B = "proc-b";
    private static final String PORT_A = "port-a";
    private static final String TIMER_DRIVEN = "TIMER_DRIVEN";
    private static final String CRON_DRIVEN = "CRON_DRIVEN";
    private static final String TIMER_PERIOD = "1 min";
    private static final String UPSTREAM_TIMER_PERIOD = "5 min";
    private static final String CRON_EXPRESSION = "0 0 1 * * ?";

    private SchedulingChangedRebaseHandler strategyHandler;
    private SchedulingChangedRebaseHandler scheduleHandler;

    @BeforeEach
    void setup() {
        strategyHandler = new SchedulingChangedRebaseHandler(DifferenceType.SCHEDULING_STRATEGY_CHANGED);
        scheduleHandler = new SchedulingChangedRebaseHandler(DifferenceType.RUN_SCHEDULE_CHANGED);
    }

    @Test
    void testNoUpstreamChange() {
        final VersionedProcessor original = createProcessor(PROC_A, TIMER_DRIVEN, TIMER_PERIOD);
        final VersionedProcessor local = createProcessor(PROC_A, CRON_DRIVEN, CRON_EXPRESSION);
        final VersionedProcessor otherOriginal = createProcessor(PROC_B, TIMER_DRIVEN, TIMER_PERIOD);
        final VersionedProcessor otherUpstream = createProcessor(PROC_B, TIMER_DRIVEN, UPSTREAM_TIMER_PERIOD);
        final Set<FlowDifference> upstreamDifferences = Set.of(scheduleDifference(otherOriginal, otherUpstream));

        assertEquals(RebaseClassification.COMPATIBLE,
                strategyHandler.classify(strategyDifference(original, local), upstreamDifferences, createRoot()).getClassification());
        assertEquals(RebaseClassification.COMPATIBLE,
                scheduleHandler.classify(scheduleDifference(original, local), upstreamDifferences, createRoot()).getClassification());
    }

    @Test
    void testUpstreamChangedRunScheduleOnlyConflictsWithLocalStrategyChange() {
        final VersionedProcessor original = createProcessor(PROC_A, TIMER_DRIVEN, TIMER_PERIOD);
        final VersionedProcessor local = createProcessor(PROC_A, CRON_DRIVEN, CRON_EXPRESSION);
        final VersionedProcessor upstream = createProcessor(PROC_A, TIMER_DRIVEN, UPSTREAM_TIMER_PERIOD);
        final Set<FlowDifference> upstreamDifferences = Set.of(scheduleDifference(original, upstream));

        final RebaseAnalysis.ClassifiedDifference result = strategyHandler.classify(strategyDifference(original, local), upstreamDifferences, createRoot());

        assertEquals(RebaseClassification.CONFLICTING, result.getClassification());
        assertEquals(RebaseConflictCode.SAME_SCHEDULING, result.getConflictCode());
    }

    @Test
    void testUpstreamChangedToSameScheduling() {
        final VersionedProcessor original = createProcessor(PROC_A, TIMER_DRIVEN, TIMER_PERIOD);
        final VersionedProcessor local = createProcessor(PROC_A, CRON_DRIVEN, CRON_EXPRESSION);
        final VersionedProcessor upstream = createProcessor(PROC_A, CRON_DRIVEN, CRON_EXPRESSION);
        final Set<FlowDifference> upstreamDifferences = Set.of(strategyDifference(original, upstream), scheduleDifference(original, upstream));

        assertEquals(RebaseClassification.COMPATIBLE,
                strategyHandler.classify(strategyDifference(original, local), upstreamDifferences, createRoot()).getClassification());
        assertEquals(RebaseClassification.COMPATIBLE,
                scheduleHandler.classify(scheduleDifference(original, local), upstreamDifferences, createRoot()).getClassification());
    }

    @Test
    void testUnsupportedComponentType() {
        final VersionedPort port = new VersionedPort();
        port.setIdentifier(PORT_A);
        final FlowDifference localDifference = new StandardFlowDifference(DifferenceType.RUN_SCHEDULE_CHANGED, port, port,
                TIMER_PERIOD, UPSTREAM_TIMER_PERIOD, "Run schedule changed");

        final RebaseAnalysis.ClassifiedDifference result = scheduleHandler.classify(localDifference, Collections.emptySet(), createRoot());

        assertEquals(RebaseClassification.UNSUPPORTED, result.getClassification());
        assertEquals(RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE, result.getConflictCode());
    }

    @Test
    void testApplyTimerDrivenToCronDriven() {
        final VersionedProcessor original = createProcessor(PROC_A, TIMER_DRIVEN, TIMER_PERIOD);
        final VersionedProcessor local = createProcessor(PROC_A, CRON_DRIVEN, CRON_EXPRESSION);
        final VersionedProcessor merged = createProcessor(PROC_A, TIMER_DRIVEN, TIMER_PERIOD);
        final VersionedProcessGroup mergedFlow = createRoot();
        mergedFlow.getProcessors().add(merged);

        strategyHandler.apply(strategyDifference(original, local), mergedFlow);
        scheduleHandler.apply(scheduleDifference(original, local), mergedFlow);

        assertEquals(CRON_DRIVEN, merged.getSchedulingStrategy());
        assertEquals(CRON_EXPRESSION, merged.getSchedulingPeriod());
    }

    private FlowDifference strategyDifference(final VersionedProcessor before, final VersionedProcessor after) {
        return new StandardFlowDifference(DifferenceType.SCHEDULING_STRATEGY_CHANGED, before, after,
                before.getSchedulingStrategy(), after.getSchedulingStrategy(), "Scheduling strategy changed");
    }

    private FlowDifference scheduleDifference(final VersionedProcessor before, final VersionedProcessor after) {
        return new StandardFlowDifference(DifferenceType.RUN_SCHEDULE_CHANGED, before, after,
                before.getSchedulingPeriod(), after.getSchedulingPeriod(), "Run schedule changed");
    }

    private VersionedProcessor createProcessor(final String identifier, final String strategy, final String period) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        processor.setSchedulingStrategy(strategy);
        processor.setSchedulingPeriod(period);
        return processor;
    }

    private VersionedProcessGroup createRoot() {
        final VersionedProcessGroup root = new VersionedProcessGroup();
        root.setIdentifier(ROOT);
        return root;
    }
}
