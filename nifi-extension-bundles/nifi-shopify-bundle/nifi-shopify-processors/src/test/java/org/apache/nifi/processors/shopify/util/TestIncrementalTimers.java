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
package org.apache.nifi.processors.shopify.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.shopify.util.IncrementalTimers.EXCLUSIVE_TIME_WINDOW_ADJUSTMENT;
import static org.apache.nifi.processors.shopify.util.IncrementalTimers.LAST_EXECUTION_TIME_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;


class TestIncrementalTimers {

    private static final Long INC_DELAY_MS = 3000L;
    private static final String TEST_TIME_STRING = "2022-08-11T12:34:23Z";
    private static final Instant CURRENT_EXECUTION_TIME = Instant.parse(TEST_TIME_STRING);

    @Test
    void testInitialStartTime() {
        final String initialStartTime = "2022-08-10T12:34:23Z";
        final IncrementalTimers timers = IncrementalTimers.ofState(Collections.emptyMap(), initialStartTime, INC_DELAY_MS, CURRENT_EXECUTION_TIME);
        assertEquals(initialStartTime, timers.getStartTime());
    }

    @Test
    void testNextStartTimeIsLastExecutionTime() {
        final String initialStartTime = "2022-08-10T12:34:23Z";
        final String lastExecutionTime = "2022-08-11T12:34:23Z";
        final Map<String, String> stateMap = new HashMap<>();
        stateMap.put(LAST_EXECUTION_TIME_KEY, lastExecutionTime);
        final IncrementalTimers timers = IncrementalTimers.ofState(stateMap, initialStartTime, INC_DELAY_MS, CURRENT_EXECUTION_TIME);
        assertEquals(lastExecutionTime, timers.getStartTime());
    }

    @Test
    void testNextEndTimeIsCurrentExecutionTimeMinusDelay() {
        final String expectedEndTime = CURRENT_EXECUTION_TIME.minus(INC_DELAY_MS, ChronoUnit.MILLIS).toString();
        final IncrementalTimers timers = IncrementalTimers.ofState(Collections.emptyMap(), null, INC_DELAY_MS, CURRENT_EXECUTION_TIME);
        assertEquals(expectedEndTime, timers.getEndTime());
    }

    @Test
    void testExclusiveEndTime() {
        final String expectedEndTime = CURRENT_EXECUTION_TIME.minus(INC_DELAY_MS, ChronoUnit.MILLIS).minus(EXCLUSIVE_TIME_WINDOW_ADJUSTMENT).toString();
        final IncrementalTimers timers = IncrementalTimers.ofState(Collections.emptyMap(), null, INC_DELAY_MS, CURRENT_EXECUTION_TIME);
        assertEquals(expectedEndTime, timers.getExclusiveEndTime());
    }

    @Test
    void testEndTimeIsTruncatedToSeconds() {
        TemporalAccessor temporalAccessor = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()).parse("2022-08-11T12:34:23.728");
        final Instant currentExecutionTime = Instant.from(temporalAccessor);
        final String expectedEndTime = currentExecutionTime.truncatedTo(ChronoUnit.SECONDS).toString();
        IncrementalTimers timers = IncrementalTimers.ofState(Collections.emptyMap(), null, 0L, currentExecutionTime);
        assertEquals(expectedEndTime, timers.getEndTime());
    }

}
