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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class IncrementalTimers {

    static final Duration EXCLUSIVE_TIME_WINDOW_ADJUSTMENT = Duration.ofMillis(1);
    static final String LAST_EXECUTION_TIME_KEY = "last_execution_time";

    private final String startTime;
    private final String endTime;
    private final String exclusiveEndTime;

    private IncrementalTimers(String startTime, String endTime, String exclusiveEndTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.exclusiveEndTime = exclusiveEndTime;
    }

    public static IncrementalTimers ofState(final Map<String, String> stateMap, final String initialStartTime, final Long incrDelayMs,
                                             Instant currentExecutionTime) {
        String startTime = stateMap.get(LAST_EXECUTION_TIME_KEY);
        if (startTime == null && initialStartTime != null) {
            startTime = initialStartTime;
        }

        if (incrDelayMs != null) {
            currentExecutionTime = currentExecutionTime.minus(incrDelayMs, ChronoUnit.MILLIS);
        }
        currentExecutionTime = currentExecutionTime.truncatedTo(ChronoUnit.SECONDS);
        final String endTime = currentExecutionTime.toString();
        final String exclusiveEndTime = currentExecutionTime.minus(EXCLUSIVE_TIME_WINDOW_ADJUSTMENT).toString();
        return new IncrementalTimers(startTime, endTime, exclusiveEndTime);
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getExclusiveEndTime() {
        return exclusiveEndTime;
    }
}
