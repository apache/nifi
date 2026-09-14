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
package org.apache.nifi.controller.metrics;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Single measurement for a named Counter recorded during processing
 *
 * @param name Counter Name
 * @param value Counter Value
 * @param attributes Map of keys and values associated with the Counter measurement, which may be empty but not null
 * @param recorded Timestamp when the Component recorded the Counter value
 * @param componentMetricContext Context for Component Metric record
 */
public record CounterRecord(
        String name,
        long value,
        Map<String, String> attributes,
        Instant recorded,
        ComponentMetricContext componentMetricContext
) {
    public CounterRecord {
        attributes = Map.copyOf(Objects.requireNonNull(attributes, "Attributes required"));
    }

    /**
     * Counter Record constructor for compatibility with earlier versions
     *
     * @param name Counter Name
     * @param value Counter Value
     * @param recorded Timestamp when the Processor recorded the Counter value
     * @param componentMetricContext Context for Component Metric record
     */
    public CounterRecord(
            final String name,
            final long value,
            final Instant recorded,
            final ComponentMetricContext componentMetricContext
    ) {
        this(
                name,
                value,
                Map.of(),
                recorded,
                componentMetricContext
        );
    }
}
