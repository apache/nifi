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

import java.io.Closeable;

/**
 * Framework abstraction for reporting metrics emitted from processing components
 */
public interface ComponentMetricReporter extends Closeable {
    /**
     * Configuration lifecycle method that the application invokes after instantiating the class
     *
     * @param context Configuration Context properties
     */
    default void onConfigured(ComponentMetricReporterConfigurationContext context) {

    }

    /**
     * Close resources created during Reporter configuration and processing
     */
    @Override
    default void close() {
    }

    /**
     * Record Gauge Record
     *
     * @param gaugeRecord Gauge Record required
     */
    void recordGauge(GaugeRecord gaugeRecord);

    /**
     * Record Counter Record
     *
     * @param counterRecord Counter Record required
     */
    void recordCounter(CounterRecord counterRecord);
}
