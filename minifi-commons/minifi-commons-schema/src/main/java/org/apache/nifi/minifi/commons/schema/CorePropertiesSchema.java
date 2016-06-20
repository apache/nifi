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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.MAX_CONCURRENT_THREADS_KEY;

/**
 *
 */
public class CorePropertiesSchema extends BaseSchema {

    public static final String FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY = "flow controller graceful shutdown period";
    public static final String FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY = "flow service write delay interval";
    public static final String ADMINISTRATIVE_YIELD_DURATION_KEY = "administrative yield duration";
    public static final String BORED_YIELD_DURATION_KEY = "bored yield duration";

    public static final String DEFAULT_FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD = "10 sec";
    public static final String DEFAULT_FLOW_SERVICE_WRITE_DELAY_INTERVAL = "500 ms";
    public static final String DEFAULT_ADMINISTRATIVE_YIELD_DURATION = "30 sec";
    public static final String DEFAULT_BORED_YIELD_DURATION = "10 millis";
    public static final int DEFAULT_MAX_CONCURRENT_THREADS = 1;

    private String flowControllerGracefulShutdownPeriod = DEFAULT_FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD;
    private String flowServiceWriteDelayInterval = DEFAULT_FLOW_SERVICE_WRITE_DELAY_INTERVAL;
    private String administrativeYieldDuration = DEFAULT_ADMINISTRATIVE_YIELD_DURATION;
    private String boredYieldDuration = DEFAULT_BORED_YIELD_DURATION;
    private Number maxConcurrentThreads = DEFAULT_MAX_CONCURRENT_THREADS;

    public CorePropertiesSchema() {
    }

    public CorePropertiesSchema(Map map) {
        flowControllerGracefulShutdownPeriod = getOptionalKeyAsType(map, FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY, String.class,
                CORE_PROPS_KEY, DEFAULT_FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD);
        flowServiceWriteDelayInterval = getOptionalKeyAsType(map, FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY, String.class,
                CORE_PROPS_KEY, DEFAULT_FLOW_SERVICE_WRITE_DELAY_INTERVAL);
        administrativeYieldDuration = getOptionalKeyAsType(map, ADMINISTRATIVE_YIELD_DURATION_KEY, String.class,
                CORE_PROPS_KEY, DEFAULT_ADMINISTRATIVE_YIELD_DURATION);
        boredYieldDuration = getOptionalKeyAsType(map, BORED_YIELD_DURATION_KEY, String.class, CORE_PROPS_KEY, DEFAULT_BORED_YIELD_DURATION);
        maxConcurrentThreads = getOptionalKeyAsType(map, MAX_CONCURRENT_THREADS_KEY, Number.class,
                CORE_PROPS_KEY, DEFAULT_MAX_CONCURRENT_THREADS);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY, flowControllerGracefulShutdownPeriod);
        result.put(FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY, flowServiceWriteDelayInterval);
        result.put(ADMINISTRATIVE_YIELD_DURATION_KEY, administrativeYieldDuration);
        result.put(BORED_YIELD_DURATION_KEY, boredYieldDuration);
        result.put(MAX_CONCURRENT_THREADS_KEY, maxConcurrentThreads);
        return result;
    }

    public String getFlowControllerGracefulShutdownPeriod() {
        return flowControllerGracefulShutdownPeriod;
    }

    public String getFlowServiceWriteDelayInterval() {
        return flowServiceWriteDelayInterval;
    }

    public String getAdministrativeYieldDuration() {
        return administrativeYieldDuration;
    }

    public String getBoredYieldDuration() {
        return boredYieldDuration;
    }

    public Number getMaxConcurrentThreads() {
        return maxConcurrentThreads;
    }
}
