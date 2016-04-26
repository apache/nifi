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
package org.apache.nifi.minifi.bootstrap.util.schema;

import org.apache.nifi.minifi.bootstrap.util.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.MAX_CONCURRENT_THREADS_KEY;

/**
 *
 */
public class CorePropertiesSchema extends BaseSchema {

    public static final String FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY = "flow controller graceful shutdown period";
    public static final String FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY = "flow service write delay interval";
    public static final String ADMINISTRATIVE_YIELD_DURATION_KEY = "administrative yield duration";
    public static final String BORED_YIELD_DURATION_KEY = "bored yield duration";


    private String flowControllerGracefulShutdownPeriod = "10 sec";
    private String flowServiceWriteDelayInterval = "500 ms";
    private String administrativeYieldDuration = "30 sec";
    private String boredYieldDuration = "10 millis";
    private Number maxConcurrentThreads = 1;

    public CorePropertiesSchema() {
    }

    public CorePropertiesSchema(Map map) {
        flowControllerGracefulShutdownPeriod = getOptionalKeyAsType(map, FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY, String.class, CORE_PROPS_KEY, "10 sec");
        flowServiceWriteDelayInterval = getOptionalKeyAsType(map, FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY, String.class, CORE_PROPS_KEY, "500 ms");
        administrativeYieldDuration = getOptionalKeyAsType(map, ADMINISTRATIVE_YIELD_DURATION_KEY, String.class, CORE_PROPS_KEY, "30 sec");
        boredYieldDuration = getOptionalKeyAsType(map, BORED_YIELD_DURATION_KEY, String.class, CORE_PROPS_KEY, "10 millis");
        maxConcurrentThreads = getOptionalKeyAsType(map, MAX_CONCURRENT_THREADS_KEY, Number.class, CORE_PROPS_KEY, 1);
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
