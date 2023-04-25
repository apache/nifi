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

package org.apache.nifi.minifi.toolkit.schema.v2;

import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CORE_PROPS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.MAX_CONCURRENT_THREADS_KEY;
import java.util.Map;

import org.apache.nifi.minifi.toolkit.schema.CorePropertiesSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchema;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;

public class CorePropertiesSchemaV2 extends BaseSchema implements ConvertableSchema<CorePropertiesSchema> {

    private static final int CONFIG_VERSION = 2;

    private String flowControllerGracefulShutdownPeriod = CorePropertiesSchema.DEFAULT_FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD;
    private String flowServiceWriteDelayInterval = CorePropertiesSchema.DEFAULT_FLOW_SERVICE_WRITE_DELAY_INTERVAL;
    private String administrativeYieldDuration = CorePropertiesSchema.DEFAULT_ADMINISTRATIVE_YIELD_DURATION;
    private String boredYieldDuration = CorePropertiesSchema.DEFAULT_BORED_YIELD_DURATION;
    private Number maxConcurrentThreads = CorePropertiesSchema.DEFAULT_MAX_CONCURRENT_THREADS;

    public CorePropertiesSchemaV2() {

    }

    public CorePropertiesSchemaV2(Map map) {
        flowControllerGracefulShutdownPeriod = getOptionalKeyAsType(map, CorePropertiesSchema.FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY, String.class,
                CORE_PROPS_KEY, CorePropertiesSchema.DEFAULT_FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD);
        flowServiceWriteDelayInterval = getOptionalKeyAsType(map, CorePropertiesSchema.FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY, String.class,
                CORE_PROPS_KEY, CorePropertiesSchema.DEFAULT_FLOW_SERVICE_WRITE_DELAY_INTERVAL);
        administrativeYieldDuration = getOptionalKeyAsType(map, CorePropertiesSchema.ADMINISTRATIVE_YIELD_DURATION_KEY, String.class,
                CORE_PROPS_KEY, CorePropertiesSchema.DEFAULT_ADMINISTRATIVE_YIELD_DURATION);
        boredYieldDuration = getOptionalKeyAsType(map, CorePropertiesSchema.BORED_YIELD_DURATION_KEY, String.class, CORE_PROPS_KEY, CorePropertiesSchema.DEFAULT_BORED_YIELD_DURATION);
        maxConcurrentThreads = getOptionalKeyAsType(map, MAX_CONCURRENT_THREADS_KEY, Number.class,
                CORE_PROPS_KEY, CorePropertiesSchema.DEFAULT_MAX_CONCURRENT_THREADS);
    }

    @Override
    public int getVersion() {
        return CONFIG_VERSION;
    }

    @Override
    public CorePropertiesSchema convert() {

        Map<String, Object> result = mapSupplier.get();
        result.put(CorePropertiesSchema.FLOW_CONTROLLER_SHUTDOWN_PERIOD_KEY, flowControllerGracefulShutdownPeriod);
        result.put(CorePropertiesSchema.FLOW_SERVICE_WRITE_DELAY_INTERVAL_KEY, flowServiceWriteDelayInterval);
        result.put(CorePropertiesSchema.ADMINISTRATIVE_YIELD_DURATION_KEY, administrativeYieldDuration);
        result.put(CorePropertiesSchema.BORED_YIELD_DURATION_KEY, boredYieldDuration);
        result.put(MAX_CONCURRENT_THREADS_KEY, maxConcurrentThreads);
        return new CorePropertiesSchema(result);

    }

}
