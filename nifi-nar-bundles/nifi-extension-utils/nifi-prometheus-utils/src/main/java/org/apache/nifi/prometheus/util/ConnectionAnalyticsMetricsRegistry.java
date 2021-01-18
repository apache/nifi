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
package org.apache.nifi.prometheus.util;

import io.prometheus.client.Gauge;

public class ConnectionAnalyticsMetricsRegistry extends AbstractMetricsRegistry {

    public ConnectionAnalyticsMetricsRegistry() {

        // Connection status analytics metrics
        nameToGaugeMap.put("TIME_TO_BYTES_BACKPRESSURE_PREDICTION", Gauge.build()
                .name("nifi_time_to_bytes_backpressure_prediction")
                .help("Predicted time (in milliseconds) until backpressure will be applied on the connection due to bytes in the queue")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("TIME_TO_COUNT_BACKPRESSURE_PREDICTION", Gauge.build()
                .name("nifi_time_to_count_backpressure_prediction")
                .help("Predicted time (in milliseconds) until backpressure will be applied on the connection due to number of objects in the queue")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("BYTES_AT_NEXT_INTERVAL_PREDICTION", Gauge.build()
                .name("nifi_bytes_at_next_interval_prediction")
                .help("Predicted number of bytes in the queue at the next configured interval")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("COUNT_AT_NEXT_INTERVAL_PREDICTION", Gauge.build()
                .name("nifi_count_at_next_interval_prediction")
                .help("Predicted number of objects in the queue at the next configured interval")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));
    }
}
