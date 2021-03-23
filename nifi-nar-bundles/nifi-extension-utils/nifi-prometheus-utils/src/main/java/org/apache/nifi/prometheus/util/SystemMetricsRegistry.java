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

public class SystemMetricsRegistry extends AbstractMetricsRegistry {
    public SystemMetricsRegistry() {
        nameToGaugeMap.put("MAX_EVENT_DRIVEN_THREADS", Gauge.build()
                .name("nifi_max_event_driven_threads")
                .help("The maximum number of threads for event driven processors available to the system.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("MAX_TIMER_DRIVEN_THREADS", Gauge.build()
                .name("nifi_max_timer_driven_threads")
                .help("The maximum number of threads for timer driven processors available to the system.")
                .labelNames("instance")
                .register(registry));

        nameToGaugeMap.put("REPOSITORY_USED_BYTES", Gauge.build()
                .name("nifi_repository_used_bytes")
                .help("The number of bytes currently used by the specified repository.")
                .labelNames("instance", "identifier", "repository_type")
                .register(registry));

        nameToGaugeMap.put("REPOSITORY_MAX_BYTES", Gauge.build()
                .name("nifi_repository_max_bytes")
                .help("The maximum number of bytes available to the specified repository.")
                .labelNames("instance", "identifier", "repository_type")
                .register(registry));
    }
}

