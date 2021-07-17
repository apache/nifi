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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.util.HashMap;
import java.util.Map;

public class AbstractMetricsRegistry {

    protected final CollectorRegistry registry = new CollectorRegistry();
    protected final Map<String, Gauge> nameToGaugeMap = new HashMap<>();
    protected final Map<String, Counter> nameToCounterMap = new HashMap<>();

    public CollectorRegistry getRegistry() {
        return registry;
    }

    public void setDataPoint(double val, String gaugeName, String... labels) {
        Gauge gauge = nameToGaugeMap.get(gaugeName);
        if (gauge == null) {
            throw new IllegalArgumentException("Gauge '" + gaugeName + "' does not exist in this registry");
        }

        gauge.labels(labels).set(val);
    }

    public void incrementCounter(double val, String counterName, String... labels) {
        Counter counter = nameToCounterMap.get(counterName);
        if (counter == null) {
            throw new IllegalArgumentException("Counter '" + counterName + "' does not exist in this registry");
        }

        counter.labels(labels).inc(val);
    }
}
