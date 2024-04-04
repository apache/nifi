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
package org.apache.nifi.reporting.datadog.api;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds the overall JsonObject for the Metrics.
 */
public class MetricsBuilder {

    static final String ROOT_JSON_ELEMENT = "metrics";

    private final JsonBuilderFactory factory;

    private long timestamp;
    private String applicationId;
    private String instanceId;
    private String hostname;
    private Map<String,String> metrics = new HashMap<>();

    public MetricsBuilder(final JsonBuilderFactory factory) {
        this.factory = factory;
    }

    public MetricsBuilder applicationId(final String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public MetricsBuilder instanceId(final String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public MetricsBuilder hostname(final String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MetricsBuilder timestamp(final long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public MetricsBuilder metric(final String name, String value) {
        this.metrics.put(name, value);
        return this;
    }

    public MetricsBuilder addAllMetrics(final Map<String,String> metrics) {
        this.metrics.putAll(metrics);
        return this;
    }

    public JsonObject build() {
        // builds JsonObject for individual metrics
        final MetricBuilder metricBuilder = new MetricBuilder(factory);
        metricBuilder.instanceId(instanceId).applicationId(applicationId).timestamp(timestamp).hostname(hostname);

        final JsonArrayBuilder metricArrayBuilder = factory.createArrayBuilder();

        for (Map.Entry<String,String> entry : metrics.entrySet()) {
            metricBuilder.metricName(entry.getKey()).metricValue(entry.getValue());
            metricArrayBuilder.add(metricBuilder.build());
        }

        // add the array of metrics to a top-level json object
        final JsonObjectBuilder metricsBuilder = factory.createObjectBuilder();
        metricsBuilder.add(ROOT_JSON_ELEMENT, metricArrayBuilder);
        return metricsBuilder.build();
    }

}
