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

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

/**
 * Builds the JsonObject for an individual metric.
 */
public class MetricBuilder {

    private final JsonBuilderFactory factory;

    private String applicationId;
    private String instanceId;
    private String hostname;
    private String timestamp;
    private String metricName;
    private String metricValue;

    public MetricBuilder(final JsonBuilderFactory factory) {
        this.factory = factory;
    }

    public MetricBuilder applicationId(final String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public MetricBuilder instanceId(final String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public MetricBuilder hostname(final String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MetricBuilder timestamp(final long timestamp) {
        this.timestamp = String.valueOf(timestamp);
        return this;
    }

    public MetricBuilder metricName(final String metricName) {
        this.metricName = metricName;
        return this;
    }

    public MetricBuilder metricValue(final String metricValue) {
        this.metricValue = metricValue;
        return this;
    }

    public JsonObject build() {
        return factory.createObjectBuilder()
                .add(MetricFields.METRIC_NAME, metricName)
                .add(MetricFields.APP_ID, applicationId)
                .add(MetricFields.INSTANCE_ID, instanceId)
                .add(MetricFields.HOSTNAME, hostname)
                .add(MetricFields.TIMESTAMP, timestamp)
                .add(MetricFields.START_TIME, timestamp)
                .add(MetricFields.METRICS,
                        factory.createObjectBuilder()
                                .add(String.valueOf(timestamp), metricValue)
                ).build();
    }

}
