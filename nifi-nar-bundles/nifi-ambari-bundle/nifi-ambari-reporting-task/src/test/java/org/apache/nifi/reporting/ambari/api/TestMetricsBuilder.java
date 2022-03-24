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
package org.apache.nifi.reporting.ambari.api;

import org.apache.nifi.reporting.util.metrics.api.MetricFields;
import org.apache.nifi.reporting.util.metrics.api.MetricsBuilder;
import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestMetricsBuilder {

    @Test
    public void testBuildMetricsObject() {
        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);

        final String instanceId = "1234-5678-1234-5678";
        final String applicationId = "NIFI";
        final String hostname = "localhost";
        final long timestamp = System.currentTimeMillis();

        final Map<String,String> metrics = new HashMap<>();
        metrics.put("a", "1");
        metrics.put("b", "2");

        final MetricsBuilder metricsBuilder = new MetricsBuilder(factory);
        final JsonObject metricsObject = metricsBuilder
                .applicationId(applicationId)
                .instanceId(instanceId)
                .hostname(hostname)
                .timestamp(timestamp)
                .addAllMetrics(metrics)
                .build();

        final JsonArray metricsArray = metricsObject.getJsonArray("metrics");
        assertNotNull(metricsArray);
        assertEquals(2, metricsArray.size());

        JsonObject firstMetric = metricsArray.getJsonObject(0);
        if (!"a".equals(firstMetric.getString(MetricFields.METRIC_NAME))) {
            firstMetric = metricsArray.getJsonObject(1);
        }

        assertEquals("a", firstMetric.getString(MetricFields.METRIC_NAME));
        assertEquals(applicationId, firstMetric.getString(MetricFields.APP_ID));
        assertEquals(instanceId, firstMetric.getString(MetricFields.INSTANCE_ID));
        assertEquals(hostname, firstMetric.getString(MetricFields.HOSTNAME));
        assertEquals(String.valueOf(timestamp), firstMetric.getString(MetricFields.TIMESTAMP));

        final JsonObject firstMetricValues = firstMetric.getJsonObject("metrics");
        assertEquals("1", firstMetricValues.getString("" + timestamp));
    }

}
