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
package org.apache.nifi.reporting.prometheus;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.prometheus.util.AbstractMetricsRegistry;
import org.apache.nifi.prometheus.util.ConnectionAnalyticsMetricsRegistry;
import org.apache.nifi.prometheus.util.NiFiMetricsRegistry;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.util.StringUtils.EMPTY;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPrometheusMetricsUtil {
    private static final long DEFAULT_PREDICTION_VALUE = -1L;
    private static final double EXPECTED_DEFAULT_PREDICTION_VALUE = -1.0;
    private static final double EXPECTED_BACKPRESSURE_PREDICTION_VALUE = 0.0;
    private static final double EXPECTED_FALSE_BACKPRESSURE = 0.0;
    private static final double EXPECTED_TRUE_BACKPRESSURE = 1.0;
    private static final double EXPECTED_DEFAULT_PERCENT_USED_VALUE = 0.0;
    private static final double EXPECTED_BACKPRESSURE_PERCENT_USED_VALUE = 100.0;
    private static final double EXPECTED_NESTED_BYTES_PERCENT_VALUE = 150.0 / 200.0 * 100.0;
    private static final double EXPECTED_NESTED_COUNT_PERCENT_VALUE = 5.0 / 30.0 * 100.0;
    private static final String NIFI_PERCENT_USED_BYTES = "nifi_percent_used_bytes";
    private static final String NIFI_PERCENT_USED_COUNT = "nifi_percent_used_count";
    private static final String BYTES_AT_BACKPRESSURE = "bytesAtBackpressure";
    private static final String COUNT_AT_BACKPRESSURE = "countAtBackpressure";
    private static final String NIFI_TIME_TO_BYTES_BACKPRESSURE_PREDICTION = "nifi_time_to_bytes_backpressure_prediction";
    private static final String NIFI_TIME_TO_COUNT_BACKPRESSURE_PREDICTION = "nifi_time_to_count_backpressure_prediction";
    private static final String CONNECTION_1 = "Connection1";
    private static final String CONNECTION_2 = "Connection2";
    private static final String CONNECTION_3 = "Connection3";
    private static final String CONNECTION_4 = "Connection4";
    private static final String TIME_TO_BYTES_BACKPRESSURE_MILLIS = "timeToBytesBackpressureMillis";
    private static final String TIME_TO_COUNT_BACKPRESSURE_MILLIS = "timeToCountBackpressureMillis";

    private static ProcessGroupStatus singleProcessGroupStatus;
    private static ProcessGroupStatus nestedProcessGroupStatus;
    private static ProcessGroupStatus singleProcessGroupStatusWithBytesBackpressure;
    private static ProcessGroupStatus nestedProcessGroupStatusWithCountBackpressure;
    private static Set<String> connections;
    private static Map<String, Map<String, Long>> mixedValuedPredictions;
    private static Map<String, Map<String, Long>> defaultValuedPredictions;

    @BeforeAll
    public static void setup() {
        singleProcessGroupStatus = createSingleProcessGroupStatus(0, 1, 0, 1);
        nestedProcessGroupStatus = createNestedProcessGroupStatus();
        singleProcessGroupStatusWithBytesBackpressure = createSingleProcessGroupStatus(1, 1, 0, 1);
        nestedProcessGroupStatusWithCountBackpressure = createNestedProcessGroupStatusWithCountBackpressure();
        connections = createConnections();
        mixedValuedPredictions = createPredictionsWithMixedValue();
        defaultValuedPredictions = createPredictionsWithDefaultValuesOnly();
    }

    @Test
    public void testAggregatePercentUsedWithSingleProcessGroup() {
        final Map<String, Double> aggregatedMetrics = new HashMap<>();

        PrometheusMetricsUtil.aggregatePercentUsed(singleProcessGroupStatus, aggregatedMetrics);

        assertEquals(4, aggregatedMetrics.size());
        assertEquals(EXPECTED_DEFAULT_PERCENT_USED_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_BYTES));
        assertEquals(EXPECTED_DEFAULT_PERCENT_USED_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_COUNT));
        assertEquals(EXPECTED_FALSE_BACKPRESSURE, aggregatedMetrics.get(BYTES_AT_BACKPRESSURE));
        assertEquals(EXPECTED_FALSE_BACKPRESSURE, aggregatedMetrics.get(COUNT_AT_BACKPRESSURE));
    }

    @Test
    public void testAggregatePercentUsedWithSingleProcessGroupWithBytesBackpressure() {
        final Map<String, Double> aggregatedMetrics = new HashMap<>();

        PrometheusMetricsUtil.aggregatePercentUsed(singleProcessGroupStatusWithBytesBackpressure, aggregatedMetrics);

        assertEquals(4, aggregatedMetrics.size());
        assertEquals(EXPECTED_BACKPRESSURE_PERCENT_USED_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_BYTES));
        assertEquals(EXPECTED_DEFAULT_PERCENT_USED_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_COUNT));
        assertEquals(EXPECTED_TRUE_BACKPRESSURE, aggregatedMetrics.get(BYTES_AT_BACKPRESSURE));
        assertEquals(EXPECTED_FALSE_BACKPRESSURE, aggregatedMetrics.get(COUNT_AT_BACKPRESSURE));
    }

    @Test
    public void testAggregatePercentUsedWithNestedProcessGroups() {
        final Map<String, Double> aggregatedMetrics = new HashMap<>();

        PrometheusMetricsUtil.aggregatePercentUsed(nestedProcessGroupStatus, aggregatedMetrics);

        assertEquals(4, aggregatedMetrics.size());
        assertEquals(EXPECTED_NESTED_BYTES_PERCENT_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_BYTES));
        assertEquals(EXPECTED_NESTED_COUNT_PERCENT_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_COUNT));
        assertEquals(EXPECTED_FALSE_BACKPRESSURE, aggregatedMetrics.get(BYTES_AT_BACKPRESSURE));
        assertEquals(EXPECTED_FALSE_BACKPRESSURE, aggregatedMetrics.get(COUNT_AT_BACKPRESSURE));
    }

    @Test
    public void testAggregatePercentUsedWithNestedProcessGroupsWithCountBackpressure() {
        final Map<String, Double> aggregatedMetrics = new HashMap<>();

        PrometheusMetricsUtil.aggregatePercentUsed(nestedProcessGroupStatusWithCountBackpressure, aggregatedMetrics);

        assertEquals(4, aggregatedMetrics.size());
        assertEquals(EXPECTED_NESTED_BYTES_PERCENT_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_BYTES));
        assertEquals(EXPECTED_BACKPRESSURE_PERCENT_USED_VALUE, aggregatedMetrics.get(NIFI_PERCENT_USED_COUNT));
        assertEquals(EXPECTED_FALSE_BACKPRESSURE, aggregatedMetrics.get(BYTES_AT_BACKPRESSURE));
        assertEquals(EXPECTED_TRUE_BACKPRESSURE, aggregatedMetrics.get(COUNT_AT_BACKPRESSURE));
    }

    @Test
    public void testAggregateConnectionPredictionsWithMixedValues() {
        Map<String, Double> aggregatedMetrics = new HashMap<>();
        generateConnectionAnalyticMetricsAggregation(aggregatedMetrics, mixedValuedPredictions);

        assertEquals(2, aggregatedMetrics.size());
        assertEquals(1.0, aggregatedMetrics.get(NIFI_TIME_TO_BYTES_BACKPRESSURE_PREDICTION));
        assertEquals(2.0, aggregatedMetrics.get(NIFI_TIME_TO_COUNT_BACKPRESSURE_PREDICTION));
    }

    @Test
    public void testAggregateConnectionPredictionsWithAllDefaultValues() {
        Map<String, Double> aggregatedMetrics = new HashMap<>();
        generateConnectionAnalyticMetricsAggregation(aggregatedMetrics, defaultValuedPredictions);

        assertEquals(2, aggregatedMetrics.size());
        assertEquals(EXPECTED_DEFAULT_PREDICTION_VALUE, aggregatedMetrics.get(NIFI_TIME_TO_BYTES_BACKPRESSURE_PREDICTION));
        assertEquals(EXPECTED_DEFAULT_PREDICTION_VALUE, aggregatedMetrics.get(NIFI_TIME_TO_COUNT_BACKPRESSURE_PREDICTION));
    }

    @Test
    public void testAggregateConnectionPredictionsWithBackpressure() {
        Map<String, Double> aggregatedMetrics = new HashMap<>();
        aggregatedMetrics.put(BYTES_AT_BACKPRESSURE, 1.0);
        aggregatedMetrics.put(COUNT_AT_BACKPRESSURE, 0.0);
        generateConnectionAnalyticMetricsAggregation(aggregatedMetrics, mixedValuedPredictions);

        assertEquals(EXPECTED_BACKPRESSURE_PREDICTION_VALUE, aggregatedMetrics.get(NIFI_TIME_TO_BYTES_BACKPRESSURE_PREDICTION));
        assertEquals(2.0, aggregatedMetrics.get(NIFI_TIME_TO_COUNT_BACKPRESSURE_PREDICTION));
    }

    @Test
    public void testAggregatedConnectionPredictionsDatapointCreationWithAnalyticsNotSet() {
        ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();
        Map<String, Double> emptyAggregatedMetrics = new HashMap<>();

        PrometheusMetricsUtil.createAggregatedConnectionStatusAnalyticsMetrics(connectionAnalyticsMetricsRegistry,
                emptyAggregatedMetrics,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(connectionAnalyticsMetricsRegistry);

        assertTrue(emptyAggregatedMetrics.isEmpty());
        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, everyItem(is(EXPECTED_DEFAULT_PREDICTION_VALUE)));
    }

    @Test
    public void testAggregatedConnectionPredictionsDatapointCreationWithAllDefaultValues() {
        ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();
        Map<String, Double> aggregatedMetrics = new HashMap<>();
        generateConnectionAnalyticMetricsAggregation(aggregatedMetrics, defaultValuedPredictions);

        PrometheusMetricsUtil.createAggregatedConnectionStatusAnalyticsMetrics(connectionAnalyticsMetricsRegistry,
                aggregatedMetrics,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(connectionAnalyticsMetricsRegistry);

        assertEquals(2, aggregatedMetrics.size());
        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, everyItem(is(EXPECTED_DEFAULT_PREDICTION_VALUE)));
    }

    @Test
    public void testAggregatedConnectionPredictionsDatapointCreationWithMixedValues() {
        ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();
        Map<String, Double> aggregatedMetrics = new HashMap<>();
        generateConnectionAnalyticMetricsAggregation(aggregatedMetrics, mixedValuedPredictions);

        PrometheusMetricsUtil.createAggregatedConnectionStatusAnalyticsMetrics(connectionAnalyticsMetricsRegistry,
                aggregatedMetrics,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(connectionAnalyticsMetricsRegistry);

        assertEquals(2, aggregatedMetrics.size());
        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, hasItems(1.0, 2.0));
    }

    @Test
    public void testAggregatedConnectionPredictionsDatapointCreationWithBackpressure() {
        ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();
        Map<String, Double> aggregatedMetrics = new HashMap<>();
        aggregatedMetrics.put(BYTES_AT_BACKPRESSURE, 1.0);
        aggregatedMetrics.put(COUNT_AT_BACKPRESSURE, 0.0);
        generateConnectionAnalyticMetricsAggregation(aggregatedMetrics, mixedValuedPredictions);

        PrometheusMetricsUtil.createAggregatedConnectionStatusAnalyticsMetrics(connectionAnalyticsMetricsRegistry,
                aggregatedMetrics,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(connectionAnalyticsMetricsRegistry);

        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, hasItems(0.0, 2.0));
    }

    @Test
    public void testAggregatedNifiMetricsDatapointCreationWithoutResults() {
        NiFiMetricsRegistry niFiMetricsRegistry = new NiFiMetricsRegistry();
        Map<String, Double> emptyAggregatedMetrics = new HashMap<>();

        PrometheusMetricsUtil.createAggregatedNifiMetrics(niFiMetricsRegistry,
                emptyAggregatedMetrics,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(niFiMetricsRegistry);

        assertTrue(emptyAggregatedMetrics.isEmpty());
        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, everyItem(is(EXPECTED_DEFAULT_PERCENT_USED_VALUE)));
    }

    @Test
    public void testAggregatedNifiMetricsDatapointCreationWithSingleProcessGroup() {
        NiFiMetricsRegistry niFiMetricsRegistry = new NiFiMetricsRegistry();
        Map<String, Double> result = new HashMap<>();

        PrometheusMetricsUtil.aggregatePercentUsed(singleProcessGroupStatus, result);
        PrometheusMetricsUtil.createAggregatedNifiMetrics(niFiMetricsRegistry,
                result,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(niFiMetricsRegistry);

        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, everyItem(is(EXPECTED_DEFAULT_PERCENT_USED_VALUE)));
    }

    @Test
    public void testAggregatedNifiMetricsDatapointCreationWithNestedProcessGroup() {
        NiFiMetricsRegistry niFiMetricsRegistry = new NiFiMetricsRegistry();
        Map<String, Double> result = new HashMap<>();

        PrometheusMetricsUtil.aggregatePercentUsed(nestedProcessGroupStatus, result);
        PrometheusMetricsUtil.createAggregatedNifiMetrics(niFiMetricsRegistry,
                result,
                EMPTY,
                EMPTY,
                EMPTY,
                EMPTY);

        List<Double> sampleValues = getSampleValuesList(niFiMetricsRegistry);

        assertEquals(2, sampleValues.size());
        assertThat(sampleValues, hasItems(EXPECTED_NESTED_BYTES_PERCENT_VALUE, EXPECTED_NESTED_COUNT_PERCENT_VALUE));
    }

    private static ProcessGroupStatus createSingleProcessGroupStatus(final long queuedBytes, final long bytesThreshold, final int queuedCount, final long objectThreshold) {
        ProcessGroupStatus singleStatus = new ProcessGroupStatus();
        List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        ConnectionStatus connectionStatus = new ConnectionStatus();

        connectionStatus.setQueuedBytes(queuedBytes);
        connectionStatus.setBackPressureBytesThreshold(bytesThreshold);
        connectionStatus.setQueuedCount(queuedCount);
        connectionStatus.setBackPressureObjectThreshold(objectThreshold);
        connectionStatuses.add(connectionStatus);
        singleStatus.setConnectionStatus(connectionStatuses);

        return singleStatus;
    }

    private static ProcessGroupStatus createNestedProcessGroupStatus() {
        ProcessGroupStatus rootStatus = new ProcessGroupStatus();
        ProcessGroupStatus status1 = createSingleProcessGroupStatus(15, 100, 10, 200);
        ProcessGroupStatus status2 = createSingleProcessGroupStatus(150, 200, 5, 30);

        status1.setProcessGroupStatus(Collections.singletonList(status2));
        rootStatus.setProcessGroupStatus(Collections.singletonList(status1));

        return rootStatus;
    }

    private static ProcessGroupStatus createNestedProcessGroupStatusWithCountBackpressure() {
        ProcessGroupStatus rootStatus = new ProcessGroupStatus();
        ProcessGroupStatus status1 = createSingleProcessGroupStatus(15, 100, 1, 1);
        ProcessGroupStatus status2 = createSingleProcessGroupStatus(150, 200, 5, 30);

        status1.setProcessGroupStatus(Collections.singletonList(status2));
        rootStatus.setProcessGroupStatus(Collections.singletonList(status1));

        return rootStatus;
    }

    private static Map<String, Map<String, Long>> createPredictionsWithMixedValue() {
        Map<String, Map<String, Long>> predictions = new HashMap<>();

        predictions.put(CONNECTION_1, new HashMap<String, Long>() {{
            put(TIME_TO_BYTES_BACKPRESSURE_MILLIS, Long.MAX_VALUE);
            put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, Long.MAX_VALUE);
        }});
        predictions.put(CONNECTION_2, new HashMap<String, Long>() {{
            put(TIME_TO_BYTES_BACKPRESSURE_MILLIS, DEFAULT_PREDICTION_VALUE);
            put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, DEFAULT_PREDICTION_VALUE);
        }});
        predictions.put(CONNECTION_3, new HashMap<String, Long>() {{
            put(TIME_TO_BYTES_BACKPRESSURE_MILLIS, 1L);
            put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, 4L);
        }});
        predictions.put(CONNECTION_4, new HashMap<String, Long>() {{
            put(TIME_TO_BYTES_BACKPRESSURE_MILLIS, 3L);
            put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, 2L);
        }});
        return predictions;
    }

    private static Map<String, Map<String, Long>> createPredictionsWithDefaultValuesOnly() {
        Map<String, Map<String, Long>> predictions = new HashMap<>();
        Map<String, Long> defaultPredictions = new HashMap<String, Long>() {{
            put(TIME_TO_BYTES_BACKPRESSURE_MILLIS, DEFAULT_PREDICTION_VALUE);
            put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, DEFAULT_PREDICTION_VALUE);
        }};

        predictions.put(CONNECTION_1, defaultPredictions);
        predictions.put(CONNECTION_2, defaultPredictions);
        predictions.put(CONNECTION_3, defaultPredictions);
        predictions.put(CONNECTION_4, defaultPredictions);
        return predictions;
    }

    private static Set<String> createConnections() {
        Set<String> connections = new HashSet<>();
        connections.add(CONNECTION_1);
        connections.add(CONNECTION_2);
        connections.add(CONNECTION_3);
        connections.add(CONNECTION_4);
        return connections;
    }

    private Map<String, Long> getPredictions(final Map<String, Map<String, Long>> predictions, final String connection) {
        return predictions.get(connection);
    }

    private List<Double> getSampleValuesList(final AbstractMetricsRegistry metricsRegistry) {
        return Collections.list(metricsRegistry.getRegistry().metricFamilySamples())
                .stream()
                .flatMap(familySamples -> familySamples.samples.stream())
                .map(sample -> sample.value)
                .collect(Collectors.toList());
    }

    private void generateConnectionAnalyticMetricsAggregation(final Map<String, Double> aggregatedMetrics, final Map<String, Map<String, Long>> predictions) {
        for (final String connection : connections) {
            PrometheusMetricsUtil.aggregateConnectionPredictionMetrics(aggregatedMetrics, getPredictions(predictions, connection));
        }
    }
}
