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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.util.NiFiProperties;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConnectionStatusDescriptor {

  private NiFiProperties mockNiFiProperties;
  private final String DEFAULT_LABEL = "Backpressure Estimate";
  private final String MODIFIED_LABEL = "Backpressure Estimate (Disabled)";
  private final String FIELD_NAME = "backpressureEstimate";

  private final int  COUNT_THRESHOLD = 100;
  private final long BYTE_THRESHOLD = 100L;

  /**
   * Test that Status History Backpressure label is correct when analytics enabled.
   */
  @Test
  public void testActiveLabel() {
    mockNiFiProperties = mock(NiFiProperties.class);
    when(mockNiFiProperties.getProperty(NiFiProperties.ANALYTICS_PREDICTION_ENABLED,
        NiFiProperties.DEFAULT_ANALYTICS_PREDICTION_ENABLED)).thenReturn("true");
    ConnectionStatusDescriptor.setNifiProperties(mockNiFiProperties);

    Set<MetricDescriptor<?>> CONNECTION_METRICS =
        Arrays.stream(ConnectionStatusDescriptor.values())
            .map(ConnectionStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());

    CONNECTION_METRICS.forEach(p -> {
      if (p.getField().equals(FIELD_NAME)) {
        Assert.assertEquals(DEFAULT_LABEL, p.getLabel());
      }
    });
  }

  /**
   * Test that Status History Backpressure label is correct when analytics disabled.
   */
  @Test
  public void testInactiveLabel() {
    mockNiFiProperties = mock(NiFiProperties.class);
    when(mockNiFiProperties.getProperty(NiFiProperties.ANALYTICS_PREDICTION_ENABLED,
        NiFiProperties.DEFAULT_ANALYTICS_PREDICTION_ENABLED)).thenReturn("false");
    ConnectionStatusDescriptor.setNifiProperties(mockNiFiProperties);

    Set<MetricDescriptor<?>> CONNECTION_METRICS =
        Arrays.stream(ConnectionStatusDescriptor.values())
            .map(ConnectionStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());

    CONNECTION_METRICS.forEach(p -> {
      if (p.getField().equals(FIELD_NAME)) {
        Assert.assertEquals(MODIFIED_LABEL, p.getLabel());
      }
    });
  }

  /**
   * Test correct value returned when predictions are null
   */
  @Test
  public void testGraphPointWithNoPredictions() {
    ConnectionStatus status = new ConnectionStatus();
    Assert.assertNull(status.getPredictions());
    Assert.assertEquals(-1L, ConnectionStatusDescriptor.getGraphPoint(status));
  }

  //
  // Verify getGraphPoint returns expected values given various prediction and queue values.
  //
  @Test
  public void testGraphPointValues() {
    long graphPoint;
    String GRAPH_THRESHOLD_AS_STRING = "6 hrs";
    long GRAPH_THRESHOLD = 21_600_000;
    int MISC_COUNT = 45;
    long MISC_BYTE = 55;
    long PREDICTION = 10_000_000L;
    long LARGE_PREDICTION = 20_000_000L;
    long SMALL_PREDICTION = 1000L;
    long EXCESS_PREDICTION = 50_000_000L;
    long OFFSET = 100;

    mockNiFiProperties = mock(NiFiProperties.class);
    when(mockNiFiProperties.getProperty(NiFiProperties.ANALYTICS_PREDICTION_ENABLED,
        NiFiProperties.DEFAULT_ANALYTICS_PREDICTION_ENABLED)).thenReturn("true");
    when(mockNiFiProperties.getProperty(NiFiProperties.ANALYTICS_STATUS_GRAPH_DISPLAY_THRESHOLD,
        NiFiProperties.DEFAULT_ANALYTICS_STATUS_GRAPH_THRESHOLD)).thenReturn(
        GRAPH_THRESHOLD_AS_STRING);
    ConnectionStatusDescriptor.setNifiProperties(mockNiFiProperties);
    ConnectionStatusPredictions predictions = new ConnectionStatusPredictions();
    ConnectionStatus status = new ConnectionStatus();

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-1", 0, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0L, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-2", MISC_COUNT, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0L, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-3", COUNT_THRESHOLD, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0L, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-4", 0, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0L, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-5", 0, COUNT_THRESHOLD);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0L, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-6", MISC_COUNT, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0L, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-7", 0, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(PREDICTION, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(EXCESS_PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(-1L);
    setStatusValues(predictions, status, "Connection-8", MISC_COUNT, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(GRAPH_THRESHOLD, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(PREDICTION);
    setStatusValues(predictions, status, "Connection-9", 0, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(PREDICTION, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(PREDICTION);
    setStatusValues(predictions, status, "Connection-10", COUNT_THRESHOLD, 0L);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(EXCESS_PREDICTION);
    setStatusValues(predictions, status, "Connection-11", 0, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(GRAPH_THRESHOLD, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(EXCESS_PREDICTION);
    setStatusValues(predictions, status, "Connection-12", COUNT_THRESHOLD, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(EXCESS_PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(EXCESS_PREDICTION);
    setStatusValues(predictions, status, "Connection-13", MISC_COUNT, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(GRAPH_THRESHOLD, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(LARGE_PREDICTION);
    setStatusValues(predictions, status, "Connection-14", MISC_COUNT, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(PREDICTION, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(SMALL_PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(SMALL_PREDICTION + OFFSET);
    setStatusValues(predictions, status, "Connection-15", COUNT_THRESHOLD, BYTE_THRESHOLD);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(SMALL_PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(SMALL_PREDICTION + OFFSET);
    setStatusValues(predictions, status, "Connection-16", MISC_COUNT, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(SMALL_PREDICTION, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(PREDICTION);
    setStatusValues(predictions, status, "Connection-17", COUNT_THRESHOLD, BYTE_THRESHOLD);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(-1L);
    predictions.setPredictedTimeToBytesBackpressureMillis(PREDICTION);
    setStatusValues(predictions, status, "Connection-18", MISC_COUNT, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(PREDICTION, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(SMALL_PREDICTION);
    setStatusValues(predictions, status, "Connection-19", MISC_COUNT, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(SMALL_PREDICTION, graphPoint);

    predictions.setPredictedTimeToCountBackpressureMillis(EXCESS_PREDICTION);
    predictions.setPredictedTimeToBytesBackpressureMillis(EXCESS_PREDICTION);
    setStatusValues(predictions, status, "Connection-20", COUNT_THRESHOLD, MISC_BYTE);
    graphPoint = ConnectionStatusDescriptor.getGraphPoint(status);
    Assert.assertEquals(0, graphPoint);
  }


  private void setStatusValues(ConnectionStatusPredictions predictions, ConnectionStatus status,
      String connectionName, int queuedCount, long queuedBytes) {
    status.setName(connectionName);
    status.setBackPressureObjectThreshold(COUNT_THRESHOLD);
    status.setBackPressureBytesThreshold(BYTE_THRESHOLD);
    status.setPredictions(predictions);
    status.setQueuedCount(queuedCount);
    status.setQueuedBytes(queuedBytes);
  }
}
