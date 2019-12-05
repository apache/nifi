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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;


public enum ConnectionStatusDescriptor {
    INPUT_BYTES(
        "inputBytes",
        "Bytes In (5 mins)",
        "The cumulative size of all FlowFiles that were transferred to this Connection in the past 5 minutes",
        Formatter.DATA_SIZE,
        ConnectionStatus::getInputBytes),

    INPUT_COUNT(
        "inputCount",
        "FlowFiles In (5 mins)",
        "The number of FlowFiles that were transferred to this Connection in the past 5 minutes",
        Formatter.COUNT,
        s -> Long.valueOf(s.getInputCount())),

    OUTPUT_BYTES(
        "outputBytes",
        "Bytes Out (5 mins)",
        "The cumulative size of all FlowFiles that were pulled from this Connection in the past 5 minutes",
        Formatter.DATA_SIZE,
        ConnectionStatus::getOutputBytes),

    OUTPUT_COUNT(
        "outputCount",
        "FlowFiles Out (5 mins)",
        "The number of FlowFiles that were pulled from this Connection in the past 5 minutes",
        Formatter.COUNT,
        s -> Long.valueOf(s.getOutputCount())),

    QUEUED_BYTES(
        "queuedBytes",
        "Queued Bytes",
        "The number of Bytes queued in this Connection",
        Formatter.DATA_SIZE,
        ConnectionStatus::getQueuedBytes),

    QUEUED_COUNT(
        "queuedCount",
        "Queued Count",
        "The number of FlowFiles queued in this Connection",
        Formatter.COUNT,
        s -> Long.valueOf(s.getQueuedCount())),

    TIME_TO_BACKPRESSURE(
        "backpressureEstimate",
        "Backpressure Estimate",
        "The estimated time to Backpressure for this connection. " +
            "(Values are only displayed if Prediction Analytics are enabled). " +
            "A user-defined value limits the maximum value that will be displayed. Default: 6 hrs.",
        Formatter.DURATION,
        ConnectionStatusDescriptor::getGraphPoint);

    private MetricDescriptor<ConnectionStatus> descriptor;

    ConnectionStatusDescriptor(final String field, final String label, final String description,
                              final MetricDescriptor.Formatter formatter, final ValueMapper<ConnectionStatus> valueFunction) {

        this.descriptor = new StandardMetricDescriptor<>(this::ordinal, field, label, description, formatter, valueFunction);
    }

    public String getField() {
        return descriptor.getField();
    }

    public MetricDescriptor<ConnectionStatus> getDescriptor() {
        return descriptor;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusDescriptor.class);

    private static NiFiProperties nifiProperties;

    public static void setNifiProperties(NiFiProperties nifiProperties) {
        ConnectionStatusDescriptor.nifiProperties = nifiProperties;
    }

    private void updateLabel(String alabel) {
        descriptor.updateLabel(alabel);
    }

    /**
     * Return the point to be displayed on the backpressure status history graph.
     */
    static long getGraphPoint(ConnectionStatus status) {
        if (status.getPredictions() == null) {
            TIME_TO_BACKPRESSURE.updateLabel("Backpressure Estimate (Disabled)");
            LOG.debug("StatusAnalyticsEngine not enabled.");
            return -1L;
        }
        long maxPercentage = Math.max(status.getQueuedCount() / status.getBackPressureObjectThreshold(),
            status.getQueuedBytes() / status.getBackPressureBytesThreshold());
        if (maxPercentage >= 1) {
            return 0L;
        }
        long graphThreshold = getStatusGraphThreshold();
        long predictedTimeToCountBackpressure = Math.min(status.getPredictions().getPredictedTimeToCountBackpressureMillis(),
            graphThreshold);
        long predictedTimeToBytesBackpressure = Math.min(status.getPredictions().getPredictedTimeToBytesBackpressureMillis(),
            graphThreshold);
        if (predictedTimeToCountBackpressure + predictedTimeToBytesBackpressure == -2) {
            return 0L;
        }
        if (predictedTimeToCountBackpressure * predictedTimeToBytesBackpressure < 0) {
            return Math.max(predictedTimeToCountBackpressure, predictedTimeToBytesBackpressure);
        }
        return Math.min(predictedTimeToCountBackpressure, predictedTimeToBytesBackpressure);
    }

    private static long getStatusGraphThreshold() {
        final String graphDisplayThreshold =
            nifiProperties.getProperty(NiFiProperties.ANALYTICS_STATUS_GRAPH_DISPLAY_THRESHOLD,
                NiFiProperties.DEFAULT_ANALYTICS_STATUS_GRAPH_THRESHOLD);
        return Math.round(FormatUtils.getPreciseTimeDuration(graphDisplayThreshold,
            TimeUnit.MILLISECONDS));
    }
}
