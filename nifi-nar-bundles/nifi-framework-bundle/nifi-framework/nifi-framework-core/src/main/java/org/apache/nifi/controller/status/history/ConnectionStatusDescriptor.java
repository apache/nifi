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
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;

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
        s -> Long.valueOf(s.getQueuedCount()));


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
}
