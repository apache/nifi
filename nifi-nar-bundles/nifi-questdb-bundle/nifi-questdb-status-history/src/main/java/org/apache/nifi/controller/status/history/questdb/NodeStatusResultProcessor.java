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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.StandardMetricDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.apache.nifi.questdb.QueryRowContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class NodeStatusResultProcessor implements QueryResultProcessor<List<StandardStatusSnapshot>> {
    private final Map<Integer, MetricDescriptor<NodeStatus>> metricDescriptors;
    private final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> storageMetricsByTime;
    private final List<StandardStatusSnapshot> result = new ArrayList<>();

    NodeStatusResultProcessor(
        final Map<Integer, MetricDescriptor<NodeStatus>> metricDescriptors,
        final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> storageMetricsByTime
    ) {
        this.metricDescriptors = metricDescriptors;
        this.storageMetricsByTime = storageMetricsByTime;
    }

    @Override
    public void processRow(final QueryRowContext context) {
        final long createdAt = TimeUnit.MICROSECONDS.toMillis(context.getTimestamp(0));
        final Map<StandardMetricDescriptor<NodeStatus>, Long> storageMetrics = storageMetricsByTime.get(createdAt);
        final Set<MetricDescriptor<?>> snapshotMetrics = new HashSet<>(metricDescriptors.size() + storageMetrics.keySet().size());
        snapshotMetrics.addAll(metricDescriptors.values());
        snapshotMetrics.addAll(storageMetrics.keySet());
        final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(snapshotMetrics);

        snapshot.setTimestamp(new Date(TimeUnit.MICROSECONDS.toMillis(context.getTimestamp(0))));

        for (final Map.Entry<Integer, MetricDescriptor<NodeStatus>> metricDescriptor : metricDescriptors.entrySet()) {
            snapshot.addStatusMetric(metricDescriptor.getValue(), context.getLong(metricDescriptor.getKey()));
        }

        storageMetrics.entrySet().forEach(entry -> snapshot.addStatusMetric(entry.getKey(), entry.getValue()));
        result.add(snapshot);
    }

    @Override
    public List<StandardStatusSnapshot> getResult() {
        return result;
    }
}
