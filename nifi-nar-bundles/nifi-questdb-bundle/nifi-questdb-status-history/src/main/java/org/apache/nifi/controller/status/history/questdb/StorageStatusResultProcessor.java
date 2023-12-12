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
import org.apache.nifi.questdb.QueryResultProcessor;
import org.apache.nifi.questdb.QueryRowContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class StorageStatusResultProcessor implements QueryResultProcessor<Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>>> {
    private static final String STORAGE_FREE_DESCRIPTION = "The usable space available for use by the underlying storage mechanism.";
    private static final String STORAGE_USED_DESCRIPTION = "The space in use on the underlying storage mechanism";

    private final Map<Integer, MetricDescriptor<NodeStatus>> metricDescriptors;
    private final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> result = new HashMap<>();
    private int storageNumber = 1;

    StorageStatusResultProcessor(final Map<Integer, MetricDescriptor<NodeStatus>> metricDescriptors) {
        this.metricDescriptors = metricDescriptors;
    }

    @Override
    public void processRow(final QueryRowContext context) {
        final long createdAt = TimeUnit.MICROSECONDS.toMillis(context.getTimestamp(0));
        final String name = context.getString(1);
        final short type = context.getShort(2);

        if (!result.containsKey(createdAt)) {
            result.put(createdAt, new HashMap<>());
        }

        final StorageStatusType storageStatusType = StorageStatusType.getById(type);

        result.get(createdAt).put(getDescriptor(
                metricDescriptors.size() + result.get(createdAt).size(),
                getField(storageStatusType, storageNumber, StorageMetric.FREE),
                getLabel(storageStatusType, name, StorageMetric.FREE),
                STORAGE_FREE_DESCRIPTION
        ), context.getLong(3));

        result.get(createdAt).put(getDescriptor(
                metricDescriptors.size() + result.get(createdAt).size(),
                getField(storageStatusType, storageNumber, StorageMetric.USED),
                getLabel(storageStatusType, name, StorageMetric.USED),
                STORAGE_USED_DESCRIPTION
        ), context.getLong(4));


        storageNumber++;
    }

    @Override
    public Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> getResult() {
        return result;
    }

    private StandardMetricDescriptor<NodeStatus> getDescriptor(final int ordinal, final String field, final String label, final String description) {
        return new StandardMetricDescriptor<>(() -> ordinal, field, label, description, MetricDescriptor.Formatter.DATA_SIZE, v -> 0L);
    }

    private String getField(final StorageStatusType type, final int storageNumber, final StorageMetric storageMetric) {
        return type.getField() + storageNumber + storageMetric.getField();
    }

    private String getLabel(final StorageStatusType type, final CharSequence name, final StorageMetric storageMetric) {
        return type.getLabel() + " (" + name + ") " + storageMetric.getLabel();
    }
}
