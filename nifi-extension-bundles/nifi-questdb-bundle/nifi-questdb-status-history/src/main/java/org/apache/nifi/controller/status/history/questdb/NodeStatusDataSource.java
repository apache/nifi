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
import org.apache.nifi.questdb.InsertRowContext;
import org.apache.nifi.questdb.InsertRowDataSource;

import java.util.Iterator;
import java.util.Map;

final class NodeStatusDataSource implements InsertRowDataSource {
    private final Iterator<CapturedStatus<NodeStatus>> statuses;
    private final Map<Integer, MetricDescriptor<NodeStatus>> metrics;

    NodeStatusDataSource(final Iterator<CapturedStatus<NodeStatus>> statuses, final Map<Integer, MetricDescriptor<NodeStatus>> metrics) {
        this.statuses = statuses;
        this.metrics = metrics;
    }

    @Override
    public boolean hasNextToInsert() {
        return statuses.hasNext();
    }

    @Override
    public void fillRowData(final InsertRowContext context) {
        final CapturedStatus<NodeStatus> status = statuses.next();
        context.initializeRow(status.getCaptured());

        for (final Map.Entry<Integer, MetricDescriptor<NodeStatus>> metric : metrics.entrySet()) {
            context.addLong(metric.getKey(), metric.getValue().getValueFunction().getValue(status.getStatus()));
        }
    }
}
