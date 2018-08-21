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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComponentStatusHistory {

    private final MetricRollingBuffer snapshots;
    private ComponentDetails componentDetails;

    public ComponentStatusHistory(final ComponentDetails details, final int maxCapacity) {
        this.componentDetails = details;
        snapshots = new MetricRollingBuffer(maxCapacity);
    }

    public void expireBefore(final Date timestamp) {
        snapshots.expireBefore(timestamp);
    }

    public void update(final StatusSnapshot snapshot, final ComponentDetails details) {
        if (snapshot == null) {
            return;
        }

        snapshots.update(snapshot);
        componentDetails = details;
    }

    public StatusHistory toStatusHistory(final List<Date> timestamps, final boolean includeCounters, final Set<MetricDescriptor<?>> defaultStatusMetrics) {
        final Date dateGenerated = new Date();
        final Map<String, String> componentDetailsMap = componentDetails.toMap();
        final List<StatusSnapshot> snapshotList = snapshots.getSnapshots(timestamps, includeCounters, defaultStatusMetrics);
        return new StandardStatusHistory(snapshotList, componentDetailsMap, dateGenerated);
    }
}
