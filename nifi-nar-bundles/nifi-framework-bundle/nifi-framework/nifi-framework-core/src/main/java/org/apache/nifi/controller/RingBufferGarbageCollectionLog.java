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
package org.apache.nifi.controller;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RingBufferGarbageCollectionLog implements GarbageCollectionLog, NotificationListener {
    private static final Logger logger = LoggerFactory.getLogger(RingBufferGarbageCollectionLog.class);
    private final RingBuffer<GarbageCollectionEvent> events;
    private final long minDurationThreshold;
    private final long jvmStartTime;

    // guarded by synchronizing on this
    private GarbageCollectionEvent maxDurationEvent;
    private final Map<String, Tuple<Long, Long>> timeAndCountPerAction = new HashMap<>();

    public RingBufferGarbageCollectionLog(final int eventCount, final long minDurationThreshold) {
        this.events = new RingBuffer<>(eventCount);
        this.minDurationThreshold = minDurationThreshold;
        jvmStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
    }

    @Override
    public long getMinDurationThreshold() {
        return minDurationThreshold;
    }

    @Override
    public List<GarbageCollectionEvent> getGarbageCollectionEvents() {
        return events.asList();
    }

    @Override
    public synchronized Map<String, Long> getGarbageCollectionCounts() {
        final Map<String, Long> counts = new HashMap<>();
        timeAndCountPerAction.forEach((action, tuple) -> counts.put(action, tuple.getValue()));
        return counts;
    }

    @Override
    public synchronized Map<String, Long> getAverageGarbageCollectionDurations() {
        final Map<String, Long> counts = new HashMap<>();
        timeAndCountPerAction.forEach((action, tuple) -> counts.put(action, tuple.getKey() / tuple.getValue()));
        return counts;
    }

    @Override
    public synchronized GarbageCollectionEvent getLongestGarbageCollectionEvent() {
        return maxDurationEvent;
    }

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
        if (!notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
            return;
        }

        final CompositeData compositeData = (CompositeData) notification.getUserData();
        final GarbageCollectionNotificationInfo gcNotification = GarbageCollectionNotificationInfo.from(compositeData);
        final GcInfo gcInfo = gcNotification.getGcInfo();

        final String gcName = gcNotification.getGcName();
        final String action = gcNotification.getGcAction();
        final String cause = gcNotification.getGcCause();

        final long startTime = jvmStartTime + gcInfo.getStartTime();
        final long endTime = jvmStartTime + gcInfo.getEndTime();

        final Map<String, MemoryUsage> usageAfter = gcInfo.getMemoryUsageAfterGc();
        final Map<String, MemoryUsage> usageBefore = gcInfo.getMemoryUsageBeforeGc();

        final List<GarbageCollectionEvent.GarbageCollectionHeapSize> heapSizes = new ArrayList<>();
        for (final Map.Entry<String, MemoryUsage> entry : usageAfter.entrySet()) {
            final MemoryUsage before = usageBefore.get(entry.getKey());
            if (before == null) {
                continue;
            }

            final MemoryUsage after = entry.getValue();
            if (after.getUsed() == before.getUsed()) {
                continue;
            }

            heapSizes.add(new StandardGarbageCollectionEvent.StandardGarbageCollectionHeapSize(entry.getKey(), before.getUsed(), after.getUsed()));
        }

        final GarbageCollectionEvent event = new StandardGarbageCollectionEvent(gcName, action, cause, startTime, endTime, heapSizes);

        if (gcInfo.getDuration() >= minDurationThreshold) {
            events.add(event);
        }

        synchronized (this) {
            final Tuple<Long, Long> previousTuple = timeAndCountPerAction.get(action);
            if (previousTuple == null){
                timeAndCountPerAction.put(action, new Tuple<>(gcInfo.getDuration(), 1L));
            } else {
                timeAndCountPerAction.put(action, new Tuple<>(gcInfo.getDuration() + previousTuple.getKey(), 1L + previousTuple.getValue()));
            }

            if (maxDurationEvent == null || event.getDuration() > maxDurationEvent.getDuration()) {
                maxDurationEvent = event;
            }
        }
    }
}
